from dataclasses import dataclass
from typing import Any

import numpy as np

from .document_retrieval import RetrievalResult, _positive_count


@dataclass(frozen=True)
class ReaderResult:
    answer: str | None
    source: RetrievalResult | None
    start: int | None
    end: int | None
    score_margin: float | None
    windows_read: int
    passages_read: int
    coverage_limited: bool


class ExtractiveReader:
    def __init__(
        self, tokenizer: Any, predictor: Any, max_windows: int = 10,
        max_length: int = 384, stride: int = 128, max_answer_tokens: int = 30,
        no_answer_threshold: float = 0.0, batch_size: int = 10,
    ) -> None:
        """Initialize bounded answer extraction."""
        if not tokenizer.is_fast or tokenizer.cls_token_id is None:
            raise ValueError("A fast tokenizer with a CLS token is required.")
        self.max_windows = _positive_count(max_windows, "max_windows")
        self.max_length = _positive_count(max_length, "max_length")
        self.max_answer_tokens = _positive_count(max_answer_tokens, "max_answer_tokens")
        self.batch_size = _positive_count(batch_size, "batch_size")
        if self.max_windows > 10 or self.max_length > 384:
            raise ValueError("Reader limits are ten windows and 384 tokens per window.")
        if isinstance(stride, bool) or not isinstance(stride, int) or not 0 <= stride < self.max_length:
            raise ValueError("stride must be a nonnegative integer below max_length.")
        if not np.isfinite(no_answer_threshold):
            raise ValueError("no_answer_threshold must be finite.")
        self.tokenizer = tokenizer
        self.predictor = predictor
        self.stride = stride
        self.no_answer_threshold = float(no_answer_threshold)

    def _windows(self, question: str, passages: list[RetrievalResult]) -> tuple[list, list, bool]:
        """Allocate windows across ranked passages."""
        features = []
        sources = []
        names = getattr(self.tokenizer, "model_input_names", ["input_ids", "attention_mask", "token_type_ids"])
        encodings = []
        for passage in passages[:self.max_windows]:
            encoded = self.tokenizer(
                question, passage.content, truncation="only_second", max_length=self.max_length,
                stride=self.stride, return_overflowing_tokens=True,
                return_offsets_mapping=True, padding=False,
            )
            encodings.append((passage, encoded))
        counts = [len(encoded["input_ids"]) for _, encoded in encodings]
        selected = []
        for window in range(max(counts, default=0)):
            for position, count in enumerate(counts):
                if window < count:
                    selected.append((position, window))
                if len(selected) == self.max_windows:
                    break
            if len(selected) == self.max_windows:
                break
        limited = len(passages) > len(encodings) or sum(counts) > len(selected)
        for position, window in sorted(selected):
            passage, encoded = encodings[position]
            ids = encoded["input_ids"][window]
            offsets = encoded["offset_mapping"][window]
            sequence_ids = encoded.sequence_ids(window)
            if len(ids) > self.max_length or len(ids) != len(offsets) or len(ids) != len(sequence_ids):
                raise ValueError("Tokenizer returned inconsistent window lengths.")
            if self.tokenizer.cls_token_id not in ids:
                raise ValueError("A reader window has no CLS token.")
            context = [tuple(offset) if sequence == 1 else None
                       for sequence, offset in zip(sequence_ids, offsets)]
            features.append({name: encoded[name][window] for name in names if name in encoded})
            sources.append((passage, context, ids.index(self.tokenizer.cls_token_id)))
        return features, sources, limited

    def _span(self, start: np.ndarray, end: np.ndarray, offsets: list, text: str) -> tuple | None:
        """Find the best valid context span."""
        length = len(offsets)
        valid = np.array([offset is not None and 0 <= offset[0] < offset[1] <= len(text)
                          for offset in offsets], dtype=bool)
        if not valid.any():
            return None
        starts = np.array([offset[0] if offset is not None else 0 for offset in offsets])
        ends = np.array([offset[1] if offset is not None else 0 for offset in offsets])
        positions = np.arange(length)
        allowed = valid[:, None] & valid[None, :]
        allowed &= positions[None, :] >= positions[:, None]
        allowed &= positions[None, :] - positions[:, None] < self.max_answer_tokens
        allowed &= ends[None, :] > starts[:, None]
        invalid = np.concatenate(([0], np.cumsum(~valid)))
        allowed &= invalid[1:][None, :] - invalid[:-1][:, None] == 0
        scores = np.where(allowed, start[:length, None] + end[None, :length], -np.inf)
        left, right = np.unravel_index(np.argmax(scores), scores.shape)
        if not np.isfinite(scores[left, right]):
            return None
        return int(starts[left]), int(ends[right]), float(scores[left, right])

    def read(self, question: str, passages: list[RetrievalResult]) -> ReaderResult:
        """Extract a sourced answer or abstain."""
        if not isinstance(question, str) or not question.strip():
            raise ValueError("A nonempty question is required.")
        question = question.strip()
        query_tokens = self.tokenizer(question, add_special_tokens=False)["input_ids"]
        if len(query_tokens) > 64:
            raise ValueError("Reader questions must contain at most 64 tokens.")
        features, sources, limited = self._windows(question, passages)
        best = None
        for first in range(0, len(features), self.batch_size):
            batch = features[first:first + self.batch_size]
            start_logits, end_logits = (np.asarray(values, dtype=np.float64)
                                        for values in self.predictor(batch))
            if (start_logits.ndim != 2 or start_logits.shape != end_logits.shape
                    or len(start_logits) != len(batch)
                    or start_logits.shape[1] < max(len(feature["input_ids"]) for feature in batch)
                    or not np.isfinite(start_logits).all() or not np.isfinite(end_logits).all()):
                raise ValueError("Reader logits must be finite aligned start/end matrices.")
            for row, (passage, offsets, cls) in enumerate(sources[first:first + len(batch)]):
                span = self._span(start_logits[row], end_logits[row], offsets, passage.content)
                if span is None:
                    continue
                left, right, score = span
                margin = score - float(start_logits[row, cls] + end_logits[row, cls])
                if best is None or margin > best[0]:
                    best = (margin, passage, left, right)
        windows_read = len(features)
        passages_read = len({source[0].vector_id for source in sources})
        margin = best[0] if best is not None else None
        if best is None or margin <= self.no_answer_threshold:
            return ReaderResult(None, None, None, None, margin, windows_read, passages_read, limited)
        margin, passage, left, right = best
        return ReaderResult(passage.content[left:right], passage, left, right, margin,
                            windows_read, passages_read, limited)


class _TorchPredictor:
    def __init__(self, tokenizer: Any, model: Any, torch_module: Any, device: Any) -> None:
        """Store the loaded inference components."""
        self.tokenizer = tokenizer
        self.model = model
        self.torch = torch_module
        self.device = device

    def __call__(self, features: list[dict]) -> tuple[np.ndarray, np.ndarray]:
        """Run a padded reader batch."""
        inputs = self.tokenizer.pad(features, padding=True, return_tensors="pt").to(self.device)
        with self.torch.inference_mode():
            outputs = self.model(**inputs)
        return outputs.start_logits.float().cpu().numpy(), outputs.end_logits.float().cpu().numpy()


def load_reader(
    model_name: str = "deepset/tinyroberta-squad2", device: str = "cuda",
    max_windows: int = 10, no_answer_threshold: float = 0.0, batch_size: int = 10,
) -> ExtractiveReader:
    """Load the cached answer reader once."""
    import torch
    from transformers import AutoModelForQuestionAnswering, AutoTokenizer
    target = torch.device(device)
    if target.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA is unavailable; install CUDA-enabled PyTorch or select --device cpu.")
    tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True, local_files_only=True)
    model = AutoModelForQuestionAnswering.from_pretrained(model_name, local_files_only=True)
    model.to(target).eval()
    reader = ExtractiveReader(tokenizer, _TorchPredictor(tokenizer, model, torch, target),
                              max_windows=max_windows, no_answer_threshold=no_answer_threshold,
                              batch_size=batch_size)
    reader.device = str(target)
    return reader
