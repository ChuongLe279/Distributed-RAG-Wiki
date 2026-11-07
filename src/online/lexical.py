from collections import Counter
from collections.abc import Sequence
import math
import re

import numpy as np


QUERY_STOPWORDS = frozenset(
    "a an the and or of in on at to for from by with as is are was were be been being "
    "this that these those it its their his her our your what which who whom when "
    "where why how do does did have has had can could would should will shall may might".split()
)


def _tokenize(text: str) -> list[str]:
    """Extract lowercase alphanumeric tokens."""
    return re.findall(r"[^\W_]+", text.lower())


def top_indices(scores: np.ndarray, count: int) -> np.ndarray:
    """Select positive scores with stable ties."""
    values = np.asarray(scores, dtype=np.float64)
    if values.ndim != 1 or not np.isfinite(values).all():
        raise ValueError("scores must be a finite one-dimensional array.")
    if isinstance(count, bool) or not isinstance(count, (int, np.integer)) or count <= 0:
        raise ValueError("count must be a positive integer.")
    rows = np.flatnonzero(values > 0)
    if len(rows) > count:
        positive = values[rows]
        boundary = len(rows) - count
        partition = np.argpartition(positive, boundary)
        threshold = positive[partition[boundary]]
        higher = rows[positive > threshold]
        equal = rows[positive == threshold][:count - len(higher)]
        rows = np.concatenate((higher, equal))
    return rows[np.lexsort((rows, -values[rows]))]


class BM25Index:
    """Store BM25 postings in memory."""

    def __init__(self, texts: Sequence[str], k1: float = 0.9, b: float = 0.4) -> None:
        """Build postings and length normalizers."""
        if not math.isfinite(k1) or k1 <= 0:
            raise ValueError("k1 must be finite and greater than zero.")
        if not math.isfinite(b) or not 0 <= b <= 1:
            raise ValueError("b must be finite and between zero and one.")
        if isinstance(texts, str):
            raise TypeError("texts must be a sequence of strings, not one string.")
        self._k1 = float(k1)
        self._document_count = len(texts)
        lengths = np.zeros(self._document_count, dtype=np.float64)
        postings: dict[str, tuple[list[int], list[int]]] = {}
        for document_id, text in enumerate(texts):
            if not isinstance(text, str):
                raise TypeError("texts must contain only strings.")
            frequencies = Counter(_tokenize(text))
            lengths[document_id] = sum(frequencies.values())
            for term, frequency in frequencies.items():
                ids, counts = postings.setdefault(term, ([], []))
                ids.append(document_id)
                counts.append(frequency)
        average_length = float(lengths.mean()) if self._document_count else 0.0
        self._normalizers = (
            self._k1 * (1 - b + b * lengths / average_length)
            if average_length else np.zeros_like(lengths)
        )
        self._postings: dict[str, tuple[np.ndarray, np.ndarray, float]] = {}
        for term, (ids, counts) in postings.items():
            inverse_frequency = math.log1p(
                (self._document_count - len(ids) + 0.5) / (len(ids) + 0.5)
            )
            self._postings[term] = (
                np.asarray(ids, dtype=np.intp),
                np.asarray(counts, dtype=np.float64),
                inverse_frequency,
            )

    def scores(self, question: str) -> np.ndarray:
        """Return BM25 scores in passage order."""
        scores = np.zeros(self._document_count, dtype=np.float64)
        for term in sorted(set(_tokenize(question)) - QUERY_STOPWORDS):
            posting = self._postings.get(term)
            if posting is None:
                continue
            document_ids, frequencies, inverse_frequency = posting
            scores[document_ids] += (
                inverse_frequency * frequencies * (self._k1 + 1)
                / (frequencies + self._normalizers[document_ids])
            )
        return scores
