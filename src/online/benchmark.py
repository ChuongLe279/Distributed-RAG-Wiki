from collections import Counter
from copy import deepcopy
import json
from pathlib import Path
import re
import string
import sys
from time import perf_counter
from typing import Any

import numpy as np

from .document_retrieval import _positive_count
from .qa import OnlineQA, build_parser, load_from_args


def load_questions(path: Path) -> list[dict[str, Any]]:
    """Read labeled evaluation questions from JSONL."""
    records = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or not isinstance(row.get("question"), str) or not row["question"].strip():
            raise ValueError(f"Line {line_number} requires a nonempty question.")
        answers = row.get("answers")
        if not isinstance(answers, list) or any(not isinstance(answer, str) or not answer.strip() for answer in answers):
            raise ValueError(f"Line {line_number} requires nonempty answer strings or answers=[].")
        records.append({"question": row["question"].strip(), "answers": answers})
    if len(records) < 20:
        raise ValueError("Provide at least 20 representative labeled questions.")
    if not any(row["answers"] for row in records) or not any(not row["answers"] for row in records):
        raise ValueError("Include both answerable and unanswerable questions.")
    return records


def normalize_answer(text: str) -> str:
    """Normalize answers using SQuAD conventions."""
    text = "".join(character for character in text.lower() if character not in string.punctuation)
    return " ".join(re.sub(r"\b(a|an|the)\b", " ", text).split())


def answer_scores(prediction: str | None, answers: list[str]) -> tuple[float, float]:
    """Calculate exact match and token F1."""
    predicted = normalize_answer(prediction or "")
    predicted_tokens = predicted.split()
    exact = 0.0
    f1 = 0.0
    for answer in answers or [""]:
        reference = normalize_answer(answer)
        reference_tokens = reference.split()
        exact = max(exact, float(predicted == reference))
        overlap = sum((Counter(predicted_tokens) & Counter(reference_tokens)).values())
        if not predicted_tokens or not reference_tokens:
            candidate_f1 = float(predicted_tokens == reference_tokens)
        else:
            candidate_f1 = 2 * overlap / (len(predicted_tokens) + len(reference_tokens))
        f1 = max(f1, candidate_f1)
    return exact, f1


def _synchronize(pipeline: OnlineQA) -> None:
    """Wait for the reader's CUDA work."""
    torch = sys.modules.get("torch")
    device = getattr(pipeline.reader, "device", "cpu")
    if torch is not None and str(device).startswith("cuda"):
        torch.cuda.synchronize(device)


def get_score_ranges(pipeline: OnlineQA) -> dict[str, Any] | None:
    """Copy the latest candidate score ranges."""
    ranges = getattr(pipeline.retriever, "last_score_ranges", None)
    return deepcopy(ranges) if isinstance(ranges, dict) else None


def print_score_ranges(question: str, ranges: dict[str, Any]) -> None:
    """Print candidate ranges to stderr."""
    print(json.dumps({"question": question, **ranges}, ensure_ascii=False, allow_nan=False), file=sys.stderr)


def _range_count(value: int) -> int:
    """Validate the score-range print limit."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("score_ranges must be a nonnegative integer.")
    return value


def run_benchmark(
    pipeline: OnlineQA, records: list[dict[str, Any]], warmup: int = 3, repeats: int = 1,
    score_ranges: int = 20,
) -> dict[str, Any]:
    """Measure warmed latency and answer quality."""
    repeats = _positive_count(repeats, "repeats")
    score_ranges = _range_count(score_ranges)
    if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup < 0:
        raise ValueError("warmup must be a nonnegative integer.")
    if not records:
        raise ValueError("Evaluation questions are required.")
    for index in range(warmup):
        pipeline.answer(records[index % len(records)]["question"])
    _synchronize(pipeline)
    measurements = []
    range_questions = set()
    ranges_printed = 0
    for repetition in range(repeats):
        for row in records:
            _synchronize(pipeline)
            started = perf_counter()
            result = pipeline.answer(row["question"])
            _synchronize(pipeline)
            elapsed = perf_counter() - started
            ranges = get_score_ranges(pipeline)
            if ranges is not None and row["question"] not in range_questions and ranges_printed < score_ranges:
                print_score_ranges(row["question"], ranges)
                range_questions.add(row["question"])
                ranges_printed += 1
            exact, f1 = answer_scores(result.answer, row["answers"])
            measurements.append({
                "question": row["question"], "answers": row["answers"], "prediction": result.answer,
                "repetition": repetition + 1, "latency_seconds": elapsed,
                "exact_match": exact, "token_f1": f1,
                "answerability_correct": bool(result.answer) == bool(row["answers"]),
                "score_margin": result.score_margin, "windows_read": result.windows_read,
                "passages_read": result.passages_read, "coverage_limited": result.coverage_limited,
                "source_vector_id": result.source.vector_id if result.source is not None else None,
                "score_ranges": ranges,
            })
    timings = np.asarray([row["latency_seconds"] for row in measurements])
    p50, p95, p99 = np.percentile(timings, [50, 95, 99])
    under_target = float(np.mean(timings < 2.0))
    unanswerable = [row for row in measurements if not row["answers"]]
    answerable = [row for row in measurements if row["answers"]]
    return {
        "question_count": len(records), "request_count": len(measurements),
        "warmup_requests": warmup, "repeats": repeats, "target_seconds": 2.0,
        "score_range_queries_printed": ranges_printed,
        "coverage_limited_fraction": float(np.mean([row["coverage_limited"] for row in measurements])),
        "mean_passages_read": float(np.mean([row["passages_read"] for row in measurements])),
        "min_passages_read": min(row["passages_read"] for row in measurements),
        "mean_windows_read": float(np.mean([row["windows_read"] for row in measurements])),
        "latency_seconds": {"p50": float(p50), "p95": float(p95), "p99": float(p99)},
        "fraction_under_2s": under_target, "p95_under_2s": bool(p95 < 2.0),
        "meets_latency_target": bool(p95 < 2.0 and under_target >= 0.95),
        "exact_match_percent": 100 * float(np.mean([row["exact_match"] for row in measurements])),
        "token_f1_percent": 100 * float(np.mean([row["token_f1"] for row in measurements])),
        "answerability_accuracy_percent": 100 * float(np.mean([
            row["answerability_correct"] for row in measurements
        ])),
        "unanswerable_abstention_percent": (
            100 * float(np.mean([not row["prediction"] for row in unanswerable])) if unanswerable else None
        ),
        "answerable_answer_rate_percent": (
            100 * float(np.mean([bool(row["prediction"]) for row in answerable])) if answerable else None
        ),
        "measurements": measurements,
    }


def main() -> None:
    """Benchmark the offline snapshot."""
    parser = build_parser(include_question=False)
    parser.description = "Measure warmed QA latency and quality on labeled questions."
    parser.add_argument("--questions", required=True, type=Path)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--score-ranges", type=int, default=20)
    args = parser.parse_args()
    try:
        records = load_questions(args.questions)
        pipeline = load_from_args(args)
        report = run_benchmark(pipeline, records, warmup=args.warmup, repeats=args.repeats,
                               score_ranges=args.score_ranges)
        report["configuration"] = {
            "query_model": args.query_model, "reader_model": args.reader_model,
            "device": args.device, "index_directory": str(args.index_dir),
            "vector_map": str(args.vector_map), "candidates": args.candidates,
            "dense_weight": args.dense_weight, "retrieval_k": args.k,
            "max_windows": args.max_windows, "batch_size": args.batch_size,
            "no_answer_threshold": args.no_answer_threshold,
            "score_normalization": "per_query_z_score",
        }
        serialized = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
        if args.output is not None:
            args.output.write_text(serialized + "\n", encoding="utf-8")
        print(serialized)
    except (ValueError, OSError, ImportError, RuntimeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
