import json
from pathlib import Path
from typing import Any

import numpy as np

from .benchmark import _range_count, answer_scores, get_score_ranges, load_questions, print_score_ranges
from .qa import OnlineQA, build_parser, load_from_args


def tune_weights(
    pipeline: OnlineQA, records: list[dict[str, Any]], weights: list[float], score_ranges: int = 20,
) -> dict[str, Any]:
    """Choose a dense weight on development answers."""
    values = np.asarray(weights, dtype=np.float64)
    if values.ndim != 1 or not len(values) or not np.isfinite(values).all() or (values < 0).any():
        raise ValueError("weights must contain finite nonnegative values.")
    if not records:
        raise ValueError("Development questions are required.")
    score_ranges = _range_count(score_ranges)
    original_weight = pipeline.retriever.dense_weight
    results = []
    diagnostics = []
    range_questions = set()
    completed = False
    best = None
    try:
        for weight in sorted(set(values.tolist())):
            pipeline.retriever.dense_weight = weight
            exact_scores = []
            f1_scores = []
            limited = []
            passages = []
            for row in records:
                result = pipeline.answer(row["question"])
                exact, f1 = answer_scores(result.answer, row["answers"])
                exact_scores.append(exact)
                f1_scores.append(f1)
                limited.append(result.coverage_limited)
                passages.append(result.passages_read)
                ranges = get_score_ranges(pipeline)
                if ranges is not None and row["question"] not in range_questions and len(diagnostics) < score_ranges:
                    print_score_ranges(row["question"], ranges)
                    diagnostics.append({"question": row["question"], **ranges})
                    range_questions.add(row["question"])
            results.append({
                "dense_weight": weight,
                "exact_match_percent": 100 * float(np.mean(exact_scores)),
                "token_f1_percent": 100 * float(np.mean(f1_scores)),
                "coverage_limited_fraction": float(np.mean(limited)),
                "mean_passages_read": float(np.mean(passages)),
            })
        best = max(results, key=lambda row: (
            row["exact_match_percent"], row["token_f1_percent"], -row["dense_weight"],
        ))
        completed = True
    finally:
        pipeline.retriever.dense_weight = best["dense_weight"] if completed else original_weight
    return {
        "dev_count": len(records), "best_dense_weight": best["dense_weight"],
        "selection_metrics": ["exact_match_percent", "token_f1_percent", "lower_dense_weight"],
        "score_normalization": "per_query_z_score", "results": results,
        "score_ranges": diagnostics,
    }


def main() -> None:
    """Tune fusion weights on dev.jsonl."""
    parser = build_parser(include_question=False)
    parser.description = "Tune the normalized hybrid weight on labeled development questions."
    parser.add_argument("--dev", required=True, type=Path)
    parser.add_argument("--weights", nargs="+", type=float, default=[0.0, 0.5, 1.0, 1.1, 2.0])
    parser.add_argument("--score-ranges", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        records = load_questions(args.dev)
        pipeline = load_from_args(args)
        report = tune_weights(pipeline, records, args.weights, score_ranges=args.score_ranges)
        report["configuration"] = {
            "dev": str(args.dev), "query_model": args.query_model, "reader_model": args.reader_model,
            "device": args.device, "candidates": args.candidates, "retrieval_k": args.k,
            "max_windows": args.max_windows, "batch_size": args.batch_size,
            "no_answer_threshold": args.no_answer_threshold,
        }
        serialized = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
        if args.output is not None:
            args.output.write_text(serialized + "\n", encoding="utf-8")
        print(serialized)
    except (ValueError, OSError, ImportError, RuntimeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
