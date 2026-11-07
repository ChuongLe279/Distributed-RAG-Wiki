import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

from .document_retrieval import HybridRetriever, _positive_count, load_retriever
from .answer_extraction import ExtractiveReader, ReaderResult, load_reader


class OnlineQA:
    def __init__(
        self, retriever: HybridRetriever, reader: ExtractiveReader, retrieval_k: int = 10,
    ) -> None:
        """Connect retrieval and answer extraction."""
        self.retrieval_k = _positive_count(retrieval_k, "retrieval_k")
        self.retriever = retriever
        self.reader = reader

    def answer(self, question: str) -> ReaderResult:
        """Extract a sourced answer or abstain."""
        if not isinstance(question, str) or not question.strip():
            raise ValueError("A nonempty question is required.")
        question = question.strip()
        passages = self.retriever.retrieve(question, k=self.retrieval_k)
        return self.reader.read(question, passages)


def load_qa(
    index_directory: Path, parquet_directory: Path, vector_map_path: Path,
    query_model: str = "intfloat/e5-base-v2", reader_model: str = "deepset/tinyroberta-squad2",
    index_prefix: str = "e5-base-v2", device: str = "cuda", candidate_count: int = 2000,
    dense_weight: float = 1.1, retrieval_k: int = 10, max_windows: int = 10,
    no_answer_threshold: float = 0.0, batch_size: int = 10,
) -> OnlineQA:
    """Load reusable retrieval and reader models."""
    retrieval_k = _positive_count(retrieval_k, "retrieval_k")
    retriever = load_retriever(
        index_directory=index_directory, parquet_directory=parquet_directory,
        vector_map_path=vector_map_path, model_name=query_model, index_prefix=index_prefix,
        dense_weight=dense_weight, candidate_count=candidate_count, device=device,
    )
    reader = load_reader(
        model_name=reader_model, device=device, max_windows=max_windows,
        no_answer_threshold=no_answer_threshold, batch_size=batch_size,
    )
    return OnlineQA(retriever, reader, retrieval_k)


def build_parser(include_question: bool = True) -> argparse.ArgumentParser:
    """Build shared online QA arguments."""
    data_directory = Path(__file__).resolve().parents[1] / "data"
    parser = argparse.ArgumentParser(description="Extract sourced answers from the offline snapshot.")
    if include_question:
        request = parser.add_mutually_exclusive_group(required=True)
        request.add_argument("--question")
        request.add_argument("--interactive", action="store_true", help="Read one question per stdin line.")
    parser.add_argument("--vector-map", type=Path, required=True)
    parser.add_argument("--index-dir", type=Path, default=data_directory / "vectorstore")
    parser.add_argument("--parquet-dir", type=Path, default=data_directory / "parquet")
    parser.add_argument("--query-model", default="intfloat/e5-base-v2")
    parser.add_argument("--reader-model", default="deepset/tinyroberta-squad2")
    parser.add_argument("--index-prefix", default="e5-base-v2")
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--candidates", type=int, default=2000)
    parser.add_argument("--dense-weight", type=float, default=1.1)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--max-windows", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--no-answer-threshold", type=float, default=0.0)
    return parser


def load_from_args(args: argparse.Namespace) -> OnlineQA:
    """Load QA using parsed arguments."""
    return load_qa(
        index_directory=args.index_dir, parquet_directory=args.parquet_dir,
        vector_map_path=args.vector_map, query_model=args.query_model,
        reader_model=args.reader_model, index_prefix=args.index_prefix, device=args.device,
        candidate_count=args.candidates, dense_weight=args.dense_weight, retrieval_k=args.k,
        max_windows=args.max_windows, no_answer_threshold=args.no_answer_threshold,
        batch_size=args.batch_size,
    )


def main() -> None:
    """Run one query or a persistent session."""
    parser = build_parser()
    args = parser.parse_args()
    try:
        pipeline = load_from_args(args)
        if not args.interactive:
            print(json.dumps(asdict(pipeline.answer(args.question)), ensure_ascii=False))
            return
    except (ValueError, OSError, ImportError, RuntimeError) as error:
        parser.error(str(error))
    for line in sys.stdin:
        try:
            result = asdict(pipeline.answer(line.rstrip("\r\n")))
        except (ValueError, RuntimeError) as error:
            result = {"error": str(error)}
        print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
