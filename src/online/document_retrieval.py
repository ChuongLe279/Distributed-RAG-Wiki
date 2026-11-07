import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .lexical import BM25Index, top_indices
from .query_encoding import encode_question


@dataclass(frozen=True)
class IndexedChunk:
    vector_id: int
    article_id: int
    revision_id: int
    chunk_id: int
    title: str
    content: str


@dataclass(frozen=True)
class RetrievalResult:
    vector_id: int
    article_id: int
    revision_id: int
    chunk_id: int
    title: str
    content: str
    rank: int
    dense_score: float
    bm25_score: float
    hybrid_score: float


def zscore(values: np.ndarray) -> np.ndarray:
    """Standardize scores within one candidate set."""
    values = np.asarray(values, dtype=np.float64)
    if not values.size:
        return values.copy()
    return (values - values.mean()) / (values.std() + 1e-9)


def _score_range(values: np.ndarray) -> dict[str, float | None]:
    """Summarize one candidate score array."""
    if not values.size:
        return {"min": None, "max": None, "std": None}
    return {"min": float(values.min()), "max": float(values.max()), "std": float(values.std())}


def _identifier(value: Any, field: str) -> int:
    """Validate and return an integer ID."""
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer.")
    number = int(value)
    if field == "vector_id" and not 0 < number <= np.iinfo(np.uint32).max:
        raise ValueError("vector_id must be a positive uint32 DiskANN tag.")
    return number


def _positive_count(value: Any, field: str) -> int:
    """Validate a positive search limit."""
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)) or value <= 0:
        raise ValueError(f"{field} must be a positive integer.")
    return int(value)


def _mapping_row(row: Any) -> dict[str, int]:
    """Validate vector-to-chunk IDs."""
    fields = ["vector_id", "article_id", "revision_id", "chunk_id"]
    if not isinstance(row, dict) or not set(fields).issubset(row):
        raise ValueError("Each mapping row requires vector_id, article_id, revision_id, and chunk_id.")
    return {field: _identifier(row[field], field) for field in fields}


def _read_partition(root: Path, folder: str, columns: list[str]) -> pd.DataFrame:
    """Read selected columns from Parquet partitions."""
    files = sorted((root / folder).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {root / folder}.")
    return pd.concat([pd.read_parquet(path, columns=columns) for path in files], ignore_index=True)


def load_chunks(parquet_dir: Path, vector_map_path: Path) -> list[IndexedChunk]:
    """Load chunks matched to DiskANN tags."""
    if not vector_map_path.is_file():
        raise FileNotFoundError(f"Missing vector-to-chunk mapping: {vector_map_path}. Export it at insertion time.")
    keys = ["article_id", "revision_id", "chunk_id"]
    mapping = [json.loads(line) for line in vector_map_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not mapping:
        raise ValueError("The vector-to-chunk mapping is empty.")
    mapped = [_mapping_row(row) for row in mapping]
    map_frame = pd.DataFrame(mapped)
    if map_frame["vector_id"].duplicated().any() or map_frame.duplicated(keys).any():
        raise ValueError("The mapping must uniquely pair each vector_id and chunk identity.")
    articles = _read_partition(parquet_dir, "articles", ["article_id", "revision_id", "title"]).drop_duplicates()
    chunks = _read_partition(parquet_dir, "chunks", [*keys, "content"]).drop_duplicates()
    if articles.duplicated(keys[:2]).any() or chunks.duplicated(keys).any():
        raise ValueError("Conflicting Parquet rows share an article revision or chunk identity.")
    joined = map_frame.merge(chunks, on=keys, how="left", validate="one_to_one", indicator=True)
    if not joined["_merge"].eq("both").all():
        raise ValueError("A mapped vector_id has no matching chunk in the Parquet snapshot.")
    joined = joined.drop(columns="_merge").merge(articles, on=keys[:2], how="left", validate="many_to_one")
    if joined[["title", "content"]].isna().any().any():
        raise ValueError("A mapped chunk has no source article title or content.")
    if not all(isinstance(value, str) and value.strip() for value in joined["content"]):
        raise ValueError("Mapped chunk content must be nonempty text.")
    return [IndexedChunk(**row) for row in joined.to_dict(orient="records")]


def _load_vectors(index_directory: Path, index_prefix: str, chunks: list[IndexedChunk], diskann: Any) -> np.ndarray:
    """Align saved vectors with chunk IDs."""
    tags = np.asarray(diskann.tags_from_file(str(index_directory / f"{index_prefix}.tags")))
    vectors = diskann.vectors_from_file(str(index_directory / f"{index_prefix}.data"), dtype=np.float32)
    if tags.ndim != 1 or vectors.ndim != 2 or len(tags) != len(vectors):
        raise ValueError("DiskANN vector and tag files have incompatible shapes.")
    tagged_rows = {int(tag): row for row, tag in enumerate(tags) if tag != 0}
    if len(tagged_rows) != int(np.count_nonzero(tags)):
        raise ValueError("DiskANN contains duplicate nonzero tags.")
    expected = {chunk.vector_id for chunk in chunks}
    if set(tagged_rows) != expected:
        raise ValueError("DiskANN tags and the vector-to-chunk mapping describe different snapshots.")
    return vectors[[tagged_rows[chunk.vector_id] for chunk in chunks]]


class HybridRetriever:
    def __init__(
        self, chunks: list[IndexedChunk], vectors: np.ndarray, dense_index: Any, encoder: Any,
        dense_weight: float = 1.1, candidate_count: int = 2000, search_complexity: int = 128,
    ) -> None:
        """Initialize hybrid search components."""
        if not chunks:
            raise ValueError("At least one indexed chunk is required.")
        candidate_count = _positive_count(candidate_count, "candidate_count")
        search_complexity = _positive_count(search_complexity, "search_complexity")
        if not np.isfinite(dense_weight) or dense_weight < 0:
            raise ValueError("dense_weight must be finite and nonnegative.")
        self.chunks = chunks
        self.vector_rows = {_identifier(chunk.vector_id, "vector_id"): row for row, chunk in enumerate(chunks)}
        if len(self.vector_rows) != len(chunks):
            raise ValueError("Chunk vector IDs must be unique.")
        self.vectors = np.asarray(vectors, dtype=np.float32)
        if self.vectors.shape != (len(chunks), 768) or not np.isfinite(self.vectors).all():
            raise ValueError("Expected one finite 768-dimensional E5 vector per chunk.")
        if not np.allclose(np.linalg.norm(self.vectors, axis=1), 1.0, atol=1e-3):
            raise ValueError("Saved E5 passage vectors must already be normalized.")
        self.dense_index = dense_index
        self.encoder = encoder
        self.dense_weight = dense_weight
        self.candidate_count = candidate_count
        self.search_complexity = search_complexity
        self.lexical = BM25Index([f"{chunk.title} {chunk.content}" for chunk in chunks], k1=0.9, b=0.4)
        self.last_score_ranges = None

    def retrieve(self, question: str, k: int = 20) -> list[RetrievalResult]:
        """Retrieve and rank chunks with hybrid scores."""
        if not isinstance(question, str) or not question.strip():
            raise ValueError("A nonempty question is required.")
        k = _positive_count(k, "k")
        question = question.strip()
        query = encode_question(question, self.encoder)
        count = min(self.candidate_count, len(self.chunks))
        response = self.dense_index.search(
            query, k_neighbors=count, complexity=max(self.search_complexity, count),
        )
        dense_ids = {_identifier(tag, "vector_id") for tag in response.identifiers}
        if not dense_ids.issubset(self.vector_rows):
            raise ValueError("DiskANN returned an unmapped vector_id; check the offline snapshot mapping.")
        lexical_scores = self.lexical.scores(question)
        lexical_rows = top_indices(lexical_scores, count)
        candidate_ids = sorted(dense_ids | {self.chunks[row].vector_id for row in lexical_rows})
        rows = np.array([self.vector_rows[tag] for tag in candidate_ids], dtype=np.int64)
        dense_scores = self.vectors[rows] @ query
        bm25_scores = lexical_scores[rows]
        dense_z = zscore(dense_scores)
        bm25_z = zscore(bm25_scores)
        hybrid_scores = bm25_z + self.dense_weight * dense_z
        self.last_score_ranges = {
            "candidate_count": len(candidate_ids), "dense": _score_range(dense_scores),
            "bm25": _score_range(bm25_scores), "dense_z": _score_range(dense_z),
            "bm25_z": _score_range(bm25_z),
        }
        order = np.argsort(-hybrid_scores, kind="stable")[:k]
        return [
            RetrievalResult(
                **asdict(self.chunks[rows[position]]), rank=rank,
                dense_score=float(dense_scores[position]), bm25_score=float(bm25_scores[position]),
                hybrid_score=float(hybrid_scores[position]),
            )
            for rank, position in enumerate(order, 1)
        ]


def load_retriever(
    index_directory: Path, parquet_directory: Path, vector_map_path: Path,
    model_name: str = "intfloat/e5-base-v2", index_prefix: str = "e5-base-v2",
    dense_weight: float = 1.1, candidate_count: int = 2000, search_complexity: int = 128,
    device: str = "cpu",
) -> HybridRetriever:
    """Load the offline index and cached encoder."""
    candidate_count = _positive_count(candidate_count, "candidate_count")
    search_complexity = _positive_count(search_complexity, "search_complexity")
    chunks = load_chunks(parquet_directory, vector_map_path)
    for suffix in ["", ".data", ".tags"]:
        path = index_directory / f"{index_prefix}{suffix}"
        if not path.is_file():
            raise FileNotFoundError(f"Missing DiskANN artifact: {path}.")
    import diskannpy
    from sentence_transformers import SentenceTransformer
    vectors = _load_vectors(index_directory, index_prefix, chunks, diskannpy)
    index = diskannpy.DynamicMemoryIndex.from_file(
        index_directory=str(index_directory), max_vectors=max(100_000, len(chunks)),
        complexity=64, graph_degree=32, index_prefix=index_prefix,
        distance_metric="cosine", vector_dtype=np.float32, dimensions=768,
        initial_search_complexity=max(search_complexity, min(candidate_count, len(chunks))),
        search_threads=1,
    )
    encoder = SentenceTransformer(model_name, device=device, local_files_only=True)
    return HybridRetriever(chunks, vectors, index, encoder, dense_weight, candidate_count, search_complexity)


def main() -> None:
    """Run retrieval and print JSON results."""
    data_directory = Path(__file__).resolve().parents[1] / "data"
    parser = argparse.ArgumentParser(description="Online hybrid retrieval over the offline E5/DiskANN snapshot.")
    parser.add_argument("--question", required=True)
    parser.add_argument("--vector-map", required=True, type=Path)
    parser.add_argument("--index-dir", type=Path, default=data_directory / "vectorstore")
    parser.add_argument("--parquet-dir", type=Path, default=data_directory / "parquet")
    parser.add_argument("--model", default="intfloat/e5-base-v2", help="The exact checkpoint used by offline encoding.")
    parser.add_argument("--index-prefix", default="e5-base-v2")
    parser.add_argument("--k", type=int, default=20)
    parser.add_argument("--candidates", type=int, default=2000)
    parser.add_argument("--dense-weight", type=float, default=1.1)
    parser.add_argument("--device", default="cpu")
    args = parser.parse_args()
    try:
        retriever = load_retriever(
            args.index_dir, args.parquet_dir, args.vector_map, args.model, args.index_prefix,
            args.dense_weight, args.candidates, device=args.device,
        )
        results = retriever.retrieve(args.question, args.k)
    except (FileNotFoundError, ValueError, ImportError, OSError) as error:
        parser.error(str(error))
    print(json.dumps([asdict(result) for result in results], ensure_ascii=False))


if __name__ == "__main__":
    main()

