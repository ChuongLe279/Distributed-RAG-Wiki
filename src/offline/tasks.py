from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from models import Article, Chunk
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import asdict
import pandas as pd
from uuid import uuid4
import requests
import re
import diskannpy
from sentence_transformers import SentenceTransformer
import numpy as np


def split_chunk(article: Article, num: int) -> list[Chunk]:
    """Chia chunk for article + gán id.
    Args:
        article: bài wiki.
        num: 1 chunk có bao nhiêu từ.
    """
    from models import Chunk

    if num <= 0: print("Chunk size phải > 0.")
    article_content = article.content.split()
    chunks = []
    for index, start in enumerate((range(0, len(article_content), num))):
        content = content=" ".join(article_content[start:start + num])
        chunk_id = index
        article_id=article.article_id
        revision_id=article.revision_id
        chunks.append(Chunk(chunk_id, article_id, revision_id, content))
    return chunks
    
def clean_article(article: Article) -> Article:
    """Làm sạch content và trả về bài đã làm sạch.
    Sẽ làm những bước sau đây:
        1. Loại bỏ các thư mục trống (Do hàm fetch không fetch được bảng, hình ảnh, biểu đồ, vv.)
        2. Bỏ dấu cách cuối bằng strip()
        3. Xóa dòng trống bị thừa.
        4. Thêm title vào content.
    """
    text = article.content
    title = article.title
    text = "Title: " + title + "\n" + text
    text = text.strip()
    text = re.sub(r'\n{2,}', '\n', text)

    article.content = text
    pattern = (
        r"^==[ \t]*[^=\n]+[ \t]*==[ \t]*"
        r"(?:\n[ \t]*)*"
        r"(?=^==[ \t]*[^=\n]+[ \t]*==[ \t]*$|\Z)"
    )
    re.sub(pattern, "", text, flags=re.MULTILINE).strip()
    return article

def fetch_newest_article(api: str, header: dict) -> Article:
    """
    Lấy wiki mới nhất. Cho updated data = created date.
    Luồng hoạt động của hàm:
        Lấy danh sách các bài mới tạo.
        Lấy nội dung của bài mới nhất.
    """
    from models import Article

    params = {
        "action": "query",
        "format": "json",
        "list": "recentchanges",
        "rctype": "new",
        "rcshow": "!redirect",
        "rcnamespace": 0,
        "rclimit": 1,
        "rcprop": "title|timestamp|user|ids",
    }
    
    # Lấy danh sách bài mới nhất
    r = requests.get(api, params=params, headers=header, timeout=20)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"])

    changes = data["query"]["recentchanges"]

    # Lấy bài mới nhất trong dánh sách
    for change in changes:
        extract_params = {
            "action": "query",
            "format": "json",
            "prop": "extracts",
            "explaintext": 1,
            "redirects": 1,
            "pageids": change["pageid"],
        }
        r = requests.get(api, params=extract_params, headers=header, timeout=20)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise RuntimeError(data["error"])

        page = next(iter(data["query"]["pages"].values()))
        if "missing" in page or not page.get("extract"):
            continue
        created_date = datetime.fromisoformat(change["timestamp"].replace("Z", "+00:00")).date()

        return Article(
            article_id=page["pageid"],
            revision_id=change["revid"],
            created_date=created_date,
            updated_date=created_date,
            title=page["title"],
            content=page["extract"],
        )

    raise RuntimeError("Không tìm thấy bài viết mới có nội dung.")
            
def embed_chunks(article: Article, chunk: Chunk, model: SentenceTransformer, batch_size: int) -> np.ndarray:
    """Tạo embedding cho 1 chunk duy nhất."""
    if chunk is None:
        raise ValueError("Chunk không được là None.")
    # Kiểm tra id của chunk trước khi index.
    validate_chunk(article, chunk)

    if not chunk: return np.empty((0, 768), dtype=np.float32)
    # Embedding
    vec = model.encode(chunk.content, batch_size = batch_size, normalize_embeddings=True).astype(np.float32)
    return vec

def index_chunks(vectors: np.ndarray, vector_store: str, next_id: int) -> int:
    """ghi các chunk vào chỉ mục vector."""
    root = Path(vector_store)
    root.mkdir(parents=True, exist_ok=True)
    index = diskannpy.DynamicMemoryIndex(
        distance_metric="cosine",
        vector_dtype=np.float32,
        dimensions=768, # model: "intfloat/e5-base-v2"
        max_vectors=100_000,
        complexity=64,
        graph_degree=32,
    )
    for vec in vectors:
        index.insert(vec, vector_id=np.uint32(next_id))
        next_id += 1
    # Nhét vào vector store
    index.save(save_path=str(root), index_prefix="e5-base-v2")
    # Phải trả lại next id vì id các chunk giữa các bài báo không được trùng nhau.
    return next_id

def validate_chunk(article: Article, chunk: Chunk):
    """Kiểm tra định dạng id của chunk trước khi index."""
    if chunk.article_id != article.article_id:
        raise ValueError("Chunk không thuộc bài viết này.")

    if chunk.revision_id != article.revision_id:
        raise ValueError("Revision của chunk không khớp bài viết.")

    if not chunk.content.strip():
        raise ValueError(f"Chunk {chunk.chunk_id} không có nội dung.")


def save_parquet(articles: list[Article], parquet_folder: str, batch_size: int = 1000) -> None:
    """
    Ghi một lô bài đã làm sạch vào Parquet. Lưu theo kiểu HIVE.
    Lưu cả chunk và article.
    articles
        date
            batch-01.parquet
            batch-02.parquet
    chunks
        date
            batch-01.parquet
            batch-02.parquet
    """
    if batch_size <= 0:
        raise ValueError("batch_size phải > 0.")
    if not articles:
        return

    ingest_date = datetime.now(timezone.utc).date().isoformat()
    root = Path(parquet_folder)

    article_folder = root / "articles" / f"{ingest_date}"
    chunk_folder = root / "chunks" / f"{ingest_date}"
    article_folder.mkdir(parents=True, exist_ok=True)
    chunk_folder.mkdir(parents=True, exist_ok=True)
    for start in range(0, len(articles), batch_size):
        batch = articles[start:start + batch_size]
        batch_id = uuid4().hex
        article_rows = [asdict(article) for article in batch]
        chunk_rows = []
        for article in batch:
            chunks = split_chunk(article, num=200)
            chunk_rows.extend(asdict(chunk) for chunk in chunks)
        pd.DataFrame(article_rows).to_parquet(
            article_folder / f"{batch_id}.parquet",
            engine="pyarrow",
            compression="snappy", # Dùng để giảm dung lượng
            index=False,
        )
        if chunk_rows:
            pd.DataFrame(chunk_rows).to_parquet(
                chunk_folder / f"{batch_id}.parquet",
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

def delete_parquet():
    """Chưa nghĩ ra thuật toán để tìm parquet."""
    raise NotImplementedError



    
    
