from __future__ import annotations

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from models import Article, Chunk
from sentence_transformers import SentenceTransformer
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import re
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, trim, posexplode
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    ArrayType
)
from pyspark.sql import DataFrame

def create_article_dataframe(xml_data):
    """
    Đọc Wikipedia XML dump thành Spark DataFrame.
    xml_data: đường dẫn tới file .xml.
    Dataframe sẽ có dạng:
        article_id
        revision_id
        title
        content
    """
    spark = SparkSession.builder.getOrCreate()


    page_schema = StructType([
        StructField("id", LongType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("ns", IntegerType(), nullable=True),
        StructField("revision", StructType([
            StructField("id", LongType(), nullable=True),
            StructField("text", StructType([
                StructField("_VALUE", StringType(), nullable=True),
            ]), nullable=True),
        ]), nullable=True),
    ])

    pages = (
        spark.read
        .format("xml")
        .option("rowTag", "page")
        .option("valueTag", "_VALUE")
        .option("mode", "FAILFAST")
        .schema(page_schema)
        .load(str(xml_data))
    )

    # Lọc trước khi chọn cột
    articles = pages.filter(col("ns") == 0).select(
        col("id").alias("article_id"),
        col("revision.id").alias("revision_id"),
        col("title"),
        col("revision.text._VALUE").alias("content"),
    )

    @udf(returnType=StringType(), useArrow=False)
    def strip_wikitext(text):
        """Cái này là thư viện dùng để làm sạch file xml từ wiki dump."""
        import mwparserfromhell
        if not text:
            return ""
        return mwparserfromhell.parse(text).strip_code(normalize=True, collapse=True).strip()

    articles = articles.withColumn("content", strip_wikitext(col("content")))
    return articles.filter(col("content").isNotNull() & (trim(col("content")) != ""))

def create_chunk_dataframe(articles: DataFrame, chunk_size: int):
    """
    Đọc article dataframe, với mỗi article thì split nó ra thành chunk 200 chữ.
    Dataframe sẽ có dạng:
        article_id
        revision_id
        chunk_index: thứ tự của chunk trong article.
        content
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size phải lớn hơn 0.")
    
    @udf(returnType=ArrayType(StringType()), useArrow=False)
    def split_content(content):
        words = (content or "").split()
        return [
            " ".join(words[start:start + chunk_size])
            for start in range(0, len(words), chunk_size)
        ]
    return articles.select(
        col("article_id"),
        col("revision_id"),
        posexplode(split_content(col("content"))).alias("chunk_index", "content"),
    )

def split_chunk(article: Article, num: int) -> list[Chunk]:
    """
    Chia chunk for article + gán id.
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
        chunks.append(Chunk(chunk_id, article_id, revision_id, content,""))
    return chunks
    
def clean_content(title: str, content: str) -> str:
    """Làm sạch chuỗi nội dung; dùng chung cho DataFrame và Article."""
    pattern = (
        r"^==[ \t]*[^=\n]+[ \t]*==[ \t]*"
        r"(?:\n[ \t]*)*"
        r"(?=^==[ \t]*[^=\n]+[ \t]*==[ \t]*$|\Z)"
    )
    text = re.sub(pattern, "", content, flags=re.MULTILINE).strip()
    text = re.sub(r'\n[ \t]*\n(?:[ \t]*\n)*', '\n', text)
    return "Title: " + title + "\n" + text
            
def embed_chunk(chunk: Chunk, model: SentenceTransformer, batch_size: int) -> np.ndarray:
    """Tạo embedding cho 1 chunk duy nhất."""
    if chunk is None:
        raise ValueError("Chunk không được là None.")
    if not chunk: return np.empty((0, 768), dtype=np.float32)
    # Embedding
    vec = model.encode(chunk.content, batch_size = batch_size, normalize_embeddings=True).astype(np.float32)
    return vec

def index_chunk(vector: np.ndarray, vector_store: str, next_id: int) -> int:
    """Ghi 1 chunk vào chỉ mục vector."""
    import diskannpy

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
    index.insert(vector, vector_id=np.uint32(next_id))
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

def save_parquet(text: str, text_id: int, parquet_folder: str, category: str) -> str:
    """
    Ghi một lô bài đã làm sạch vào Parquet. Lưu theo kiểu HIVE.
    Category: article hoặc chunks
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
    if not text:
        raise ValueError("Text trước khi save parquet bị rỗng.")

    if category not in ("article", "chunk"):
        raise ValueError("Category phải là article hoặc chunk.")

    ingest_date = datetime.now(timezone.utc).date().isoformat()
    root = Path(parquet_folder)
    save_folder = root / f"{category}" / f"date={ingest_date}"
    save_folder.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"text_id": text_id, "text": text}]).to_parquet(
        save_folder / f"{text_id}.parquet",
        engine="pyarrow",
        compression="snappy", # Dùng để giảm dung lượng
        index=False,
    )
    return str(save_folder / f"{text_id}.parquet")

def delete_parquet(parquet_path: str) -> None:
    """Xóa file parquet khi có update."""
    path = Path(parquet_path)
    path.unlink(missing_ok=True)





    
    
