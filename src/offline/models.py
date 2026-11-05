from typing import Literal
from dataclasses import dataclass
from datetime import date
from tasks import (
    clean_article,
    delete_parquet,
    fetch_newest_article,
    index_chunk,
    save_parquet,
    split_chunk,
    validate_chunk,
)

@dataclass
class Article:
    article_id: int
    revision_id: int
    created_date: date
    updated_date: date
    title: str
    content: str

@dataclass
class Chunk:
    chunk_id: int
    article_id: int
    revision_id: int
    content: str
    metadata: str = "{}" # json

@dataclass(frozen=True)
class ArticleTask:
    article_id: int
    revision_id: int
    source: Literal["backfill", "incremental"]




