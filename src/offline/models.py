from typing import Literal
from dataclasses import dataclass, field
from datetime import date

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

