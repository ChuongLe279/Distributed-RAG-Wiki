"""
Chúng ta có một số pipeline quan trọng sau đây:
1. SparkArticlePipeline: Làm sạch, chia chunk, embedding và lưu dữ liệu song song bằng Spark.
2. BackFillPipeline: Nạp các bài viết cũ theo batch vào SparkArticlePipeline.
3. ArticleUpdatePipeline: Nhận bài viết cập nhật, xử lý phiên bản mới và thay thế dữ liệu cũ.
4. NewArticlePipeline: Nhận bài viết mới và đưa vào SparkArticlePipeline.
"""
from models import Article, ArticleTask, Chunk
from tasks import (
    clean_article,
    delete_parquet,
    fetch_newest_article,
    embed_chunks,
    index_chunks,
    save_parquet,
    split_chunk,
)
from pyspark.sql import SparkSession
from pyspark import RDD
import torch
from datasets import load_dataset
from dataclasses import dataclass
torch.backends.mkldnn.enabled = False


@dataclass
class SparkArticlePipeline:
    spark: SparkSession
    model_name: str
    partition_num: int
    embedding_batch_num: int
    chunk_size: int

    def preprocessing(self, article: list[Article]) -> RDD:
        """Clean và chia chunk song song."""
        sc = self.spark.sparkContext
        article_rdd = sc.parallelize(article, numSlices=self.partition_num)
        return (
            article_rdd
            .map(clean_article)
            .flatMap(lambda article: split_chunk(article, num=self.chunk_size))
        )

    def embed(self, chunk: RDD, article: RDD) -> RDD:
        return chunk.map(embed_chunks)

    def index(self, chunk: RDD, article: RDD) -> RDD:
        raise NotImplementedError

    def save(self) -> None:
        raise NotImplementedError

    def run(self, article: RDD) -> None:
        raise NotImplementedError

    """
    TODO: KIểm tra bài trùng nhau trước khi nhét vào vector store.
    """

@dataclass
class BackFillPipeline:
    spark: SparkSession

    def run(self) -> None:
        # Tải bản snapshot tiếng Anh (ví dụ mốc tháng 11/2025)
        ds = load_dataset("wikimedia/wikipedia", "20231101.en", split="train")
        # Ghi ra Parquet để giữ chặt schema và tối ưu I/O
        ds.to_parquet("en_wikipedia_20251101.parquet") # type: ignore

"""Đang nghĩ đến chuyện dùng kafka."""
@dataclass
class ArticleUpdatePipeline:
    spark: SparkSession

    def run(self) -> None:
        raise NotImplementedError

@dataclass  
class NewArticlePipeline:
    spark: SparkSession

    def run(self) -> None:
        raise NotImplementedError
