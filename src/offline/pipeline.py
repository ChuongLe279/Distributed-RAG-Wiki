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
    embed_chunk,
    index_chunk,
    save_parquet,
    validate_chunk,
    split_chunk,
)
from pyspark.sql import SparkSession
from pyspark import RDD
import torch
from datasets import load_dataset
import time
from dataclasses import dataclass
from sentence_transformers import SentenceTransformer
from dataclasses import replace
torch.backends.mkldnn.enabled = False


@dataclass
class SparkArticlePipeline:
    spark: SparkSession
    model: SentenceTransformer
    partition_num: int
    embedding_batch_num: int
    chunk_size: int
    vector_store: str
    parquet_folder: str
    articles_batch_len: int
    api: str
    header: dict

    def preprocessing(self, articles: list[Article]) -> RDD:
        """Clean và chia chunk song song."""
        sc = self.spark.sparkContext
        article_rdd = sc.parallelize(articles, numSlices=self.partition_num)
        chunk_size = self.chunk_size
        return (
            article_rdd
            .map(clean_article)
            .flatMap(lambda article: split_chunk(article, num=chunk_size))
        )

    def validate(self, chunks: RDD, articles: RDD) -> None:
        """Kiểm tra định dạng id của chunk trước khi index."""
        pairs = (
            chunks.map(lambda c: (c.article_id, c))
            .leftOuterJoin(articles.map(lambda a: (a.article_id, a)))
        )
        def validate_pair(item):
            article_id, (c, a) = item
            if a is None:
                raise ValueError(f"Không tìm thấy bài viết {article_id}.")
            validate_chunk(a, c)
        pairs.foreach(validate_pair)

    def embed(self, chunks: RDD) -> RDD:
        model = self.model
        batch_size = self.embedding_batch_num
        return chunks.map(
            lambda c: embed_chunk(c, model=model, batch_size=batch_size)
        )

    def index(self, vectors: RDD) -> RDD:
        vector_store = self.vector_store
        return vectors.zipWithIndex().map(
            lambda item: index_chunk(item[0], vector_store, next_id=item[1] + 1)
        )

    def save(self, articles: RDD, chunks: RDD) -> tuple[RDD, RDD]:
        folder = self.parquet_folder
        saved_articles = articles.map(
            lambda a: replace(
                a,
                parquet_path=save_parquet(
                    a.content, a.article_id, folder, "article"
                ),
            )
        )
        saved_chunks = chunks.zipWithIndex().map(
            lambda item: replace(
                item[0],
                parquet_path=save_parquet(
                    item[0].content, item[1] + 1, folder, "chunk"
                ),
            )
        )

        return saved_articles, saved_chunks

    def run(self) -> None:
        articles = []
        sc = self.spark.sparkContext       
        seen_ids = set()
        while (len(articles) < self.articles_batch_len):
            article = fetch_newest_article(self.api, self.header)
            if article.article_id not in seen_ids:
                articles.append(article)
                seen_ids.add(article.article_id)

            if len(articles) < self.articles_batch_len:
                time.sleep(20)  # TODO: viết hàm bool kiểm tra xem wiki có đăng bài chưa
                
        articles_rdd = sc.parallelize(articles, numSlices=self.partition_num)
        chunks = self.preprocessing(articles).cache()  # Trả về RDD
        self.validate(chunks, articles_rdd)
        vectors = self.embed(chunks)
        indexed = self.index(vectors)
        saved_articles, saved_chunks = self.save(articles_rdd, chunks)
        # RDD là lazy, phải gọi action thì mới thực sự chạy
        indexed.count()
        saved_articles.count()
        saved_chunks.count()
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
