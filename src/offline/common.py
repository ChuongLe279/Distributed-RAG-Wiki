from dataclasses import dataclass
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from sentence_transformers import SentenceTransformer
import offline.tasks as tasks
from pathlib import Path


@dataclass
class CommonPipeline:
    """
    Quy trình nó như sau:
    1. Làm sạch nội dung của content và chia content thành nhiều chunks có dạng (article_id, revision_id, chunk_index, content)
    2. Các chunks sẽ được embed và tạo thành vectorstore.
    3. Content sẽ được chuyển thành delta lake.
    4. Delta lake và vector store sẽ được phân tán vào 2 máy thông qua các node docker.
    """    
    article: DataFrame
    model: SentenceTransformer
    collection_name: str = "wiki_chunks"
    vector_store: str = ""
    delta_lake_path: str = ""

    def clean_article(self) -> DataFrame:
        """Làm sạch article.
        Note: hiện tại thì t thấy cũng chả cần làm sạch gì.
        """
        return self.article

    def split_article_into_chunks(self, article: DataFrame) -> DataFrame:
        """Chia article thành chunk 200 chữ."""
        return tasks.create_chunk_dataframe(article, chunk_size=200)
    
    def validate_chunk(self, chunk: DataFrame):
        """Kiểm tra định dạng của chunk trước khi embed."""
        tasks.validate_chunk(chunk)

    def embed_chunk(self, chunk: DataFrame) -> DataFrame:
        """Embed chunk."""
        return tasks.embed_chunks(
            chunk,
            model=self.model,
            batch_size=32,
        )

    def index_chunk(self, chunk: DataFrame) -> int:
        """Nhét chunk vào vector store."""
        return tasks.index_chunk(
            chunk,
            vector_store=self.vector_store,
            collection_name=self.collection_name,
            batch_size=1000,
        )

    def create_delta_lake_table(self, article: DataFrame, chunk: DataFrame) -> None:
        """Tạo bảng delta table cho cả article và chunk."""
        base_path = Path(self.delta_lake_path)
        tasks.create_delta_lake_table(article, str(base_path / "articles"))
        print(f"Đã tạo bảng delta lake cho article")
        tasks.create_delta_lake_table(chunk, str(base_path / "chunks"))
        print(f"Đã tạo bảng delta lake cho chunk")

    def run(self):
        persisted_frames = []
        try:
            # Reuse cleaned articles for validation, embedding and the articles table.
            clean_article = self.clean_article().persist(StorageLevel.MEMORY_AND_DISK)
            persisted_frames.append(clean_article)

            chunk_df = self.split_article_into_chunks(clean_article).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            persisted_frames.append(chunk_df)
            self.validate_chunk(chunk_df)

            # Keep embeddings for both Milvus actions and the chunks table write.
            embedded_chunk_df = self.embed_chunk(chunk_df).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            persisted_frames.append(embedded_chunk_df)
            total_index = self.index_chunk(embedded_chunk_df)
            print(f"Total index = {total_index}")
            self.create_delta_lake_table(clean_article, embedded_chunk_df)
        finally:
            for dataframe in reversed(persisted_frames):
                dataframe.unpersist()

if __name__ == "__main__":
    pass
