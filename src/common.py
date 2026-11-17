from dataclasses import dataclass
from pathlib import Path
import sys
import os
import numpy as np
import pyspark
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, udf
from pyspark.sql.types import StringType

from tasks import *


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

    def clean_article(self) -> DataFrame:
        """Làm sạch article."""
        raise NotImplementedError

    def split_article_into_chunks(self, article: DataFrame) -> DataFrame:
        """Chia article thành chunk 200 chữ."""
        raise NotImplementedError
    
    def validate_chunk(self, chunk: DataFrame):
        """Kiểm tra định dạng của chunk trước khi embed."""
        raise NotImplementedError

    def embed_chunk(self, chunk: DataFrame) -> DataFrame:
        """Embed chunk."""
        raise NotImplementedError

    def index_chunk(self, chunk: DataFrame) -> int:
        """Nhét chunk vào vector store."""
        raise NotImplementedError

    def create_delta_table(self):
        """Tạo bảng delta table để phân tán."""
        raise NotImplementedError

    def run(self):
        clean_article = self.clean_article()
        chunk_df = self.split_article_into_chunks(clean_article)
        self.validate_chunk(chunk_df)
        self.embed_chunk(chunk_df)
        total_index = self.index_chunk(chunk_df)
        print(f"Total index = {total_index}")
        self.create_delta_table()

if __name__ == "__main__":
    pass
