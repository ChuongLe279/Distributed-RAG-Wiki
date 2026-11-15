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

    def clean_article(self, article_dataframe: DataFrame) -> DataFrame:
        """Làm sạch dataframe content."""
        clean = udf(clean_content, StringType(), useArrow=False)
        return article_dataframe.withColumn(
            "content",
            clean(
                coalesce(col("title"), lit("")),
                coalesce(col("content"), lit("")),
            ),
        )

    def split_article_into_chunks(self, article_dataframe: DataFrame) -> DataFrame:
        """Chia article thành chunk 200 chữ."""
        raise NotImplementedError
    
    def validate_article(self) -> bool:
        raise NotImplementedError

    def embed_chunk(self) -> np.ndarray:
        raise NotImplementedError

    def index_chunk(self):
        raise NotImplementedError

    def create_delta_table(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

if __name__ == "__main__":
    pass
