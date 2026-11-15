from dataclasses import dataclass
from pathlib import Path
import sys
import os
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType




@dataclass
class PrefillingPipeline:
    """
    Quy trình nó như sau:
    1. Đọc file xml dump và chuyển nó thành dataframe có dạng (article_id, revision_id, title, content)
    2. Làm sạch nội dung của content và chia content thành nhiều chunks.
    3. Các chunks sẽ được embed và tạo thành vectorstore.
    4. Content sẽ được chuyển thành delta lake.
    5. Delta lake và vector store sẽ được phân tán vào 2 máy thông qua các node docker.
    """    
    def load_xml_and_create_dataframe(self):
        raise NotImplementedError
    
    def preprocessing(self):
        """Làm sạch dataframe content và split."""
        raise NotImplementedError

    def validate(self):
        raise NotImplementedError

    def embed(self):
        raise NotImplementedError

    def index(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

if __name__ == "__main__":
    pass
