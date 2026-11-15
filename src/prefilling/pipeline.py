from dataclasses import dataclass
from pathlib import Path
import sys
import os
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Cho phép chạy trực tiếp file này và import tasks.py trong thư mục src.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tasks import create_dataframe, clean_content

@dataclass
class PrefillingPipeline:
    pass    

def main():
    # Worker Spark dùng cùng môi trường Python có mwparserfromhell.
    os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = Path(sys.executable).name
    os.environ["PYSPARK_DRIVER_PYTHON"] = Path(sys.executable).name
    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).resolve().parent))
    project_folder = Path(__file__).resolve().parents[2]
    # Chế độ local: worker cần tìm được tasks.py trong src.
    os.environ["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(project_folder / "src"), os.environ.get("PYTHONPATH")])
    )
    xml_file = (
        project_folder / "data" / "raw"
        / "simplewiki-latest-pages-articles.xml"
        / "simplewiki-latest-pages-articles.xml"
    )

    spark = (
        SparkSession.builder
        .appName("DistributedRAG")
        .master("local[2]")
        # Batch nhỏ để xem thử vài bài mà không phải clean hàng trăm bài trước.
        .config("spark.sql.execution.python.udf.maxRecordsPerBatch", 10)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        articles = create_dataframe(xml_file)
        # Dùng logic clean trong tasks.py cho từng dòng của DataFrame.
        clean = udf(clean_content, StringType(), useArrow=False)
        articles = articles.withColumn("content", clean(col("title"), col("content")))
        articles.printSchema()
        # In 5 bài, mỗi cột hiển thị tối đa 100 ký tự cho dễ đọc.
        articles.show(1, truncate=1000)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    pass
