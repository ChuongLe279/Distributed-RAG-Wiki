from __future__ import annotations
from typing import Iterator
from pymilvus import MilvusClient, DataType
from sentence_transformers import SentenceTransformer
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, trim, posexplode, length
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    ArrayType,
    FloatType
)
from pyspark.sql import DataFrame

def create_article_dataframe(xml_data) -> DataFrame:
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
        StructField("redirect", StructType([
            StructField("_title", StringType(), nullable=True),
        ]), nullable=True),
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
    articles = pages.filter((col("ns") == 0) & col("redirect").isNull()).select(
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

def create_chunk_dataframe(articles: DataFrame, chunk_size: int) -> DataFrame:
    """
    Đọc article dataframe, với mỗi article thì split nó ra thành chunk 200 chữ.
    Dataframe sẽ có dạng:
        article_id
        revision_id
        chunk_index: thứ tự của chunk trong article.
        content
    Đầu mỗi chunk sẽ được + title.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size phải lớn hơn 0.")
    
    @udf(returnType=ArrayType(StringType()), useArrow=False)
    def split_content(content, title):
        words = (content or "").split()
        title = (title or "").strip()

        chunks = []
        for start in range(0, len(words), chunk_size):
            text = " ".join(words[start:start + chunk_size])
            chunks.append(f"{title}\n\n{text}" if title else text)
        return chunks
    
    return articles.select(
        col("article_id"),
        col("revision_id"),
        posexplode(split_content(col("content"), col("title"))).alias("chunk_index", "content"),
    )

            
def embed_chunks(chunk: DataFrame, model: SentenceTransformer, batch_size: int = 32) -> DataFrame:
    """
    Gán thêm cột embedding cho từng chunk.
    Chunk dataframe sẽ có dạng này:
        article_id
        revision_id
        chunk_index: thứ tự của chunk trong article.
        content
        embedding
    """
    # Thêm cột embedding
    output_schema = StructType(
        chunk.schema.fields + [StructField("embedding", ArrayType(FloatType()), True)]
    )
    def embedding(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        # Embedding
        for pdf in iterator:
            # Bỏ qua nếu batch rỗng
            if pdf.empty:
                continue

            # Chuyển cột content thành list để encode theo batch
            texts = pdf["content"].fillna("").tolist()

            # Encode toàn bộ batch trong pandas DataFrame
            embeddings = model.encode(
                texts,
                batch_size=batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            )

            # Gán kết quả vào cột mới (chuyển về list of floats cho Spark)
            pdf["embedding"] = embeddings.astype(np.float32).tolist()
            yield pdf
    
    return chunk.mapInPandas(embedding, schema=output_schema)

def index_chunk(chunk: DataFrame, vector_store: str, collection_name = "wiki_chunks", batch_size = 1000) -> int:
    """
    Nhét toàn bộ cột embedding trong chunk dataframe vào vectorstore.
    """
    if not vector_store:
        raise ValueError("vector_store must be a Milvus URI or database path.")
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    # Initialize on the driver before Spark starts inserting partitions.
    sample = chunk.select("embedding").take(1)
    if not sample:
        return 0
    vector = sample[0]["embedding"]
    if vector is None or len(vector) == 0:
        raise ValueError("embedding must be a non-empty vector.")
    dimension = len(vector)

    client = MilvusClient(uri=vector_store)
    try:
        if not client.has_collection(collection_name=collection_name):
            schema = MilvusClient.create_schema(auto_id=True, enable_dynamic_field=False)
            schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
            schema.add_field(field_name="article_id", datatype=DataType.INT64)
            schema.add_field(field_name="revision_id", datatype=DataType.INT64)
            schema.add_field(field_name="chunk_index", datatype=DataType.INT64)
            schema.add_field(field_name="content", datatype=DataType.VARCHAR, max_length=65535)
            schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=dimension)
            index_params = client.prepare_index_params()
            index_params.add_index(field_name="embedding", index_type="AUTOINDEX", metric_type="COSINE")
            client.create_collection(
                collection_name=collection_name, schema=schema, index_params=index_params,
            )
        else:
            fields = client.describe_collection(collection_name=collection_name)["fields"] # type: ignore
            embedding_field = next((field for field in fields if field["name"] == "embedding"), None)
            if embedding_field is None or int(embedding_field.get("params", {}).get("dim", 0)) != dimension:
                raise ValueError("Existing collection embedding dimension does not match the chunks.")
    finally:
        client.close()

    status_schema = StructType([StructField("inserted_count", LongType(), False)])

    def insert_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        # hởi tạo kết nối Milvus 1 lần duy nhất cho mỗi partition
        client = MilvusClient(uri=vector_store)

        for pdf in iterator:
            if pdf.empty:
                continue

            # Chuyển pandas DataFrame thành list of dicts: [{"article_id": ..., "embedding": ...}, ...]
            # Lưu ý: Tên các cột trong DataFrame phải khớp với schema của Collection trong Milvus
            records = pdf.to_dict(orient="records")

            # Chia nhỏ tiếp (sub-batch) để tránh vượt quá giới hạn gRPC message size (thường là 64MB) của Milvus
            inserted_in_pdf = 0
            for i in range(0, len(records), batch_size):
                sub_batch = records[i : i + batch_size]
                res = client.insert(
                    collection_name=collection_name,
                    data=sub_batch,
                )
                inserted_in_pdf += res.get("insert_count", len(sub_batch))

            # 4. Yield số lượng đã insert của batch này để giải phóng RAM và tiếp tục batch kế
            yield pd.DataFrame({"inserted_count": [inserted_in_pdf]})

        client.close()

    result_df = chunk.mapInPandas(insert_partition, schema=status_schema)
    total_inserted = result_df.agg({"inserted_count": "sum"}).collect()[0][0]

    return total_inserted or 0


def validate_chunk(chunk: DataFrame):
    """Kiểm tra định dạng id của chunk trước khi index."""
    if chunk is None:
        raise ValueError("Chunk DataFrame không được là None.")

    required_columns = {"article_id", "revision_id", "chunk_index", "content",}
    missing_columns = required_columns - set(chunk.columns)

    if missing_columns:
        raise ValueError(f"Thiếu các cột: {sorted(missing_columns)}")

    invalid_rows = chunk.filter(
        col("article_id").isNull()
        | col("revision_id").isNull()
        | col("chunk_index").isNull()
        | (col("chunk_index") < 0)
        | col("content").isNull()
        | (length(trim(col("content"))) == 0)
    )

    if invalid_rows.limit(1).count() > 0:
        raise ValueError("Chunk có ID null, chunk_index âm hoặc nội dung rỗng.")

    duplicate_keys = (
        chunk.groupBy("article_id", "revision_id", "chunk_index")
        .count()
        .filter(col("count") > 1)
    )

    if duplicate_keys.limit(1).count() > 0:
        raise ValueError("Trùng khóa (article_id, revision_id, chunk_index).")

def create_delta_lake_table(dataframe: DataFrame, save_path: str) -> None:
    dataframe.write.format("delta").mode("overwrite").save(save_path)
