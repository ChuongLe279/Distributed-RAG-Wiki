"""Integration tests: python -m pytest src/test.py -v

Requires Java, PySpark and a compatible delta-spark installation.
The first run may download Delta Lake JARs from Maven.
"""

import os
import sys

import pytest
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from tasks import create_delta_lake_table


@pytest.fixture(scope="module")
def spark():
    # Use the same Python interpreter for local Spark workers.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("test-delta-lake-table")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    try:
        yield session
    finally:
        session.stop()


SCHEMA = "article_id LONG, revision_id LONG, chunk_index INT, content STRING"


def test_create_delta_lake_table_round_trip(spark, tmp_path):
    rows = [(1, 101, 0, "Nội dung bài viết"), (2, 201, 0, "Delta Lake")]
    dataframe = spark.createDataFrame(rows, SCHEMA)
    table_path = str(tmp_path / "chunks")

    create_delta_lake_table(dataframe, table_path)

    assert DeltaTable.isDeltaTable(spark, table_path)
    actual = spark.read.format("delta").load(table_path)
    assert actual.schema == dataframe.schema
    assert sorted(tuple(row) for row in actual.collect()) == sorted(rows)


def test_create_delta_lake_table_overwrites_existing_data(spark, tmp_path):
    table_path = str(tmp_path / "chunks")
    original = spark.createDataFrame([(1, 101, 0, "Old content")], SCHEMA)
    replacement_rows = [(2, 202, 0, "Updated content")]
    replacement = spark.createDataFrame(replacement_rows, SCHEMA)

    create_delta_lake_table(original, table_path)
    create_delta_lake_table(replacement, table_path)

    actual = spark.read.format("delta").load(table_path)
    assert [tuple(row) for row in actual.collect()] == replacement_rows


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
