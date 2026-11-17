"""In content cua mot vai article de kiem tra truoc khi viet clean_article."""

import argparse
import os
from pathlib import Path
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--xml",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "data/raw/prefilling/simplewiki-latest-pages-articles.xml",
        help="Duong dan Wikipedia XML dump.",
    )
    parser.add_argument("--limit", type=int, default=3, help="So article can in.")
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit phai lon hon 0.")
    if not args.xml.is_file():
        parser.error(f"Khong tim thay XML: {args.xml}")

    # Hien thi Unicode tren terminal Windows.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    # Spark .cmd tren Windows khong quote duong dan Python co khoang trang.
    # Dat thu muc interpreter len dau PATH de ca driver va worker dung dung venv.
    python_executable = sys.executable
    if os.name == "nt":
        os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")
        python_executable = Path(sys.executable).name
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    import pyspark

    # Tranh script .cmd tu tim Spark bang duong dan Python co khoang trang.
    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).resolve().parent))

    from pyspark.sql import SparkSession
    from tasks import create_article_dataframe

    spark = SparkSession.builder.appName("Preview article content").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        article_df = create_article_dataframe(args.xml)
        article_df.printSchema()

        # Chi dua vai article ve driver, khong collect toan bo dump.
        articles = article_df.select(
            "article_id", "revision_id", "title", "content"
        ).take(args.limit)

        if not articles:
            print("Khong co article de hien thi.")

        for article in articles:
            print("\n" + "=" * 80)
            print(f"Title: {article.title}")
            print(f"Article ID: {article.article_id} | Revision ID: {article.revision_id}")
            print("-" * 80)
            # print giu xuong dong va khong cat ngan content nhu show().
            print(article.content)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
