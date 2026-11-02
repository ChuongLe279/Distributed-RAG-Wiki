"""
Test và chạy thử các hàm trong tasks.py
"""
from models import Article, ArticleTask, Chunk
from tasks import (
    clean_article,
    delete_parquet,
    fetch_newest_article,
    index_chunk,
    save_parquet,
    split_chunk,
)
from sentence_transformers import SentenceTransformer
from pathlib import Path
import torch
torch.backends.mkldnn.enabled = False



VECTOR_STORE_PATH = str(Path(__file__).resolve().parents[1] / "data" / "vectorstore")
PARQUET_PATH = str(Path(__file__).resolve().parents[1] / "data" / "parquet")
# Chạy lần đầu thì phải tải xuống đấy
model = SentenceTransformer("intfloat/e5-base-v2")

if __name__ == "__main__":    
    print("Đang fetch article.")
    article = fetch_newest_article(
        api="https://en.wikipedia.org/w/api.php",
        header={
            "User-Agent": "DistributedRAG/0.1 (local research script)",
            "Accept": "application/json",
        },
    )
    print(f"Tên bài báo: {article.title}")
    print("Đang làm sạch article.")
    article = clean_article(article)
    print("Đang chia chunks cho article.")
    chunks = split_chunk(article, num=200)
    first_chunk_id = 1
    print("Đang index chunk.")
    next_id = index_chunk(article, chunks, VECTOR_STORE_PATH, model, first_chunk_id)
    save_parquet([article], PARQUET_PATH, batch_size=1)
    print("Xong.")

    """
    TODO: KIểm tra bài trùng nhau trước khi nhét vào vector store.
    """


