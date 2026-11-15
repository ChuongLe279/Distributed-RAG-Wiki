"""Cấu hình của producer.

Mỗi giá trị đọc từ biến môi trường; nếu không đặt thì dùng giá trị mặc định.
"""
import os


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = _env(name, str(default))
    try:
        return int(raw)
    except ValueError as error:
        raise ValueError(f"{name} phải là số nguyên, nhận được {raw!r}.") from error


# Kafka
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_NEW = _env("TOPIC_NEW", "new_article")
TOPIC_UPDATED = _env("TOPIC_UPDATED", "updated_article")
TOPIC_DLQ = _env("TOPIC_DLQ", "article_dlq")
# Kích thước message tối đa. Wikitext của bài dài có thể vượt mức 1 MB mặc định của Kafka.
KAFKA_MAX_MESSAGE_BYTES = _env_int("KAFKA_MAX_MESSAGE_BYTES", 8 * 1024 * 1024)

# Producer ở chế độ --live (nghe trực tiếp từ Wikipedia)
WIKI_EVENTSTREAMS_URL = _env(
    "WIKI_EVENTSTREAMS_URL",
    "https://stream.wikimedia.org/v2/stream/mediawiki.page-create,mediawiki.revision-create",
)
WIKI_API_URL = _env("WIKI_API_URL", "https://en.wikipedia.org/w/api.php")
# Email hoặc URL liên hệ, gửi kèm trong User-Agent theo yêu cầu của Wikimedia. Bắt buộc ở chế độ --live.
WIKI_CONTACT = _env("WIKI_CONTACT", "")
WIKI_API_RATE = float(_env("WIKI_API_RATE", "1.0"))  # số lần gọi API Wikipedia tối đa mỗi giây

# Sharding (dùng cho các bước sau; test kiểm tra file mẫu có bài thuộc nhiều shard)
SHARD_RANGE_SIZE = _env_int("SHARD_RANGE_SIZE", 10_000_000)


def shard_id(article_id: int) -> int:
    """Tính shard của một bài: article_id // SHARD_RANGE_SIZE. Mỗi shard chứa một khoảng article_id liên tiếp."""
    return int(article_id) // SHARD_RANGE_SIZE
