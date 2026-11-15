"""Định dạng message Kafka mà producer gửi đi, và hàm kiểm tra message."""
from datetime import datetime
from typing import Any

EVENT_TYPES = ("NEW", "UPDATED")
INT64_MAX = 2**63 - 1

# Các trường bắt buộc của một message Kafka và kiểu dữ liệu của từng trường.
KAFKA_MESSAGE_FIELDS: dict[str, type] = {
    "article_id": int,
    "revision_id": int,
    "title": str,
    "raw_wikitext": str,
    "event_type": str,
    "event_time": str,
    "source": str,
}


def _is_int64(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value <= INT64_MAX


def validate_message(message: Any) -> str | None:
    """Kiểm tra một message Kafka. Trả về None nếu hợp lệ, hoặc một chuỗi mô tả lỗi."""
    if not isinstance(message, dict):
        return "message không phải JSON object"
    for field, kind in KAFKA_MESSAGE_FIELDS.items():
        if field not in message or message[field] is None:
            return f"thiếu trường {field}"
        if kind is int:
            if not _is_int64(message[field]):
                return f"{field} phải là số nguyên dương int64"
        elif not isinstance(message[field], str):
            return f"{field} phải là chuỗi"
    if not message["title"].strip():
        return "title rỗng"
    if message["event_type"] not in EVENT_TYPES:
        return f"event_type không hợp lệ: {message['event_type']!r}"
    try:
        parsed = datetime.fromisoformat(message["event_time"].replace("Z", "+00:00"))
    except ValueError:
        return f"event_time không phải ISO-8601: {message['event_time']!r}"
    if parsed.tzinfo is None:
        return "event_time thiếu múi giờ"
    return None
