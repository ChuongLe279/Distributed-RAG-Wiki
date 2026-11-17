"""Kafka producer: đẩy bài Wikipedia mới và bài được sửa vào Kafka.

Hai chế độ:
- ``--replay <file.jsonl>``: đọc file mẫu và đẩy vào Kafka, không cần mạng.
- ``--live``: nghe Wikimedia EventStreams (page-create, revision-create), lấy wikitext qua API.
"""
import argparse
from collections.abc import Callable, Iterable, Iterator
import json
import logging
from pathlib import Path
import queue
import threading
import time
from typing import Any

from common import config
from common.schemas import validate_message

log = logging.getLogger("wiki_producer")

STREAM_PAGE_CREATE = "mediawiki.page-create"
STREAM_REVISION_CREATE = "mediawiki.revision-create"
API_BATCH_SIZE = 50  # số revision tối đa lấy được trong một lần gọi API MediaWiki
QUEUE_SIZE = 1000


# Kafka

def topic_for(event_type: str) -> str:
    """Chọn topic theo loại sự kiện."""
    if event_type == "NEW":
        return config.TOPIC_NEW
    if event_type == "UPDATED":
        return config.TOPIC_UPDATED
    raise ValueError(f"event_type không hợp lệ: {event_type!r}")


def make_kafka_producer(bootstrap: str | None = None) -> Any:
    """Tạo Kafka producer với acks=all và enable.idempotence, để message không bị mất và không bị ghi trùng
    khi producer tự gửi lại.
    """
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": bootstrap or config.KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "message.max.bytes": config.KAFKA_MAX_MESSAGE_BYTES,
        "linger.ms": 20,
    })


class Publisher:
    """Kiểm tra message rồi gửi vào topic tương ứng.

    Key là article_id, nên mọi revision của một bài vào cùng partition và giữ đúng thứ tự.
    """

    def __init__(self, producer: Any) -> None:
        self.producer = producer
        self.sent = 0
        self.failed = 0
        self.skipped = 0

    def _on_delivery(self, error: Any, _message: Any) -> None:
        if error is not None:
            self.failed += 1
            log.error("Gửi Kafka thất bại: %s", error)
        else:
            self.sent += 1

    def publish(self, message: dict[str, Any]) -> bool:
        reason = validate_message(message)
        if reason is not None:
            self.skipped += 1
            log.warning("Bỏ message không hợp lệ (%s)", reason)
            return False
        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
        if len(payload) > config.KAFKA_MAX_MESSAGE_BYTES:
            self.skipped += 1
            log.warning("Bỏ bài %s: %d byte vượt giới hạn message.", message["article_id"], len(payload))
            return False
        while True:
            try:
                self.producer.produce(
                    topic_for(message["event_type"]), key=str(message["article_id"]).encode("ascii"),
                    value=payload, on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self.producer.poll(0.5)  # hàng đợi gửi của producer đang đầy: chờ gửi bớt rồi thử lại
        self.producer.poll(0)
        return True

    def flush(self, timeout: float = 60.0) -> None:
        remaining = self.producer.flush(timeout)
        if remaining:
            self.failed += remaining
            log.error("Còn %d message chưa gửi được sau %.0fs.", remaining, timeout)


# Replay

def read_replay(path: Path) -> Iterator[dict[str, Any]]:
    """Đọc từng message từ file JSONL, giữ nguyên thứ tự dòng."""
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError as error:
                log.warning("Dòng %d không phải JSON: %s", line_number, error)
                continue
            if isinstance(message, dict):
                message["source"] = "replay"
            yield message


def run_replay(path: Path, publisher: Publisher, delay: float = 0.0) -> None:
    """Đẩy toàn bộ message trong file JSONL vào Kafka."""
    for message in read_replay(path):
        publisher.publish(message)
        if delay > 0:
            time.sleep(delay)
    publisher.flush()


# EventStreams

def event_to_candidate(event: Any) -> dict[str, Any] | None:
    """Lọc một sự kiện từ EventStreams (theo schema mediawiki/revision/create).

    Trả về thông tin của revision cần tải nội dung, hoặc None nếu sự kiện bị bỏ qua.
    """
    if not isinstance(event, dict):
        return None
    meta = event.get("meta") or {}
    if meta.get("domain") == "canary":  # sự kiện giả do Wikimedia phát để kiểm tra hệ thống
        return None
    if event.get("database") != "enwiki" or event.get("page_namespace") != 0:
        return None
    if (event.get("performer") or {}).get("user_is_bot"):
        return None
    stream = meta.get("stream")
    has_parent = bool(event.get("rev_parent_id"))
    if stream == STREAM_PAGE_CREATE:
        event_type = "NEW"
    elif stream == STREAM_REVISION_CREATE and has_parent:
        event_type = "UPDATED"
    else:
        # revision-create không có rev_parent_id là revision đầu tiên của một trang mới.
        # Trang đó đã được báo qua page-create, nên bỏ qua để không gửi trùng.
        return None
    page_id, rev_id, title = event.get("page_id"), event.get("rev_id"), event.get("page_title")
    if not isinstance(page_id, int) or not isinstance(rev_id, int) or not isinstance(title, str):
        return None
    return {
        "article_id": page_id,
        "revision_id": rev_id,
        "title": title.replace("_", " "),
        "event_type": event_type,
        "event_time": event.get("rev_timestamp") or meta.get("dt"),
    }


def iter_sse_events(lines: Iterable[str]) -> Iterator[tuple[str | None, dict[str, Any]]]:
    """Đọc luồng Server-Sent Events và tách ra từng sự kiện JSON. Trả về các cặp (event_id, dữ liệu)."""
    event_id: str | None = None
    data: list[str] = []
    for line in lines:
        if line is None:
            continue
        if line == "":
            if data:
                try:
                    yield event_id, json.loads("\n".join(data))
                except json.JSONDecodeError:
                    log.debug("Bỏ sự kiện SSE không phải JSON.")
            data = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value


def parse_revisions_response(payload: Any) -> dict[int, dict[str, str]]:
    """Lấy wikitext từ phản hồi của API (action=query, formatversion=2).

    Trả về {rev_id: {"content", "timestamp"}}.
    """
    found: dict[int, dict[str, str]] = {}
    pages = ((payload or {}).get("query") or {}).get("pages") or []
    for page in pages:
        for revision in page.get("revisions") or []:
            main = (revision.get("slots") or {}).get("main") or {}
            content = main.get("content")
            if isinstance(revision.get("revid"), int) and isinstance(content, str):
                found[revision["revid"]] = {"content": content, "timestamp": revision.get("timestamp")}
    return found


class WikiApi:
    """Tải wikitext theo rev_id. Tự giới hạn tốc độ gọi và thử lại với thời gian chờ tăng dần khi API báo lỗi."""

    def __init__(
        self, session: Any, contact: str, rate: float = 1.0, max_retries: int = 5,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not contact.strip():
            raise ValueError("Cần WIKI_CONTACT (email hoặc URL liên hệ) cho User-Agent.")
        if rate <= 0:
            raise ValueError("rate phải lớn hơn 0.")
        self.session = session
        self.headers = {"User-Agent": f"wiki-rag-streaming/0.1 ({contact.strip()})"}
        self.min_interval = 1.0 / rate
        self.max_retries = max_retries
        self.sleep = sleep
        self._last_call = 0.0

    def _throttle(self) -> None:
        wait = self._last_call + self.min_interval - time.monotonic()
        if wait > 0:
            self.sleep(wait)
        self._last_call = time.monotonic()

    def fetch(self, rev_ids: list[int]) -> dict[int, dict[str, str]]:
        if not rev_ids:
            return {}
        if len(rev_ids) > API_BATCH_SIZE:
            raise ValueError(f"Tối đa {API_BATCH_SIZE} rev_id mỗi request.")
        params = {
            "action": "query", "prop": "revisions", "revids": "|".join(str(rev) for rev in rev_ids),
            "rvslots": "main", "rvprop": "ids|content|timestamp", "formatversion": "2",
            "format": "json", "maxlag": "5",
        }
        for attempt in range(self.max_retries):
            self._throttle()
            backoff = min(60.0, 2.0 ** attempt)
            try:
                response = self.session.get(config.WIKI_API_URL, params=params, headers=self.headers, timeout=30)
            except Exception as error:  # lỗi mạng: thử lại
                log.warning("Lỗi gọi API (%s), thử lại sau %.0fs.", error, backoff)
                self.sleep(backoff)
                continue
            if response.status_code == 429 or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After", "")
                backoff = float(retry_after) if retry_after.isdigit() else backoff
                log.warning("API trả %d, thử lại sau %.0fs.", response.status_code, backoff)
                self.sleep(backoff)
                continue
            response.raise_for_status()
            payload = response.json()
            if (payload.get("error") or {}).get("code") == "maxlag":
                self.sleep(backoff)
                continue
            return parse_revisions_response(payload)
        log.error("Bỏ %d revision sau %d lần thử.", len(rev_ids), self.max_retries)
        return {}


def build_messages(candidates: list[dict[str, Any]], contents: dict[int, dict[str, str]]) -> list[dict[str, Any]]:
    """Ghép thông tin sự kiện với wikitext vừa tải thành message Kafka."""
    messages = []
    for candidate in candidates:
        revision = contents.get(candidate["revision_id"])
        if revision is None:  # không tải được nội dung: revision đã bị xoá hoặc bị ẩn
            continue
        messages.append({
            "article_id": candidate["article_id"],
            "revision_id": candidate["revision_id"],
            "title": candidate["title"],
            "raw_wikitext": revision["content"],
            "event_type": candidate["event_type"],
            "event_time": candidate["event_time"] or revision.get("timestamp"),
            "source": "eventstreams",
        })
    return messages


def _listen(session: Any, headers: dict[str, str], pending: "queue.Queue[dict[str, Any]]",
            stop: threading.Event, stats: dict[str, int]) -> None:
    """Chạy ở luồng nền: đọc EventStreams, đưa các sự kiện đã lọc vào hàng đợi, tự kết nối lại khi mất kết nối."""
    last_id: str | None = None
    while not stop.is_set():
        request_headers = dict(headers, Accept="text/event-stream")
        if last_id:
            request_headers["Last-Event-ID"] = last_id
        try:
            with session.get(config.WIKI_EVENTSTREAMS_URL, headers=request_headers, stream=True, timeout=(10, 90)) as response:
                response.raise_for_status()
                response.encoding = "utf-8"
                for event_id, event in iter_sse_events(response.iter_lines(decode_unicode=True)):
                    if stop.is_set():
                        return
                    last_id = event_id or last_id
                    candidate = event_to_candidate(event)
                    if candidate is None:
                        continue
                    try:
                        pending.put_nowait(candidate)
                    except queue.Full:
                        stats["dropped"] += 1
                        if stats["dropped"] % 100 == 1:
                            log.warning("Hàng đợi đầy, đã bỏ %d sự kiện.", stats["dropped"])
        except Exception as error:
            if not stop.is_set():
                log.warning("Mất kết nối EventStreams (%s), nối lại sau 5s.", error)
                stop.wait(5)


def run_live(publisher: Publisher, max_messages: int | None = None, duration: float | None = None) -> None:
    """Nghe EventStreams và gửi bài mới, bài được sửa vào Kafka."""
    import requests

    session = requests.Session()
    api = WikiApi(session, config.WIKI_CONTACT, rate=config.WIKI_API_RATE)
    pending: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=QUEUE_SIZE)
    stop = threading.Event()
    stats = {"dropped": 0}
    listener = threading.Thread(target=_listen, args=(session, api.headers, pending, stop, stats), daemon=True)
    listener.start()
    deadline = time.monotonic() + duration if duration else None
    published = 0
    try:
        while (max_messages is None or published < max_messages) and (deadline is None or time.monotonic() < deadline):
            batch: list[dict[str, Any]] = []
            try:
                batch.append(pending.get(timeout=1.0))
            except queue.Empty:
                continue
            while len(batch) < API_BATCH_SIZE:
                try:
                    batch.append(pending.get_nowait())
                except queue.Empty:
                    break
            for message in build_messages(batch, api.fetch([item["revision_id"] for item in batch])):
                published += publisher.publish(message)
            log.info("Đã gửi %d message (bỏ %d sự kiện do đầy hàng đợi).", published, stats["dropped"])
    except KeyboardInterrupt:
        log.info("Dừng theo yêu cầu.")
    finally:
        stop.set()
        publisher.flush()


# CLI

def main() -> None:
    parser = argparse.ArgumentParser(description="Đẩy bài Wikipedia mới/sửa vào Kafka.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--replay", type=Path, help="File JSONL mẫu để đẩy vào Kafka (không cần mạng).")
    mode.add_argument("--live", action="store_true", help="Nghe Wikimedia EventStreams.")
    parser.add_argument("--delay", type=float, default=0.0, help="Giây nghỉ giữa các message khi replay.")
    parser.add_argument("--max-messages", type=int, help="Dừng --live sau khi gửi đủ số message.")
    parser.add_argument("--duration", type=float, help="Dừng --live sau số giây này.")
    parser.add_argument("--bootstrap", help="Ghi đè KAFKA_BOOTSTRAP.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.replay is not None and not args.replay.is_file():
        parser.error(f"Không tìm thấy file replay: {args.replay}")
    if args.live and not config.WIKI_CONTACT.strip():
        parser.error("Chế độ --live cần biến môi trường WIKI_CONTACT (email hoặc URL liên hệ).")
    publisher = Publisher(make_kafka_producer(args.bootstrap))
    if args.replay is not None:
        run_replay(args.replay, publisher, delay=args.delay)
    else:
        run_live(publisher, max_messages=args.max_messages, duration=args.duration)
    log.info("Xong: gửi %d, lỗi %d, bỏ %d.", publisher.sent, publisher.failed, publisher.skipped)
    if publisher.failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
