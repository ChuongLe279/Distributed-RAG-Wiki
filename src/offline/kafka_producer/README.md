# Kafka producer

Producer đẩy bài Wikipedia vào Kafka:

```
Wikipedia → producer → Kafka (new_article, updated_article)
                         └→ (chưa làm) workers → Bronze → Silver → Milvus + Delta
```

Đầu ra của producer là các message nằm trong hai topic `new_article` và `updated_article`, chờ consumer ở bước sau đọc.

Gói chạy độc lập, gồm Kafka và một container Python 3.11.

## Cấu trúc

| File | Vai trò |
|---|---|
| `producer/wiki_kafka_producer.py` | Toàn bộ producer: chế độ `--replay` và `--live`. |
| `common/schemas.py` | Định dạng message (`KAFKA_MESSAGE_FIELDS`) và hàm `validate_message`. |
| `common/config.py` | Cấu hình đọc từ biến môi trường: Kafka, `WIKI_*`, `SHARD_RANGE_SIZE`. |
| `tests/test_producer.py` | Unit test, không cần Kafka. |
| `tests/test_replay_kafka.py` | Test tích hợp, cần Kafka đang chạy. |
| `tests/fixtures/replay_sample.jsonl` | 22 message mẫu của 20 bài (nội dung tự viết). Có sẵn các trường hợp để test bước sau: trang redirect, trang định hướng, bài có bảng và infobox, một bài được sửa 2 lần. Tạo lại bằng `python tests/fixtures/make_replay_sample.py`. |
| `docker-compose.yml`, `Dockerfile` | Kafka 3.7.2, service tạo topic, container producer. |

## Định dạng message

Mỗi message là một JSON object, gửi vào topic `new_article` (khi `event_type` là `NEW`) hoặc `updated_article` (khi là `UPDATED`). Key của message là `article_id`, nên mọi revision của một bài vào cùng một partition và giữ đúng thứ tự.

| Trường | Kiểu | Ý nghĩa |
|---|---|---|
| `article_id` | int | page_id trên Wikipedia |
| `revision_id` | int | rev_id của revision này |
| `title` | string | tiêu đề bài, không rỗng |
| `raw_wikitext` | string | nội dung wikitext gốc |
| `event_type` | string | `NEW` hoặc `UPDATED` |
| `event_time` | string | thời điểm ISO-8601 có múi giờ, ví dụ `2026-01-05T08:00:00Z` |
| `source` | string | `eventstreams` (live) hoặc `replay` |

'raw_wikitext' là 'content' trong pipeline.

**Các bước sau sẽ đọc message theo đúng định dạng này.** Khi viết consumer, dùng lại `validate_message` trong `common/schemas.py` để kiểm tra message, thay vì viết lại điều kiện. Muốn thêm, bớt hay đổi tên trường thì sửa ở `common/schemas.py` để producer và consumer dùng chung một định nghĩa.

Topic `article_dlq` đã được tạo nhưng chưa có gì ghi vào. Topic này dành cho worker sau này, để chuyển các message lỗi sang đó.

Message lớn hơn 8 MB (`KAFKA_MAX_MESSAGE_BYTES`) bị bỏ qua và ghi log.

## Chạy

1. Khởi động Kafka và container producer. Lần đầu sẽ build image, mất khoảng 1 phút:

   ```bash
   docker compose up -d --build
   ```

2. Kiểm tra: `kafka` phải `healthy`, `kafka-init` phải `Exited (0)`:

   ```bash
   docker compose ps -a
   ```

### Đọc lại message trong Kafka

Chưa có consumer nào, để xem producer đã gửi gì:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic new_article --from-beginning --property print.key=true --max-messages 3
```

Đổi `new_article` thành `updated_article` để xem bài được sửa. Từ máy host, Kafka mở ở `localhost:29092`.

### Chế độ replay

Đẩy file mẫu vào Kafka (mode này không cần mạng):

```bash
docker compose exec producer python -m producer.wiki_kafka_producer --replay tests/fixtures/replay_sample.jsonl
```

Thêm `--delay 0.5` để nghỉ 0,5 giây giữa các message để mô phỏng luồng sự kiện.

### Chế độ live

"Nghe" Wikimedia EventStreams (`mediawiki.page-create`, `mediawiki.revision-create`), chỉ giữ bài trên Wikipedia English, thuộc namespace 0 (bài viết chính) và không phải do bot sửa, rồi lấy wikitext qua API Wikipedia.

Wikimedia yêu cầu `User-Agent` có thông tin liên hệ, nên phải đặt `WIKI_CONTACT` là email hoặc URL của user:

```bash
docker compose exec -e WIKI_CONTACT=you@example.org producer python -m producer.wiki_kafka_producer --live --max-messages 20
```

Tuỳ chọn:
- `--max-messages N`: dừng sau khi gửi đủ N message.
- `--duration S`: dừng sau S giây.
- `--bootstrap HOST:PORT`: dùng Kafka khác thay cho `KAFKA_BOOTSTRAP`.
- Biến môi trường `WIKI_API_RATE`: số lần gọi API tối đa mỗi giây, mặc định là 1.

## Test

Unit test:

```bash
docker compose exec producer pytest -m "not integration" -q
```

Test tích hợp đẩy file mẫu vào Kafka rồi đọc lại. Test dùng topic tạm và xoá chúng sau khi chạy:

```bash
docker compose exec producer pytest -m integration -q
```

## Việc cần làm

- Chưa có consumer. Kafka mặc định chỉ giữ message 7 ngày (`log.retention.hours=168`), nên message gửi trước khi có consumer quá 7 ngày sẽ bị xoá. Muốn giữ lâu hơn thì tăng `retention.ms` của topic.
- Chế độ `--live` mới được test bằng dữ liệu giả lập (SSE và API), chưa chạy với EventStreams.
- Khi mất kết nối, producer nối lại EventStreams bằng `Last-Event-ID`, nhưng vị trí này chỉ nằm trong bộ nhớ. Nếu khởi động lại tiến trình, producer nghe tiếp từ thời điểm hiện tại và bỏ lỡ các sự kiện trong lúc dừng.
- Nếu hàng đợi nội bộ đầy (1000 sự kiện), sự kiện mới bị bỏ và ghi log.
- Revision bị xoá hoặc bị ẩn trước khi kịp tải nội dung thì bị bỏ qua.

## Dừng

```bash
docker compose down
```

Thêm `-v` để xoá dữ liệu Kafka.
