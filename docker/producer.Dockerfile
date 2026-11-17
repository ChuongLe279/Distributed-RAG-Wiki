FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

# Producer chỉ cần 3 thư viện. Lấy đúng version của chúng từ requirement.txt chung, để không phải cài
# torch, pyspark... và để version chỉ khai báo ở một chỗ.
COPY requirement.txt /tmp/requirement.txt
RUN grep -E '^(confluent-kafka|requests|pytest)==' /tmp/requirement.txt > /tmp/producer-requirements.txt \
    && pip install --no-cache-dir -r /tmp/producer-requirements.txt

# Code không copy vào image mà được mount từ repo lúc chạy (xem docker-compose.yml).
WORKDIR /opt/app/src/offline/kafka_producer
