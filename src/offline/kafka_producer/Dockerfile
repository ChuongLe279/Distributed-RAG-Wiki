FROM python:3.11-slim

WORKDIR /opt/app
ENV PYTHONPATH=/opt/app PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
