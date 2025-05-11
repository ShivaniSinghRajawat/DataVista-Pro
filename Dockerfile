
# Dockerfile for Kafka Producer Scripts
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_producers/ kafka_producers/
COPY data/ data/

CMD ["tail", "-f", "/dev/null"]
