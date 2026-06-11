FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY kafka_to_redis.py .
CMD ["python", "-u", "kafka_to_redis.py"]
