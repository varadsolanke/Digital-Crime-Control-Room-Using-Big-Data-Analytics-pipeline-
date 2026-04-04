FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app/backend
COPY analytics /app/analytics
COPY hdfs /app/hdfs

ENV PYTHONPATH=/app/backend:/app/analytics

EXPOSE 5000

CMD ["python", "/app/backend/app.py"]
