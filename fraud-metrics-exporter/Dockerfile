
FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "export_metrics_to_influx:app", "--host", "0.0.0.0", "--port", "8080"]
