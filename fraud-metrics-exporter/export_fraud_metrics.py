from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import base64
import os
import json
from dotenv import load_dotenv
from datetime import datetime
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

load_dotenv()

# InfluxDB configuration
INFLUX_URL = os.getenv("INFLUXDB_URL")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG = os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET")
# Retry settings
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.2  # seconds

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "ok"}

@app.post("/metrics-api/push")
async def pubsub_push_handler(request: Request):
    try:
        body = await request.json()
        pubsub_message = body.get("message", {})

        if not pubsub_message or "data" not in pubsub_message:
            return JSONResponse(content={"error": "Missing message.data"}, status_code=400)

        # Decode base64 data
        data_bytes = base64.b64decode(pubsub_message["data"])
        try:
            event = json.loads(data_bytes)
            logging.info(f"Received event: {event}")
        except json.JSONDecodeError:
            return JSONResponse(content={"error": "Invalid JSON in message data"}, status_code=400)

        iso_time = event.get("event_time") 
        try:
            # Parse and convert to nanoseconds - required by InfluxDB
            # Note: InfluxDB expects timestamps in nanoseconds
            parsed_time = datetime.fromisoformat(iso_time)
            ns_timestamp = int(parsed_time.timestamp() * 1e9)  # nanoseconds
            logging.info(f"Parsed event time: {parsed_time} (ns: {ns_timestamp})")
        except Exception:
            ns_timestamp = time.time_ns() # Fallback to current time in nanoseconds
            logging.warning("Invalid event_time format, using current time instead")

        point = (
            Point("fraud_events")
            .tag("event_id", str(event.get("event_id")))
            .tag("risk_level", str(event.get("risk_level")))
            .field("user_id", str(event.get("user_id")))
            .field("fraud_score", float(event.get("fraud_score")))
            .time(ns_timestamp, WritePrecision.NS)
        )

        for attempt in range(MAX_RETRIES):
            try:
                write_api.write(bucket=INFLUX_BUCKET, record=point)
                logging.info(f"Influx write successful for user_id: {event.get('user_id')}")
                break
            except Exception as e:
                wait = INITIAL_BACKOFF * (2 ** attempt)
                logging.warning(f"Influx write failed (attempt {attempt + 1}): {e} — retrying in {wait:.1f}s")
                time.sleep(wait)
        else:
            logging.error("All Influx retries failed")
            return JSONResponse(content={"error": "InfluxDB write failed after retries"}, status_code=500)

        return JSONResponse(content={"status": "OK"})

    except Exception as e:
        logging.error(f"Unhandled error: {e}")
        return JSONResponse(content={"error": "Internal server error"}, status_code=500)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # Required by Cloud Run
    uvicorn.run("export_fraud_metrics:app", host="0.0.0.0", port=port)
