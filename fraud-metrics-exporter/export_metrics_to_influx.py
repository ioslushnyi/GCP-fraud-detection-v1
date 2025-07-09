from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
from influxdb_client import InfluxDBClient, Point, WritePrecision
import base64
import os
import json
from dotenv import load_dotenv
from datetime import datetime
import time

import logging

logging.basicConfig(level=logging.INFO)

load_dotenv()

app = FastAPI()

INFLUX_URL = os.getenv("INFLUXDB_URL")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG = os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET")

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()

# Retry settings
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.2  # seconds

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
            logging.info(f"📩 Received event: {event}")
        except json.JSONDecodeError:
            return JSONResponse(content={"error": "Invalid JSON in message data"}, status_code=400)

        #iso_time = event.get("event_time") 
        logging.info(f"⏰ event.get('event_time'): {event.get("event_time")}")
        iso_time = '2025-07-09T18:18:51.211239'
        try:
            # Replace 'Z' with '+00:00' to make it UTC-aware for Python
            if iso_time.endswith("Z"):
                iso_time = iso_time.replace("Z", "+00:00")
            # Parse and convert to nanoseconds - required by InfluxDB
            # Note: InfluxDB expects timestamps in nanoseconds
            parsed_time = datetime.fromisoformat(iso_time)
            ns_timestamp = int(parsed_time.timestamp() * 1e9)  # nanoseconds
            logging.info(f"⏰ Parsed event time: {parsed_time} (ns: {ns_timestamp})")
        except Exception:
            ns_timestamp = time.time_ns() # Fallback to current time in nanoseconds
            logging.warning("⚠️ Invalid event_time format, using current time instead")

        point = (
            Point("fraud_events")
            .tag("user_id", str(event.get("user_id", "unknown")))
            .field("risk_level", str(event.get("risk_level", 0)))
            .field("fraud_score", float(event.get("fraud_score", 0)))
            .time(ns_timestamp, WritePrecision.NS)
        )

        # Retry on failure with exponential backoff
        for attempt in range(MAX_RETRIES):
            try:
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                logging.info(f"✅ Influx write successful for user_id: {event.get('user_id')}")
                break  # success, exit retry loop
            except Exception as e:
                wait = INITIAL_BACKOFF * (2 ** attempt)
                logging.warning(f"⚠️ Influx write failed (attempt {attempt + 1}): {e} — retrying in {wait:.1f}s")
                time.sleep(wait)
        else:
            logging.error("❌ All Influx retries failed")
            return JSONResponse(content={"error": "InfluxDB write failed after retries"}, status_code=500)

        return JSONResponse(content={"status": "OK"})

    except Exception as e:
        logging.error(f"❌ Unhandled error: {e}")
        return JSONResponse(content={"error": "Internal server error"}, status_code=500)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # Required by Cloud Run
    uvicorn.run("export_metrics_to_influx:app", host="0.0.0.0", port=port)