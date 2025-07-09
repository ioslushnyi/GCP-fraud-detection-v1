from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

INFLUX_URL = os.getenv("INFLUXDB_URL")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG = os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Set measurement name
MEASUREMENT_NAME = "fraud_events_v2"

# Connect
with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Write a single clean record to lock schema
    point = (
        Point(MEASUREMENT_NAME)
        .tag("user_id", "init-schema-test")
        .field("fraud_score", "0.99")             # float ✅
        .field("risk_level", "critical")        # string ✅
        .time(1752085131211200949, WritePrecision.NS)
    )

    write_api.write(bucket=INFLUX_BUCKET, record=point)
    write_api.flush()

print(f"✅ Initialized schema for {MEASUREMENT_NAME} in {INFLUX_BUCKET}")
