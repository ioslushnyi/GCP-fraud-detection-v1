from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from dotenv import load_dotenv
import time
from datetime import datetime


load_dotenv()

client = InfluxDBClient(
    url=os.getenv("INFLUXDB_URL"),
    token=os.getenv("INFLUXDB_TOKEN"),
    org=os.getenv("INFLUXDB_ORG")
)

write_api = client.write_api(write_options=SYNCHRONOUS)

iso_time = '2025-07-09T18:18:51.211255'
# Replace 'Z' with '+00:00' to make it UTC-aware for Python
if iso_time.endswith("Z"):
    iso_time = iso_time.replace("Z", "+00:00")
# Parse and convert to nanoseconds - required by InfluxDB
# Note: InfluxDB expects timestamps in nanoseconds
parsed_time = datetime.fromisoformat(iso_time)
ns_timestamp = int(parsed_time.timestamp() * 1e9)  # nanoseconds
print(f"⏰ Parsed event time: {parsed_time} (ns: {ns_timestamp})")

point = (
    Point("fraud_events")
    .tag("user_id", "test-user")
    .field("fraud_score", 0.95)
    .field("risk_level", "critical")
    .time(ns_timestamp, WritePrecision.NS)
)

write_api.write(bucket=os.getenv("INFLUXDB_BUCKET"), record=point)
write_api.flush()
print("✅ Local test write done")
