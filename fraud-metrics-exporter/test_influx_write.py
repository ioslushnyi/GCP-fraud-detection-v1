from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from dotenv import load_dotenv
import time

load_dotenv()

client = InfluxDBClient(
    url=os.getenv("INFLUXDB_URL"),
    token=os.getenv("INFLUXDB_TOKEN"),
    org=os.getenv("INFLUXDB_ORG")
)

write_api = client.write_api(write_options=SYNCHRONOUS)
ts = time.time_ns() 
point = (
    Point("fraud_events")
    .tag("user_id", "test-user")
    .field("fraud_score", 0.95)
    .field("risk_level", "critical")
    .time(ts, WritePrecision.NS)
)

write_api.write(bucket=os.getenv("INFLUXDB_BUCKET"), record=point)
write_api.flush()
print("✅ Local test write done")
print(f"time: {ts}")
