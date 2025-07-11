from datetime import datetime, timezone
import time

int(datetime.fromisoformat('2025-07-10T09:50:50.881230').timestamp())
event = {}
event["timestamp"] = datetime.now(timezone.utc).isoformat()

print(f"⚠️ Invalid event_time format ({event["timestamp"]}), using current time instead")
iso_time = event.get("timestamp") 
parsed_time = datetime.fromisoformat(iso_time)
print(f"⏰ parsed_time: {parsed_time}")

ns_timestamp1 = int(parsed_time.timestamp() * 1e9)  # nanoseconds
ns_timestamp2 = time.time_ns()  # nanoseconds
print(f"⏰ ns_timestamp1: {ns_timestamp1} type: {type(ns_timestamp1)}")
print(f"⏰ ns_timestamp2: {ns_timestamp2} type: {type(ns_timestamp2)})")