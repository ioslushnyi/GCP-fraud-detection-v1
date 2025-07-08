import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import joblib
import json
import pandas as pd
from datetime import datetime
import typing

# --- Load model and encoders globally ---
model = joblib.load("fraud_model_v3.pkl")
le_currency = joblib.load("le_currency_v3.pkl")
le_country = joblib.load("le_country_v3.pkl")
le_ip_country = joblib.load("le_ip_country_v3pkl")
le_device = joblib.load("le_device_v3.pkl")

# --- User activity buffer ---
user_event_buffer = {}

# --- Scoring function ---
def process_event(message: bytes) -> typing.Optional[str]:
    try:
        payload = json.loads(message.decode("utf-8"))
        user_id = payload.get("user_id", "unknown")
        timestamp = datetime.fromisoformat(payload["timestamp"])

        # Update buffer for txn count in last 10 min
        if user_id not in user_event_buffer:
            user_event_buffer[user_id] = []
        user_event_buffer[user_id].append(timestamp)
        user_event_buffer[user_id] = [
            t for t in user_event_buffer[user_id]
            if (timestamp - t).total_seconds() <= 600
        ]
        txn_count_last_10min = len(user_event_buffer[user_id])

        X = pd.DataFrame([{
            "amount": payload["amount"],
            "currency": le_currency.transform([payload["currency"]])[0] if payload["currency"] in le_currency.classes_ else -1,
            "country": le_country.transform([payload["country"]])[0] if payload["country"] in le_country.classes_ else -1,
            "ip_country": le_ip_country.transform([payload["ip_country"]])[0] if payload["ip_country"] in le_ip_country.classes_ else -1,
            "device": le_device.transform([payload["device"]])[0] if payload["device"] in le_device.classes_ else -1,
            "hour": timestamp.hour,
            "txn_count_last_10min": txn_count_last_10min
        }])

        risk_score = model.predict_proba(X)[0][1]
        fraud_label = int(risk_score > 0.5)
        risk_level = (
            "critical" if risk_score > 0.9 else
            "high" if risk_score > 0.7 else
            "medium-high" if risk_score > 0.5 else
            "medium" if risk_score > 0.3 else
            "low" if risk_score > 0.1 else
            "minimal"
        )

        print(f"\nEvent: {payload}\n txn_count_last_10min: {txn_count_last_10min}")
        print(f"\U0001F9E0 Risk Score: {risk_score:.4f}, Risk level: {risk_level} → Fraud: {fraud_label}")

        # Enrich payload for downstream (optional)
        payload.update({
            "fraud_score": risk_score,
            "fraud_label": fraud_label,
            "risk_level": risk_level,
            "txn_count_last_10min": txn_count_last_10min
        })
        return json.dumps(payload)

    except Exception as e:
        print("❌ Error in scoring:", e)
        return None


# --- Pipeline Definition ---
def run():
    options = PipelineOptions(
        streaming=True,
        project="fraud-detection-v1",
        region="europe-central2",
        temp_location="gs://your-bucket/tmp",
        job_name="fraud-streaming-pipeline"
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (p
         | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/fraud-detection-v1/subscriptions/test-sub")
         | "ScoreFraud" >> beam.Map(process_event)
         | "DropNulls" >> beam.Filter(lambda x: x is not None)
         | "WriteToGCS" >> beam.io.WriteToText("gs://your-bucket/fraud_output/scored", file_name_suffix=".json")
         )

if __name__ == "__main__":
    run()
