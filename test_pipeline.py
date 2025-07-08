import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime
import json
import pandas as pd
import joblib

# --- Load model and encoders locally ---
model = joblib.load("ml-model/fraud_model_v3.pkl")
le_currency = joblib.load("ml-model/le_currency_v3.pkl")
le_country = joblib.load("ml-model/le_country_v3.pkl")
le_ip_country = joblib.load("ml-model/le_ip_country_v3.pkl")
le_device = joblib.load("ml-model/le_device_v3.pkl")
feature_order = joblib.load("ml-model/feature_order_v3.pkl")

# --- Dummy input events for local testing ---
FAKE_EVENTS = [
    {
        "user_id": "user_1",
        "timestamp": datetime.utcnow().isoformat(),
        "amount": 120.5,
        "currency": "USD",
        "country": "US",
        "ip_country": "US",
        "device": "mobile"
    },
    {
        "user_id": "user_1",
        "timestamp": datetime.utcnow().isoformat(),
        "amount": 400.0,
        "currency": "USD",
        "country": "US",
        "ip_country": "US",
        "device": "desktop"
    },
    {
        "user_id": "user_2",
        "timestamp": datetime.utcnow().isoformat(),
        "amount": 50.0,
        "currency": "EUR",
        "country": "DE",
        "ip_country": "DE",
        "device": "tablet"
    }
]

# --- Scoring ---
def score_event(event):
    try:
        timestamp = datetime.fromisoformat(event["timestamp"])
        event["hour"] = timestamp.hour
        event["txn_count_last_10min"] = 1  # For now

        df = pd.DataFrame([{
            "amount": event["amount"],
            "currency": le_currency.transform([event["currency"]])[0] if event["currency"] in le_currency.classes_ else -1,
            "country": le_country.transform([event["country"]])[0] if event["country"] in le_country.classes_ else -1,
            "ip_country": le_ip_country.transform([event["ip_country"]])[0] if event["ip_country"] in le_ip_country.classes_ else -1,
            "device": le_device.transform([event["device"]])[0] if event["device"] in le_device.classes_ else -1,
            "hour": event["hour"],
            "txn_count_last_10min": event["txn_count_last_10min"]
        }])

        risk_score = model.predict_proba(df[feature_order])[0][1]
        fraud_label = int(risk_score > 0.5)
        risk_level = (
            "critical" if risk_score > 0.9 else
            "high" if risk_score > 0.7 else
            "medium-high" if risk_score > 0.5 else
            "medium" if risk_score > 0.3 else
            "low" if risk_score > 0.1 else
            "minimal"
        )

        enriched = {
            "user_id": event["user_id"],
            "timestamp": event["timestamp"],
            "amount": event["amount"],
            "currency": event["currency"],
            "country": event["country"],
            "ip_country": event["ip_country"],
            "device": event["device"],
            "hour": event["hour"],
            "txn_count_last_10min": event["txn_count_last_10min"],
            "fraud_score": risk_score,
            "fraud_label": fraud_label,
            "risk_level": risk_level
        }
        print(f"\n✅ Scored Event: {json.dumps(enriched, indent=2)}")
        return enriched

    except Exception as e:
        print(f"❌ Error scoring event: {e}")
        return None


# --- Beam Test Pipeline ---
def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = False

    with beam.Pipeline(options=options) as p:
        (
            p
            | "CreateFakeEvents" >> beam.Create(FAKE_EVENTS)
            | "ScoreLocally" >> beam.Map(score_event)
            | "DropFailed" >> beam.Filter(lambda x: x is not None)
            | "PrintScored" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
