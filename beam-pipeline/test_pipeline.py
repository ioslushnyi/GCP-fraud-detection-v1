import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime, timezone
import json
import pandas as pd
import joblib

# --- Load model and encoders locally ---
model = joblib.load("../ml-model/fraud_model.pkl")
le_currency = joblib.load("../ml-model/le_currency.pkl")
le_country = joblib.load("../ml-model/le_country.pkl")
le_ip_country = joblib.load("../ml-model/le_ip_country.pkl")
le_device = joblib.load("../ml-model/le_device.pkl")
feature_order = joblib.load("../ml-model/feature_order.pkl")

# --- Dummy input events for local testing ---
FAKE_EVENTS = [
    {
        "user_id": "user_2",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": 9999.0,
        "currency": "EUR",
        "country": "PL",
        "ip_country": "DE",
        "device": "Linux"
    }
]

# --- Scoring ---
def score_event(event):
    try:
        def safe_encode(encoder, value):
            return encoder.transform([value])[0] if value in encoder.classes_ else -1

        # Enrich event before scoring
        timestamp = datetime.fromisoformat(event["timestamp"])
        event["hour"] = timestamp.hour
        event["txn_count_last_10min"] = 5

        df = pd.DataFrame([{
            "amount": event["amount"],
            "currency": safe_encode(le_currency, event["currency"]),
            "country": safe_encode(le_country, event["country"]),
            "ip_country": safe_encode(le_ip_country, event["ip_country"]),
            "device": safe_encode(le_device, event["device"]),
            "hour": event["hour"],
            "txn_count_last_10min": event["txn_count_last_10min"]
        }])
        print("\n--- Input Features ---")
        print(df)
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
    options.view_as(StandardOptions).runner = 'DirectRunner'
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
