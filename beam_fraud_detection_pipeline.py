import json
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.userstate import BagStateSpec
import joblib
import pandas as pd

# --- Load model and encoders ---
model = joblib.load("fraud_model_v2.pkl")
le_currency = joblib.load("le_currency.pkl")
le_country = joblib.load("le_country.pkl")
le_ip_country = joblib.load("le_ip_country.pkl")
le_device = joblib.load("le_device.pkl")

class ScoreWithUserActivity(beam.DoFn):
    ACTIVITY_WINDOW = 600  # 10 minutes
    STATE_SPEC = BagStateSpec("user_timestamps", beam.coders.VarIntCoder())

    def process(self, element, state=beam.DoFn.StateParam(STATE_SPEC)):
        try:
            payload = json.loads(element)
            user_id = payload.get("user_id", "unknown")
            timestamp = datetime.fromisoformat(payload["timestamp"])
            event_ts = int(timestamp.timestamp())

            # Update state
            state.add(event_ts)
            all_timestamps = list(state.read())
            filtered = [t for t in all_timestamps if event_ts - t <= self.ACTIVITY_WINDOW]
            state.clear()
            for t in filtered:
                state.add(t)
            txn_count = len(filtered)

            # Prepare features
            X = pd.DataFrame([{
                "amount": payload["amount"],
                "currency": le_currency.transform([payload["currency"]])[0] if payload["currency"] in le_currency.classes_ else -1,
                "country": le_country.transform([payload["country"]])[0] if payload["country"] in le_country.classes_ else -1,
                "ip_country": le_ip_country.transform([payload["ip_country"]])[0] if payload["ip_country"] in le_ip_country.classes_ else -1,
                "device": le_device.transform([payload["device"]])[0] if payload["device"] in le_device.classes_ else -1,
                "hour": timestamp.hour,
                "txn_count_last_10min": txn_count
            }])

            # Predict
            risk_score = model.predict_proba(X)[0][1]
            fraud_label = int(risk_score > 0.5)
            risk_level = (
                "critical" if risk_score > 0.95 else
                "high" if risk_score >= 0.7 else
                "medium" if risk_score >= 0.3 else
                "low" if risk_score >= 0.1 else
                "minimal"
            )

            yield {
                "user_id": user_id,
                "risk_score": round(risk_score, 4),
                "risk_level": risk_level,
                "predicted_fraud": fraud_label,
                "txn_count_last_10min": txn_count,
                "raw": payload
            }
        except Exception as e:
            yield {"error": str(e), "raw": element}

# --- Beam Pipeline ---
options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=options) as p:
    (
        p
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/fraud-detection-v1/subscriptions/test-sub")
        | "DecodeBytes" >> beam.Map(lambda x: x.decode("utf-8"))
        | "ScoreAndTrack" >> beam.ParDo(ScoreWithUserActivity())
        | "LogOutput" >> beam.Map(print)
    )
