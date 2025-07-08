import json
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.coders import VarIntCoder
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
    ACTIVITY_WINDOW = 600  # 10 minutes in seconds
    STATE_SPEC = BagStateSpec("user_timestamps", VarIntCoder())  # stores timestamps per user

    def process(self, element, state=beam.DoFn.StateParam(STATE_SPEC)):
        try:
            user_id, payload_str = element
            payload = json.loads(payload_str)
            timestamp = datetime.fromisoformat(payload["timestamp"])
            event_ts = int(timestamp.timestamp())

            # --- UPDATE STATE (per user_id) ---
            state.add(event_ts)
            all_timestamps = list(state.read())
            # Filter timestamps to only those in the last 10 minutes
            recent_txns = [t for t in all_timestamps if event_ts - t <= self.ACTIVITY_WINDOW]

            # --- CLEAN OLD STATE ---
            state.clear()
            for t in recent_txns:
                state.add(t)

            txn_count = len(recent_txns)
            is_rapid_fraud = txn_count > 5  # ❗Our fraud condition

            # --- Feature extraction ---
            X = pd.DataFrame([{
                "amount": payload["amount"],
                "currency": le_currency.transform([payload["currency"]])[0] if payload["currency"] in le_currency.classes_ else -1,
                "country": le_country.transform([payload["country"]])[0] if payload["country"] in le_country.classes_ else -1,
                "ip_country": le_ip_country.transform([payload["ip_country"]])[0] if payload["ip_country"] in le_ip_country.classes_ else -1,
                "device": le_device.transform([payload["device"]])[0] if payload["device"] in le_device.classes_ else -1,
                "hour": timestamp.hour,
                "txn_count_last_10min": txn_count
            }])

            # --- ML model scoring ---
            risk_score = model.predict_proba(X)[0][1]
            ml_predicted_fraud = int(risk_score > 0.5)

            # --- Combine with user behavior rule ---
            fraud_label = int(ml_predicted_fraud or is_rapid_fraud)
            fraud_reason = "model" if ml_predicted_fraud else "txn_volume" if is_rapid_fraud else "none"
            risk_level = (
                "critical" if risk_score > 0.9 else
                "high" if risk_score > 0.7 else
                "medium-high" if risk_score > 0.5 else
                "medium" if risk_score > 0.3 else
                "low" if risk_score > 0.1 else
                "minimal"
            )

            enriched = {
                "user_id": user_id,
                "txn_count_last_10min": txn_count,
                "risk_score": round(risk_score, 4),
                "risk_level": risk_level,
                "fraud_reason": fraud_reason,
                "fraud_label": fraud_label,
                "raw_event": payload
            }

            yield enriched

        except Exception as e:
            yield {"error": str(e), "raw_event": element}

# --- Beam Pipeline Entry Point ---
def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/fraud-detection-v1/subscriptions/test-sub")
            | "DecodeBytes" >> beam.Map(lambda x: x.decode("utf-8"))
            | "KeyByUserId" >> beam.Map(lambda msg: (json.loads(msg)["user_id"], msg))  # ✅ Per-user keying
            | "ScoreAndTrack" >> beam.ParDo(ScoreWithUserActivity())
            | "PrintResults" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()
