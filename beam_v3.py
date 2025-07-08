import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.coders import VarIntCoder
from apache_beam.transforms.userstate import BagStateSpec, TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain

import joblib
import json
import pandas as pd
from datetime import datetime
import typing
import time
import argparse

# --- Load model and encoders globally ---
model = joblib.load("ml-model/fraud_model_v3.pkl")
le_currency = joblib.load("ml-model/le_currency_v3.pkl")
le_country = joblib.load("ml-model/le_country_v3.pkl")
le_ip_country = joblib.load("ml-model/le_ip_country_v3pkl")
le_device = joblib.load("ml-model/le_device_v3.pkl")
feature_order = joblib.load("ml-model/feature_order_v3.pkl")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', default='DirectRunner')
    return parser.parse_args()

def get_enriched_payload(payload: dict, risk_score: float, fraud_label: int, risk_level: str) -> dict:
    enriched = {
        "user_id": payload["user_id"],
        "event_time": payload["timestamp"],
        "amount": payload["amount"],
        "currency": payload["currency"],
        "country": payload["country"],
        "ip_country": payload["ip_country"],
        "device": payload["device"],
        "hour": datetime.fromisoformat(payload["timestamp"]).hour,
        "txn_count_last_10min": payload.get("txn_count_last_10min", 0),
        "fraud_score": risk_score,
        "fraud_label": fraud_label,
        "risk_level": risk_level
    }
    return enriched

# --- Stateful user event counter ---
class AddTxnCount(beam.DoFn):
    TXN_STATE = BagStateSpec('txn_timestamps', VarIntCoder())
    CLEANUP_TIMER = TimerSpec('cleanup', TimeDomain.WATERMARK)

    def process(self, element, txn_state=beam.DoFn.StateParam(TXN_STATE),
                         timer=beam.DoFn.TimerParam(CLEANUP_TIMER)):

        user_id, payload = element
        try:
            ts = int(datetime.fromisoformat(payload["timestamp"]).timestamp())
        except Exception:
            return  # Skip invalid timestamps

        # Add timestamp and set cleanup timer
        txn_state.add(ts)
        timer.set(datetime.fromtimestamp(ts + 600))

        # Get valid timestamps
        recent = [t for t in txn_state.read() if t >= ts - 600]

        txn_state.clear()
        for t in recent:
            txn_state.add(t)

        payload["txn_count_last_10min"] = len(recent)
        yield payload

    @on_timer(CLEANUP_TIMER)
    def on_cleanup(self, txn_state=beam.DoFn.StateParam(TXN_STATE)):
        now_ts = int(time.time())
        recent = [t for t in txn_state.read() if t >= now_ts - 600]
        txn_state.clear()
        for t in recent:
            txn_state.add(t)


# --- Fraud scoring function ---
def score_event(payload: dict) -> typing.Optional[str]:
    try:
        timestamp = datetime.fromisoformat(payload["timestamp"])

        X = pd.DataFrame([{
            "amount": payload["amount"],
            "currency": le_currency.transform([payload["currency"]])[0] if payload["currency"] in le_currency.classes_ else -1,
            "country": le_country.transform([payload["country"]])[0] if payload["country"] in le_country.classes_ else -1,
            "ip_country": le_ip_country.transform([payload["ip_country"]])[0] if payload["ip_country"] in le_ip_country.classes_ else -1,
            "device": le_device.transform([payload["device"]])[0] if payload["device"] in le_device.classes_ else -1,
            "hour": timestamp.hour,
            "txn_count_last_10min": payload.get("txn_count_last_10min", 0)
        }])
        assert all(col in X.columns for col in feature_order), "❌ Feature mismatch: some expected columns are missing"

        risk_score = model.predict_proba(X[feature_order])[0][1]
        fraud_label = int(risk_score > 0.5)
        risk_level = (
            "critical" if risk_score > 0.9 else
            "high" if risk_score > 0.7 else
            "medium-high" if risk_score > 0.5 else
            "medium" if risk_score > 0.3 else
            "low" if risk_score > 0.1 else
            "minimal"
        )

        print(f"\n📦 Event: {payload}")
        print(f"🧠 Risk Score: {risk_score:.4f}, Risk Level: {risk_level}, Fraud: {fraud_label}")
        
        # Enrich payload for downstream
        enriched = get_enriched_payload(payload, risk_score, fraud_label, risk_level)
        return json.dumps(enriched)

    except Exception as e:
        print("❌ Error in scoring:", e)
        return None


# --- Pipeline definition ---
def run():
    """options = PipelineOptions(
        streaming=True,
        project="fraud-detection-v1",
        region="europe-central2",
        temp_location="gs://your-bucket/tmp",
        job_name="fraud-streaming-pipeline"
    )"""
    args = parse_args()
    options = PipelineOptions()
    
    options.view_as(StandardOptions).runner = args.runner
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "FakeInput" >> beam.Create([
                {
                    "user_id": "u123",
                    "timestamp": "2025-07-08T18:15:00",
                    "amount": 89.99,
                    "currency": "USD",
                    "country": "US",
                    "ip_country": "US",
                    "device": "desktop"
                },
                {
                    "user_id": "u123",
                    "timestamp": "2025-07-08T18:18:00",
                    "amount": 30,
                    "currency": "USD",
                    "country": "US",
                    "ip_country": "US",
                    "device": "desktop"
                }
            ])
            #| "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/fraud-detection-v1/subscriptions/test-sub")
            | "Decode" >> beam.Map(lambda m: json.loads(m.decode("utf-8")))
            | "KeyByUser" >> beam.Map(lambda x: (x["user_id"], x))
            | "AddTxnCount" >> beam.ParDo(AddTxnCount())
            | "ScoreEvent" >> beam.Map(score_event)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            #| "WriteToGCS" >> beam.io.WriteToText("gs://your-bucket/fraud_output/scored", file_name_suffix=".json")
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table="your_project.your_dataset.fraud_scored_events",
                schema={
                    "fields": [
                        {"name": "user_id", "type": "STRING"},
                        {"name": "event_time", "type": "TIMESTAMP"},
                        {"name": "amount", "type": "FLOAT"},
                        {"name": "currency", "type": "STRING"},
                        {"name": "country", "type": "STRING"},
                        {"name": "ip_country", "type": "STRING"},
                        {"name": "device", "type": "STRING"},
                        {"name": "hour", "type": "INTEGER"},
                        {"name": "txn_count_last_10min", "type": "INTEGER"},
                        {"name": "fraud_score", "type": "FLOAT"},
                        {"name": "fraud_label", "type": "INTEGER"},
                        {"name": "risk_level", "type": "STRING"}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()
