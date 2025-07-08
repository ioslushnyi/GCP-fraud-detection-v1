# main_pipeline.py

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
model = joblib.load("fraud_model_v3.pkl")
le_currency = joblib.load("le_currency_v3.pkl")
le_country = joblib.load("le_country_v3.pkl")
le_ip_country = joblib.load("le_ip_country_v3.pkl")
le_device = joblib.load("le_device_v3.pkl")
feature_order = joblib.load("feature_order_v3.pkl")

# --- Parse command line arguments ---
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', default='DirectRunner')
    #parser.add_argument('--env', default='local', choices=['local', 'production'])
    return parser.parse_args()

# --- Helper: build enriched output ---
def get_enriched_payload(payload: dict, risk_score: float, fraud_label: int, risk_level: str) -> dict:
    return {
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

# --- User state tracker ---
class AddTxnCount(beam.DoFn):
    TXN_STATE = BagStateSpec('txn_timestamps', VarIntCoder())
    CLEANUP_TIMER = TimerSpec('cleanup', TimeDomain.WATERMARK)

    def process(self, element, txn_state=beam.DoFn.StateParam(TXN_STATE),
                         timer=beam.DoFn.TimerParam(CLEANUP_TIMER)):
        user_id, payload = element
        try:
            ts = int(datetime.fromisoformat(payload["timestamp"]).timestamp())
        except Exception:
            return
        txn_state.add(ts)
        timer.set(datetime.fromtimestamp(ts + 600))
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

# --- Fraud scoring ---
def score_event(payload: dict) -> typing.Optional[dict]:
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
        return get_enriched_payload(payload, risk_score, fraud_label, risk_level)
    except Exception as e:
        print("❌ Scoring error:", e)
        return None

# --- Main runner ---
def run():
    args = parse_args()

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = args.runner
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/fraud-detection-v1/subscriptions/test-sub")
            | "DecodePubSub" >> beam.Map(lambda m: json.loads(m.decode("utf-8")))
            | "KeyByUser" >> beam.Map(lambda x: (x["user_id"], x))
            | "AddTxnCount" >> beam.ParDo(AddTxnCount())
            | "ScoreEvent" >> beam.Map(score_event)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            #| "PrintOutput" >> beam.Map(print) NOT FRO PRODUCTION
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
