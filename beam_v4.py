import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.coders import VarIntCoder
from apache_beam.transforms.userstate import BagStateSpec, TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain

import joblib
import json
import pandas as pd
from datetime import datetime, timezone
import typing
import time
import argparse
from typing import Tuple

# --- Load model and encoders globally ---
model = joblib.load("ml-model/fraud_model_v3.pkl")
le_currency = joblib.load("ml-model/le_currency_v3.pkl")
le_country = joblib.load("ml-model/le_country_v3.pkl")
le_ip_country = joblib.load("ml-model/le_ip_country_v3.pkl")
le_device = joblib.load("ml-model/le_device_v3.pkl")
feature_order = joblib.load("ml-model/feature_order_v3.pkl")


def safe_encode(encoder, value):
    return encoder.transform([value])[0] if value in encoder.classes_ else -1

def safe_decode(m):
    try:
        return json.loads(m.decode("utf-8"))
    except Exception as e:
        print("\u274c Decode error:", e)
        return None

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', default='DirectRunner', choices=['DirectRunner', 'DataflowRunner'])
    return parser.parse_args()

def get_enriched_event(event: dict, risk_score: float, fraud_label: int, risk_level: str) -> dict:
    event_time = event["timestamp"]
    if not event_time.endswith("Z"):
        event_time += "Z"
    return {
        "user_id": event["user_id"],
        "event_time": event_time,
        "amount": float(event["amount"]),
        "currency": event["currency"],
        "country": event["country"],
        "ip_country": event["ip_country"],
        "device": event["device"],
        "hour": int(datetime.fromisoformat(event["timestamp"]).hour),
        "txn_count_last_10min": int(event.get("txn_count_last_10min", 0)),
        "fraud_score": float(risk_score),
        "fraud_label": fraud_label,
        "risk_level": risk_level
    }

class AddTxnCount(beam.DoFn):
    TXN_STATE = BagStateSpec('txn_timestamps', VarIntCoder())
    CLEANUP_TIMER = TimerSpec('cleanup', TimeDomain.WATERMARK)

    def process(self, element: Tuple[str, dict], txn_state=beam.DoFn.StateParam(TXN_STATE),
                         timer=beam.DoFn.TimerParam(CLEANUP_TIMER)):
        user_id, event = element
        try:
            ts = int(datetime.fromisoformat(event["timestamp"]).timestamp())
        except Exception:
            return
        txn_state.add(ts)
        timer.set(datetime.fromtimestamp(ts + 600, tz=timezone.utc))
        recent = [t for t in txn_state.read() if t >= ts - 600]
        txn_state.clear()
        for t in recent:
            txn_state.add(t)
        event["txn_count_last_10min"] = len(recent)
        yield event

    @on_timer(CLEANUP_TIMER)
    def on_cleanup(self, txn_state=beam.DoFn.StateParam(TXN_STATE)):
        now_ts = int(time.time())
        recent = [t for t in txn_state.read() if t >= now_ts - 600]
        txn_state.clear()
        for t in recent:
            txn_state.add(t)

def score_event(event: dict) -> typing.Optional[dict]:
    try:
        timestamp = datetime.fromisoformat(event["timestamp"])
        X = pd.DataFrame([{
            "amount": event["amount"],
            "currency": safe_encode(le_currency, event["currency"]),
            "country": safe_encode(le_country, event["country"]),
            "ip_country": safe_encode(le_ip_country, event["ip_country"]),
            "device": safe_encode(le_device, event["device"]),
            "hour": timestamp.hour,
            "txn_count_last_10min": event["txn_count_last_10min"]
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
        return get_enriched_event(event, risk_score, fraud_label, risk_level)
    except Exception as e:
        print("\u274c Scoring error:", e)
        return None

def run():
    args = parse_args()

    runner_opts = []
    if args.runner == "DataflowRunner":
        runner_opts = [
            "--runner=DataflowRunner",
            "--project=fraud-detection-v1",
            "--region=us-central1",
            "--temp_location=gs://fraud-detection-temp-bucket/temp",
            "--staging_location=gs://fraud-detection-temp-bucket/staging",
            "--streaming"
        ]

    options = PipelineOptions(runner_opts)
    options.view_as(StandardOptions).runner = args.runner
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/fraud-detection-v1/subscriptions/test-sub")
            | "DecodePubSub" >> beam.Map(safe_decode)
            | "FilterValid" >> beam.Filter(lambda x: x is not None)
            | "KeyByUser" >> beam.Map(lambda x: (x["user_id"], x)).with_output_types(Tuple[str, dict])
            | "AddTxnCount" >> beam.ParDo(AddTxnCount())
            | "ScoreEvent" >> beam.Map(score_event)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            | "PrintOutput" >> beam.Map(print)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table="fraud-detection-v1.realtime_analytics.fraud_scored_events",
                schema="auto",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()
