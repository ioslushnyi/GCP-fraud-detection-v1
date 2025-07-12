# This code implements a real-time fraud detection pipeline using Apache Beam.
# It reads payment events from a Pub/Sub topic, calculates their fraud risk scores using a pre-trained ML model,
# and writes the enriched events to BigQuery. 
# It also publishes scored events to another Pub/Sub topic for further export to influx db for real-time monitoring.

# --- Import libraries ---
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.coders import VarIntCoder
from apache_beam.transforms.userstate import BagStateSpec, TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain
from google.cloud import pubsub_v1
import joblib
import json
import pandas as pd
from datetime import datetime, timezone
import typing
import time
import argparse
from typing import Tuple
import logging
import os

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# --- Define paths for model and encoders ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # gets path to beam-pipeline/
MODEL_PATH = os.path.join(BASE_DIR, "../ml-model/fraud_model.pkl")
LE_CURRENCY_PATH = os.path.join(BASE_DIR, "../ml-model/le_currency.pkl")
LE_COUNTRY_PATH = os.path.join(BASE_DIR, "../ml-model/le_country.pkl")
LE_IP_COUNTRY_PATH = os.path.join(BASE_DIR, "../ml-model/le_ip_country.pkl")
LE_DEVICE_PATH = os.path.join(BASE_DIR, "../ml-model/le_device.pkl")
FEATURE_ORDER_PATH = os.path.join(BASE_DIR, "../ml-model/feature_order.pkl")

# --- Load model and encoders globally ---
try:
    model = joblib.load(MODEL_PATH)
    le_currency = joblib.load(LE_CURRENCY_PATH)
    le_country = joblib.load(LE_COUNTRY_PATH)
    le_ip_country = joblib.load(LE_IP_COUNTRY_PATH)
    le_device = joblib.load(LE_DEVICE_PATH)
    feature_order = joblib.load(FEATURE_ORDER_PATH)
except Exception as e:
    logging.error(f"❌ Failed load ml model or encoders: {e}")

# --- Utility functions ---
# This function encodes categorical values using pre-fitted LabelEncoders.
def safe_encode(encoder, value):
    return encoder.transform([value])[0] if value in encoder.classes_ else -1
# This function decodes bytes to JSON
def safe_decode(m):
    try:
        return json.loads(m.decode("utf-8"))
    except Exception as e:
        logging.error(f"❌ Decode error: {e}")
        return None
# This function sets up argument parsing for the pipeline runner.
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', default='DirectRunner', choices=['DirectRunner', 'DataflowRunner'])
    return parser.parse_args()

# --- Function to enrich events ---
# This function formats the event with additional fields for BigQuery.
def get_enriched_event(event: dict, risk_score: float, fraud_label: int, risk_level: str) -> dict:
    return {
        "user_id": event["user_id"],
        "event_time": event["timestamp"], # "event_time": datetime.fromisoformat(event["timestamp"]).replace(microsecond=0).isoformat() + "Z",
        "amount": float(event["amount"]), # Ensure amount is a float
        "currency": event["currency"],
        "country": event["country"],
        "ip_country": event["ip_country"],
        "device": event["device"],
        "hour": int(datetime.fromisoformat(event["timestamp"]).astimezone(timezone.utc).hour), #int(datetime.fromisoformat(event["timestamp"]).hour), # Ensure hour is an integer
        "txn_count_last_10min": int(event.get("txn_count_last_10min", 0)), # Ensure txn_count_last_10min is an integer
        "fraud_score": float(risk_score), # Ensure fraud_score is a float
        "fraud_label": int(fraud_label), # Ensure fraud_label is an integer
        "risk_level": risk_level
    }

# --- Beam DoFn to add transaction count ---
# This function maintains a state of transaction timestamps and counts transactions in the last 10 minutes.
class AddTxnCount(beam.DoFn):
    TXN_STATE = BagStateSpec('txn_timestamps', VarIntCoder())
    CLEANUP_TIMER = TimerSpec('cleanup', TimeDomain.WATERMARK)

    def process(self, element: Tuple[str, dict], txn_state=beam.DoFn.StateParam(TXN_STATE),
                         timer=beam.DoFn.TimerParam(CLEANUP_TIMER)):
        user_id, event = element
        try:
            ts = int(datetime.fromisoformat(event["timestamp"]).timestamp())
        except Exception:
            ts = int(datetime.now(timezone.utc).timestamp())
            logging.warning(f"⚠️ Invalid event_time format ({event['timestamp']}), using current time instead.")
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

# --- Function to score events ---
# This function takes an event, processes it, and returns an enriched event with risk score and risk level.
def score_event(event: dict) -> typing.Optional[dict]:
    def get_risk_level(risk_score: float) -> str:
        return  (
            "critical" if risk_score > 0.9 else
            "high" if risk_score > 0.7 else
            "medium" if risk_score > 0.5 else
            "low" if risk_score > 0.1 else
            "minimal"
        )
    try:
        X = pd.DataFrame([{
            "amount": event["amount"],
            "currency": safe_encode(le_currency, event["currency"]),
            "country": safe_encode(le_country, event["country"]),
            "ip_country": safe_encode(le_ip_country, event["ip_country"]),
            "device": safe_encode(le_device, event["device"]),
            "hour": datetime.fromisoformat(event["timestamp"]).astimezone(timezone.utc).hour, #"hour": datetime.fromisoformat(event["timestamp"]).hour,
            "txn_count_last_10min": event["txn_count_last_10min"]
        }])

        risk_score = model.predict_proba(X[feature_order])[0][1]
        fraud_label = int(risk_score > 0.5)
        risk_level = get_risk_level(risk_score)

        return get_enriched_event(event, risk_score, fraud_label, risk_level)
    except Exception as e:
        logging.error(f"❌ Scoring error: {e}")
        return None

# --- Beam DoFn to publish metrics to Pub/Sub ---
# This function publishes the processed event metrics to a Pub/Sub topic.
class PublishMetricsToPubSub(beam.DoFn):
    def __init__(self, topic_path):
        self.topic_path = topic_path
        self.publisher = None

    def setup(self):
        self.publisher = pubsub_v1.PublisherClient()

    def process(self, event):
        payload = {
            "user_id": event["user_id"],
            "event_time": event["event_time"],
            "fraud_score": event["fraud_score"],
            "risk_level": event["risk_level"]
        }
        data = json.dumps(payload).encode("utf-8")
        try:
            self.publisher.publish(self.topic_path, data=data)
        except Exception as e:
            logging.error(f"❌ Failed to publish message: {e}")
        yield event  # Continue downstream

# --- Beam DoFn to log rows ---
# This function logs each row processed in the pipeline for debugging purposes.
class LogRow(beam.DoFn):
    def process(self, element):
        logging.info(f"🧪 {element}")
        yield element

# --- Main runner function ---
# This function sets up the Apache Beam pipeline with the specified options and transforms.
def run_pipeline():
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
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/fraud-detection-v1/subscriptions/payment-events-sub")
            | "DecodePubSub" >> beam.Map(safe_decode)
            | "FilterValid" >> beam.Filter(lambda x: x is not None)
            | "KeyByUser" >> beam.Map(lambda x: (x["user_id"], x)).with_output_types(Tuple[str, dict])
            | "AddTxnCount" >> beam.ParDo(AddTxnCount())
            | "ScoreEvent" >> beam.Map(score_event)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            | "LogRow" >> beam.ParDo(LogRow())
            | "PublishMetrics" >> beam.ParDo(PublishMetricsToPubSub("projects/fraud-detection-v1/topics/scored-events-fraud-metrics"))
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table="fraud-detection-v1.realtime_analytics.fraud_scored_events",
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
    run_pipeline()
