#!/bin/bash

# ─── CONFIGURATION ────────────────────────────────────────────
PROJECT_ID="fraud-detection-v1"
REGION="us-central1"
BQ_DATASET="realtime_analytics"
BQ_TABLE="fraud_scored_events"
CLOUD_RUN_METRICS_URL="https://your-cloud-run-url.a.run.app/metrics"

# Replace these with real values or export as env vars before running
INFLUX_URL="https://your-influxdb-host:8086/api/v2/write"
INFLUX_TOKEN="your-influx-token"
INFLUX_ORG="your-org"
INFLUX_BUCKET="your-bucket"

# ─── TOPICS ────────────────────────────────────────────────────

echo "📌 Creating Pub/Sub topics..."
gcloud pubsub topics create payment-events --project=$PROJECT_ID
gcloud pubsub topics create scored-events-fraud-metrics --project=$PROJECT_ID

# ─── SUBSCRIPTIONS ────────────────────────────────────────────

echo "📌 Creating subscriptions..."
# Beam (pulls raw payment events)
gcloud pubsub subscriptions create payment-events-sub \
  --topic=payment-events \
  --project=$PROJECT_ID

# Cloud Run (receives scored events)
gcloud pubsub subscriptions create scored-events-fraud-metrics-sub \
  --topic=scored-events \
  --push-endpoint=$CLOUD_RUN_METRICS_URL \
  --push-auth-token-audience=$CLOUD_RUN_METRICS_URL \
  --project=$PROJECT_ID

# ─── BIGQUERY ─────────────────────────────────────────────────

echo "📌 Creating BigQuery dataset..."
bq --location=US mk --dataset ${PROJECT_ID}:${BQ_DATASET}

# Optional: create table manually (Beam can create it if CREATE_IF_NEEDED is used)

# ─── CLOUD RUN (FastAPI Metrics API) ──────────────────────────

echo "🚀 Deploying Cloud Run metrics API..."
gcloud run deploy metrics-exporter \
  --source ./metrics-api \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --project=$PROJECT_ID \
  --set-env-vars INFLUX_URL=$INFLUX_URL,INFLUX_TOKEN=$INFLUX_TOKEN,INFLUX_ORG=$INFLUX_ORG,INFLUX_BUCKET=$INFLUX_BUCKET \
  --runtime python310

echo "✅ All infrastructure setup completed!"
