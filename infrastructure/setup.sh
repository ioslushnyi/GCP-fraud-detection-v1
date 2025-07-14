#!/bin/bash
# --------------------------------------------
# GCP Setup Script: Real-Time Fraud Detection
# --------------------------------------------
# ❗ATTENTION❗
# Before running this script, ensure you have the Google Cloud SDK installed and authenticated.
# Ensure you have the necessary permissions to create resources in your Google Cloud project.
# Ensure you have all the coniguration variables set correctly.

# CONFIGURATION GCP
PROJECT_ID="fraud-detection-v1"  # Replace with your Google Cloud project ID
REGION="us-central1" # Replace with your preferred region
BUCKET="fraud-detection-temp-bucket" # Replace with your GCS bucket name
BQ_DATASET="realtime_analytics" # Replace with your BigQuery dataset name
BQ_TABLE="fraud_scored_events" # Replace with your BigQuery table name
CLOUD_RUN_METRICS_URL="https://fraud-metrics-exporter-738698621086.us-central1.run.app/metrics-api/push" # Replace with your Cloud Run metrics URL

# CONFIGURATION INFLUXDB
# Ensure you have an InfluxDB instance running and accessible
# You can use InfluxDB Cloud or a self-hosted instance
# The following variables are used to connect to InfluxDB
# Replace these with real values or export them before running:
INFLUX_URL="YOUR_INFLUXDB_URL" 
INFLUX_TOKEN="YOUR_INFLUXDB_TOKEN"
INFLUX_ORG="YOUR_INFLUXDB_ORG"
INFLUX_BUCKET="YOUR_INFLUXDB_BUCKET"

# Enable APIs
echo "Enabling required APIs..."
gcloud services enable \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com \
  bigquery.googleapis.com \
  compute.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  artifactregistry.googleapis.com \
  cloudresourcemanager.googleapis.com

# Create Pub/Sub topics and subscription
echo "Creating Pub/Sub topics and subscriptions..."
gcloud pubsub topics create payment-events --project=$PROJECT_ID
gcloud pubsub subscriptions create payment-events-sub --topic=payment-events --project=$PROJECT_ID
gcloud pubsub topics create scored-events-fraud-metrics --project=$PROJECT_ID
gcloud pubsub subscriptions create scored-events-fraud-metrics-sub \
  --topic scored-events-fraud-metrics \
  --project=$PROJECT_ID \
  --push-endpoint=$CLOUD_RUN_METRICS_URL
  
# Create GCS bucket
echo "Creating GCS bucket..."
gsutil ls -b gs://$BUCKET >/dev/null 2>&1 || gsutil mb -l $REGION gs://$BUCKET/

# Create required subfolders for Dataflow
echo "Creating staging and temp folders..."
gsutil cp /dev/null gs://$BUCKET/temp/.init
gsutil cp /dev/null gs://$BUCKET/staging/.init

# Create BigQuery dataset
echo "Creating BigQuery dataset..."
bq ls $PROJECT_ID:$BQ_DATASET >/dev/null 2>&1 || bq --location=US mk -d $PROJECT_ID:$BQ_DATASET

# Clone repository
echo "Cloning project repository..."
git clone https://github.com/ioslushnyi/real-time-fraud-detection.git
cd real-time-fraud-detection

echo "Deploying fraud metrics export service to Cloud Run..."
gcloud run deploy fraud-metrics-exporter \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source ./fraud-metrics-exporter \
  --allow-unauthenticated \
  --set-env-vars=INFLUXDB_URL=$INFLUX_URL,INFLUXDB_TOKEN=$INFLUX_TOKEN,INFLUXDB_ORG=$INFLUX_ORG,INFLUXDB_BUCKET=$INFLUX_BUCKET

cd event-generator/cloud-run/
gcloud builds submit --tag gcr.io/fraud-detection-v1/event-generator
gcloud run jobs create event-generator-job \
  --image=gcr.io/$PROJECT_ID/event-generator \
  --region=$REGION \
  --args="--max_events=150","--min_time_between_events=10","--cooldown=120","--burst_chance=0.02","--max_duration=1740","--project=$PROJECT_ID","--output_topic=payment-events" \
  --task-timeout=1740s \
  --max-retries=0
gcloud run jobs execute event-generator-job --region=$REGION
echo "Creating Cloud Scheduler job for event generator..."
gcloud scheduler jobs create pubsub trigger-event-generator \
  --schedule="*/30 * * * *" \
  --topic=payment-events \
  --message-body="{}" \
  --project=$PROJECT_ID \
  --time-zone="UTC"

cd ../../
# Deploy Dataflow pipeline
echo "Deploying Dataflow pipeline..."
python beam/fraud_detection_pipeline.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=gs://$BUCKET/temp \
  --staging_location=gs://$BUCKET/staging \
  --input_subscription=projects/$PROJECT_ID/subscriptions/payment-events-sub \
  --output_topic=projects/$PROJECT_ID/topics/scored-events-fraud-metrics \
  --output_table=$PROJECT_ID.$BQ_DATASET.$BQ_TABLE

echo "Setup complete! Visit your Grafana dashboard or BigQuery to explore real-time fraud detection metrics."
