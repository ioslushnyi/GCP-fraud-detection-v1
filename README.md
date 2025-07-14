# Real-Time Fraud Detection with GCP

A real-time fraud detection system simulating payment events, scoring transactions using a trained ML model, and routing high-risk activity to alerting & analytics platforms. Built on Google Cloud Platform, this project demonstrates which tools can be used for streaming, pipeline orchestration, monitoring, and ML integration.

## Goal

Simulate a real-time system where user events (payments) are streamed in, enriched & scored by ML model, stored for analytics & reporting, and sent to real-time monitoring service.

## Tech Stack:

Cloud Infrastructure: Google Cloud Platform (GCP)\
Extraction: Python script running on Cloud Run (push)\
Ingestion: Pub/Sub\
Stream Processing: Apache Beam (Python SDK), Dataflow\
Messaging: Pub/Sub\
Storage: BigQuery, Google Cloud Storage, Looker Studio\
Analytics & Reporting: BigQuery, Looker Studio\
Real-Time Monitoring: InfluxDB, Grafana Cloud\
API & Serving: Cloud Run, FastAPI\
ML Model: Python, scikit-learn, joblib\
Orchestration: Cloud Scheduler

## Architecture

![Architecture diagram - Real-Time Fraud Detection](/diagrams/architecture.png)

## Data Lifecycle Overview

1. Ingestion - Simulate and ingest payment data
   Tool: Custom Python script using Faker + Pub/Sub

- Use Python + Faker to generate fake transactions
- Simulate irregular patterns (e.g. large amount, strange hours, IP mismatch)
- Push records to Pub/Sub using google-cloud-pubsub

2. Streaming Ingestion with Pub/Sub
   Tool: GCP Pub/Sub

- Acts as the real-time ingestion pipe
- Multiple subscriptions can consume the stream

3. Transformation / Scoring / Enrichment
   Tool: Apache Beam on Dataflow

- Beam Pipeline reads from Pub/Sub
- Enrich with reference data or other streams
- Feature engineering and fraud scoring (using ML algorithm "Random Forest Classifier", sklearn lib)
- Route suspicious events to separate sink

4. Load (Real-time Sink)
   Tool: BigQuery streaming insert

- Write transformed data into BigQuery
- Use partitioned/clustering tables

5. Visual presentation / Serving

- Looker studio, dashboards

## Setup

The script for setting up the project locally can be found in infrastructure folder:
`./infrastructure/setup.sh`
