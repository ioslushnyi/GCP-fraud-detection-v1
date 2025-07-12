## Real-Time Fraud Detection with GCP

## Project Objective:

Build a real-time fraud detection system to analyse incoming payment data streams using Google Cloud Platform

## Goal

Simulate a real-time system where user events (payment) are streamed in, enriched, cleaned, and sent to BigQuery for live dashboards.

## Tech Stack:

Event Source: Simulated user events (Python script) - Running via Cloud Run
Ingestion: GCP Pub/Sub
Processing: Dataflow (Apache Beam, Python SDK)
Storage: BigQuery + raw events into GCS (using native ub/Sub subscription to Cloud Storage)
Dashboard/Visualization: Looker Studio

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
