# Real-Time Fraud Detection with GCP

A real-time fraud detection system simulating payment events, scoring transactions using a trained ML model, and routing high-risk activity to alerting & analytics platforms. Built on Google Cloud Platform, this project demonstrates which tools can be used for streaming, pipeline orchestration, monitoring, and ML integration.

## Goal

Simulate a real-time system where user events (payments) are streamed in, enriched & scored by ML model, stored for analytics & reporting, and sent to real-time monitoring service.

## Tech Stack:

- **Cloud Infrastructure:** Google Cloud Platform (GCP)
- **Extraction:** Payments Generator (Python + [Faker](https://faker.readthedocs.io/en/master/))
- **Ingestion:** Pub/Sub
- **Stream Processing:** [Apache Beam](https://beam.apache.org/) (Python SDK), Dataflow
- **Messaging:** Pub/Sub
- **Storage:** BigQuery, Google Cloud Storage
- **Analytics & Reporting:** BigQuery, [Looker Studio](https://lookerstudio.google.com/u/0/navigation/reporting)
- **Real-Time Monitoring:** [InfluxDB Cloud](https://www.influxdata.com/products/influxdb-cloud/serverless/), [Grafana Cloud](https://grafana.com/products/cloud/)
- **API & Serving:** Cloud Run, [FastAPI](https://fastapi.tiangolo.com/)
- **ML Model:** Python, [scikit-learn](https://scikit-learn.org/), joblib
- **Orchestration:** Cloud Scheduler

## Architecture

![Architecture diagram - Real-Time Fraud Detection](/diagrams/architecture.png)

## Data Flow

1. Cloud Scheduler triggers the Payments Generator to run on Cloud Run
2. Events are sent to Pub/Sub _payment-events_ topic and Stored to GCS (via native Pub/Sub subscription)
3. Events are then consumed by Dataflow, where:
   Apache Beam Pipeline reads from Pub/Sub (pull subscription)
   Events are scored by an ML model
   Risk level is assigned to each event
4. From the pipeline result is sent to:
   BigQuery for storage & analytics
   Pub/Sub _scored-events_ topic → Consumed by FastAPI on Cloud Run → InfluxDB for real-time monitoring
5. Looker Studio and Grafana Cloud surface the results

6. Ingestion - Simulate and ingest payment data
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

```
./infrastructure/setup.sh
```

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

Please feel free to contact me if you have any questions:\
[Ihor Slushnyi](https://www.linkedin.com/in/ihor-slushnyi-a7b9441b4/)
