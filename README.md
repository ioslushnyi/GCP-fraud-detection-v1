# Real-Time Fraud Detection with GCP

A real-time fraud detection system that simulates payment events, scores them using a trained ML model, and routes results to analytics & monitoring platforms.\
This project demonstrates how to integrate streaming (Dataflow), monitoring (InfluxDB + Grafana), and machine learning (scikit-learn) into a production-style pipeline.

![Grafana Demo](/dashboards/images/animation_grafana.gif)

## Goal

Simulate a real-time fraud detection system where user events (payments) are streamed in, enriched & scored by ML model, stored for analytics and sent to real-time monitoring service.

## Tech Stack

- **Cloud Infrastructure:** Google Cloud Platform (GCP)
- **Extraction:** Payments Generator (Python + [Faker](https://faker.readthedocs.io/en/master/))
- **Ingestion:** Pub/Sub
- **Stream Processing:** [Apache Beam](https://beam.apache.org/) (Python SDK), Dataflow
- **ML Model:** Python, [scikit-learn](https://scikit-learn.org/), [joblib](https://joblib.readthedocs.io/en/stable/)
- **Storage:** BigQuery, Google Cloud Storage
- **Analytics & Reporting:** BigQuery, [Looker Studio](https://lookerstudio.google.com/u/0/navigation/reporting)
- **API & Serving:** Cloud Run, [FastAPI](https://fastapi.tiangolo.com/)
- **Real-Time Monitoring:** [InfluxDB Cloud](https://www.influxdata.com/products/influxdb-cloud/serverless/), [Grafana Cloud](https://grafana.com/products/cloud/)
- **Scheduling / Automation:** Cloud Scheduler

## Architecture
![Architecture diagram - Real-Time Fraud Detection](/diagrams/architecture.png)

## How It Works

### Data Flow

1. Cloud Scheduler triggers the Payments Generator to run on a Cloud Run Job
2. Events are sent to Pub/Sub _payment-events_ topic and stored to GCS (via native Pub/Sub subscription)
3. Events are then consumed by Dataflow, where:
   - Apache Beam pipeline reads from Pub/Sub (pull subscription)
   - Events are scored by ML model
   - Risk level and a corresponding fraud score is assigned to each event
4. From the pipeline results are routed to:
   - BigQuery for storage and analytics
   - Pub/Sub _scored-events_ topic
      - Events are pushed via a Pub/Sub subscription and consumed by a FastAPI service running on Cloud Run
      - The service then sends the data to InfluxDB for real-time monitoring
5. Dashboards in Looker Studio and Grafana Cloud present analytics and real-time metrics, respectively

### Fraud Detection Logic

Each payment event is scored using a pre-trained Random Forest Classifier model. The model uses the following features:

- `amount`
- `currency` (label-encoded)
- `country` (label-encoded)
- `ip_country` (label-encoded)
- `device` (label-encoded)
- `hour` (UTC)
- `txn_count_last_10min` (per user)

The output `fraud_score` is a probability from 0 to 1. Based on this score:

- `fraud_label = 1` if `fraud_score > 0.5`, else `0`
- `risk_level` is assigned as:
  - `critical`: >= 0.9
  - `high`: >= 0.7
  - `medium`: >= 0.4
  - `low`: >= 0.1
  - `minimal`: ≤ 0.09

For implementation details and model structure, see [`ml-model/`](ml-model/).

## Dashboards

### [Grafana Cloud](https://ihorslushnyi.grafana.net/public-dashboards/c58a9a27503147cda341f799c3a84ad6)
  [![Grafana Cloud](/dashboards/images/grafana_preview.png)](https://ihorslushnyi.grafana.net/public-dashboards/c58a9a27503147cda341f799c3a84ad6)
### [Looker Studio](https://lookerstudio.google.com/embed/reporting/a3a86a23-b364-4f25-8ff8-aef881fb0ad6/page/DlyQF)
  [![Looker Studio](/dashboards/images/looker_studio_preview.png)](https://lookerstudio.google.com/embed/reporting/a3a86a23-b364-4f25-8ff8-aef881fb0ad6/page/DlyQF)

### BigQuery Views
BigQuery views used by Looker Studio are available in [bigquery/views](bigquery/views)

## Setup

The script for setting up the project is located in the infrastructure folder:

```
./infrastructure/setup.sh
```

## Limitations / Future Improvements

- No dead-letter queue or retry logic for failed events
- No custom windowing or watermarking in Beam pipeline
- Model is a simple classifier and not trained on real fraud data
- Alerting system (e.g., Slack/Telegram) not yet implemented

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

Please feel free to contact me if you have any questions:\
[Ihor Slushnyi](https://www.linkedin.com/in/ihor-slushnyi-a7b9441b4/)
