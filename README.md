# Real-Time Fraud Detection with GCP

A real-time fraud detection system simulating payment events, scoring transactions using a trained ML model, and routing high-risk activity to alerting & analytics platforms. Built on Google Cloud Platform, this project demonstrates how to integrate streaming, orchestration, monitoring, and machine learning into a real-time fraud detection system.

![Grafana Demo](/dashboards/images/animation_grafana.gif)

## Goal

Simulate a real-time system where user events (payments) are streamed in, enriched & scored by ML model, stored for analytics and sent to real-time monitoring service.

## Tech Stack:

- **Cloud Infrastructure:** Google Cloud Platform (GCP)
- **Extraction:** Payments Generator (Python + [Faker](https://faker.readthedocs.io/en/master/))
- **Ingestion:** Pub/Sub
- **Stream Processing:** [Apache Beam](https://beam.apache.org/) (Python SDK), Dataflow
- **Storage:** BigQuery, Google Cloud Storage
- **Analytics & Reporting:** BigQuery, [Looker Studio](https://lookerstudio.google.com/u/0/navigation/reporting)
- **Real-Time Monitoring:** [InfluxDB Cloud](https://www.influxdata.com/products/influxdb-cloud/serverless/), [Grafana Cloud](https://grafana.com/products/cloud/)
- **API & Serving:** Cloud Run, [FastAPI](https://fastapi.tiangolo.com/)
- **ML Model:** Python, [scikit-learn](https://scikit-learn.org/), [joblib](https://joblib.readthedocs.io/en/stable/)
- **Scheduling / Automation:** Cloud Scheduler

## Architecture
![Architecture diagram - Real-Time Fraud Detection](/diagrams/architecture.png)

## Data Flow

**1.** Cloud Scheduler triggers the Payments Generator to run on Cloud Run Job
**2.** Events are sent to Pub/Sub _payment-events_ topic and Stored to GCS (via native Pub/Sub subscription)
**3.** Events are then consumed by Dataflow, where:
 - Apache Beam Pipeline reads from Pub/Sub (pull subscription)
 - Events are scored by an ML model
 - Risk level is assigned to each event
**4.** From the pipeline results are sent to:
 - BigQuery for storage & analytics
 - Pub/Sub _scored-events_ topic → Consumed by FastAPI on Cloud Run Service → InfluxDB for real-time monitoring
**5.** Dashboards in Looker Studio and Grafana Cloud present anylytics/metrics

## How It Works

- ML model (Random Forest Classifier) is pre-trained to classify fraudulent payments
- Apache Beam pipeline loads the model and performs the scoring
- The pipeline enriches each event with a fraud_score and a corresponding risk_level
- Messages are routed based on output needs (storage, monitoring)

## Dashboards

- ### [Grafana Cloud](https://ihorslushnyi.grafana.net/public-dashboards/c58a9a27503147cda341f799c3a84ad6)
  [![Grafana Cloud](/dashboards/images/grafana-preview.png)](https://ihorslushnyi.grafana.net/public-dashboards/c58a9a27503147cda341f799c3a84ad6)
- ### [Looker Studio](https://lookerstudio.google.com/embed/reporting/a3a86a23-b364-4f25-8ff8-aef881fb0ad6/page/DlyQF)
  [![Looker Studio](/dashboards/images/looker-studio-preview.png)](https://lookerstudio.google.com/embed/reporting/a3a86a23-b364-4f25-8ff8-aef881fb0ad6/page/DlyQF)

## Setup

The script for setting up the project locally can be found in infrastructure folder:

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
