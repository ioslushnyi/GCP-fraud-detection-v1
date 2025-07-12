## Desctiption

The script runs on Cloud Run and listens for incoming messages from Pub/Sub push subscription.
The code then exports metrics to Influx DB, which is connected to Grafana for real-time monitoring.

## Setup

.env.example is an example of actual .env file with required Influx DB configurations.
