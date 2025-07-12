## Running Locally

Use `local/generate_events.py` to simulate events without GCP costs.
Useful for testing the model pipeline or BigQuery ingestion locally.

## Running in Cloud Run

Use `cloud_run/generate_events.py`, which is packaged in a Docker container and deployed to Cloud Run.
Can be triggered manually or scheduled with Cloud Scheduler.
