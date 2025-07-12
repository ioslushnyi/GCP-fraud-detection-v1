# Generate fake payment events and publish them to a Google Cloud Pub/Sub topic
import time
from google.cloud import pubsub_v1
from datetime import datetime, timezone
from faker import Faker
import json
import uuid
import random
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--max_events", type=int, default=100)
parser.add_argument("--max_duration", type=int, default=1200)  # seconds (20 minutes)
parser.add_argument("--burst_chance", type=float, default=0.02)
parser.add_argument("--cooldown", type=int, default=60)  # seconds
args = parser.parse_args()

start_time = time.time()
fake = Faker()
project_id = "fraud-detection-v1"
topic_id = "payment-events"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Generate a single fake payment event
def generate_fake_event():
    # assume that 90% of users are from Poland or Ukraine
    country = random.choice(["PL", "UA"]) if random.random() > 0.1 else (random.choice(["DE", "CZ", "SK", "UK"]) if random.random() > 0.5 else fake.country_code())
    # and 10% use VPN, so we can generate random country code for them
    ip_country = country if random.random() > 0.1 else fake.country_code()
    # 95 of the users are making purchases between 100 and 12000 money units, 5% are making large purchases between 10000 and 20000 money units
    amount = round(random.uniform(100, 11000), 2) if random.random() > 0.1 else round(random.uniform(10000, 20000), 2)
    # 95% of users use USD, EUR, PLN or UAH, 5% use other currencies
    currency = random.choice(["USD", "EUR", "PLN", "UAH"]) if random.random() > 0.05 else fake.currency_code()
    return {
        "user_id": str(uuid.uuid4()),
        "amount": amount,
        "currency": currency,
        "country": country,
        "ip_country": ip_country,
        "device": random.choice(["iPhone", "Android", "Windows", "Linux", "Mac"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# Function to publish an event to the Pub/Sub topic
def publish_event(event):
    event_data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data=event_data)
    logging.info(f"Published message ID: {future.result()}")

logging.info(f"Starting event generator with max_events={args.max_events}, max_duration={args.max_duration}s, burst_chance={args.burst_chance}, cooldown={args.cooldown}s")

events_published = 0
last_burst_time = 0
while events_published < args.max_events and (time.time() - start_time < args.max_duration):
    try:
        event = generate_fake_event()
        # 2% chance and at least 60 seconds since last burst
        if random.random() <= args.burst_chance and time.time() - last_burst_time > args.cooldown:
            logging.info(f"Burst event sequence triggered for user {event['user_id']}")

            for _ in range(random.randrange(5, 8)):
                publish_event(event)
                events_published += 1
                # Sleep for a short time to simulate burst
                time.sleep(3)
            last_burst_time = time.time()
        else:
            publish_event(event)
    except Exception as e:
        logging.warning(f"Error occured when publishing event {event} \nError: {e}")
    # Wait for a while before publishing the next event
    time.sleep(random.uniform(5, 15))
    events_published += 1

logging.info(f"Finished publishing {events_published} events in {int(time.time() - start_time)} seconds.")
