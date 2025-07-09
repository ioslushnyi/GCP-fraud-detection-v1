# Generate fake payment events and publish them to a Google Cloud Pub/Sub topic
import time
from google.cloud import pubsub_v1
from datetime import datetime
from faker import Faker
import json
import uuid
import random

fake = Faker()
project_id = "fraud-detection-v1"
topic_id = "payment-events-test"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Generate a single fake payment event
def generate_fake_event():
    return {
        "user_id": str(uuid.uuid4()),
        "amount": round(random.uniform(10, 13000), 2),
        "currency": random.choice(["USD", "EUR", "PLN", "GBP", "UAH"]),
        "country": random.choice(["PL", "UA"]) if random.random() > 0.1 else fake.country_code(),
        "ip_country": random.choice(["PL", "UA"]) if random.random() > 0.1 else fake.country_code(),
        "device": random.choice(["iPhone", "Android", "Windows", "Linux", "Mac"]),
        "timestamp": datetime.now().isoformat()
    }

def publish_event(event):
    event_data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data=event_data)
    print(f"Published message ID: {future.result()}")

while True:
    try:
        event = generate_fake_event()

        print(f"Event: {event}")
        # random burst of events for the same user (10% chance)
        if (random.random() < 0.2 ):
            user_id = event["user_id"]
            for _ in range(random.randrange(5,10)):
                burst_event = generate_fake_event()
                burst_event["user_id"] = user_id
                print(f"Event sequence: {burst_event}")
                publish_event(burst_event)
            # Sleep for a short time to simulate burst
                time.sleep(1)
        else:
            publish_event(event)

    except Exception as e:
        print(f"Error publishing event: {e}")
    # Wait for a while before publishing the next event
    time.sleep(random.uniform(0.1, 5))  # Adjust the sleep time as needed