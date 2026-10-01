"""
Member 2 - Digital Engagement & Conversion Intelligence
Retail FMCD Industry

Generates simulated website/app clickstream events (page views, searches,
cart activity) and marketing engagement events (campaign clicks, loyalty
activity), and streams them into the Kafka topic 'digital-engagement-data'.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = "digital-engagement-data"

PRODUCTS = [
    {"id": "P1001", "name": "55-inch Smart LED TV", "category": "Television"},
    {"id": "P1002", "name": "Double Door Refrigerator 260L", "category": "Refrigerator"},
    {"id": "P1003", "name": "1.5 Ton Split Inverter AC", "category": "Air Conditioner"},
    {"id": "P1004", "name": "Front Load Washing Machine 7Kg", "category": "Washing Machine"},
    {"id": "P1005", "name": "Microwave Oven 23L Convection", "category": "Kitchen Appliance"},
]

CAMPAIGNS = ["Diwali Dhamaka Sale", "Summer Cooling Fest", "App-Only Flash Sale", "Loyalty Week Offer"]
DEVICES = ["Mobile", "Desktop", "Tablet"]

CUSTOMER_IDS = [f"CUST{1000 + i}" for i in range(20)]

active_sessions = {}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def clickstream_event():
    global active_sessions
    customer_id = random.choice(CUSTOMER_IDS)
    product = random.choice(PRODUCTS)

    event = random.choices(
        ["page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start"],
        weights=[35, 20, 25, 5, 15],
        k=1,
    )[0]

    return {
        "event_type": event,
        "session_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "product_id": product["id"] if event != "search" else None,
        "product_name": product["name"] if event != "search" else None,
        "search_query": product["category"] if event == "search" else None,
        "device": random.choice(DEVICES),
        "timestamp": now_iso(),
    }


def marketing_event():
    event = random.choice(["campaign_impression", "campaign_click", "loyalty_points_earned", "offer_redeemed"])
    return {
        "event_type": event,
        "customer_id": random.choice(CUSTOMER_IDS),
        "campaign_name": random.choice(CAMPAIGNS),
        "channel": random.choice(["Email", "SMS", "Push Notification", "Social Media"]),
        "points": random.randint(10, 200) if event == "loyalty_points_earned" else None,
        "timestamp": now_iso(),
    }


def main():
    print(f"Starting Member 2 producer -> topic '{TOPIC}' (Ctrl+C to stop)\n")
    count = 0
    try:
        while True:
            record = clickstream_event() if random.random() < 0.7 else marketing_event()
            producer.send(TOPIC, value=record)
            count += 1
            print(f"[{count}] {record['event_type']} | {record['customer_id']}")
            time.sleep(random.uniform(0.3, 1.0))
    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()