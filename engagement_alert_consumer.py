"""
Member 2 - Digital Engagement & Conversion Intelligence
Retail FMCD Industry

Consumes clickstream/marketing events from Kafka topic
'digital-engagement-data', writes every event live into MongoDB, and
raises a cart-abandonment alert (also stored live in MongoDB) when a
customer adds a product to cart but does not check out within
ABANDONMENT_WINDOW_SECONDS.
"""

import json
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer

from atlas_config import get_atlas_db, to_datetime

TOPIC = "digital-engagement-data"
ABANDONMENT_WINDOW_SECONDS = 15

# --- MongoDB setup ---
mongo_client, db = get_atlas_db()  # MongoDB Atlas (cloud), see atlas_config.py
events_collection = db["engagement_events"]
alerts_collection = db["engagement_alerts"]

# --- Kafka consumer setup ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="engagement-alert-consumer-atlas",
    consumer_timeout_ms=2000,  # lets us periodically check for stale carts even if no new message arrives
)

# customer_id -> {"product_name": ..., "added_at": wall-clock time.time()}
active_carts = {}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def raise_abandonment_alert(customer_id, cart_info):
    alert = {
        "alert_type": "CART_ABANDONMENT",
        "customer_id": customer_id,
        "product_name": cart_info["product_name"],
        "seconds_since_added": round(time.time() - cart_info["added_at"], 1),
        "detected_at": now_iso(),
        "detected_time": datetime.now(timezone.utc),
    }
    alerts_collection.insert_one(alert)
    print(f"  !!! ALERT [CART_ABANDONMENT] -> customer={customer_id} product={cart_info['product_name']}")


def check_stale_carts():
    """Scan tracked carts and fire an alert for any that have been
    sitting unconverted for longer than the abandonment window."""
    stale_customers = []
    for customer_id, cart_info in active_carts.items():
        if time.time() - cart_info["added_at"] > ABANDONMENT_WINDOW_SECONDS:
            raise_abandonment_alert(customer_id, cart_info)
            stale_customers.append(customer_id)
    for customer_id in stale_customers:
        del active_carts[customer_id]


def main():
    print(f"Listening on topic '{TOPIC}' and writing to MongoDB 'fmcd_streaming' database...\n")
    try:
        while True:
            # poll for new messages (times out after 2s so we can still check stale carts)
            for message in consumer:
                record = message.value

                # 1. Always store the raw event live in MongoDB
                events_collection.insert_one({**record, "event_time": to_datetime(record["timestamp"])})
                print(f"[stored] {record['event_type']} | {record['customer_id']}")

                # 2. Track cart activity
                customer_id = record["customer_id"]
                if record["event_type"] == "add_to_cart":
                    active_carts[customer_id] = {
                        "product_name": record["product_name"],
                        "added_at": time.time(),
                    }
                elif record["event_type"] == "checkout_start":
                    # successful conversion -> clear tracked cart, no alert
                    active_carts.pop(customer_id, None)

                check_stale_carts()

            # consumer_timeout_ms triggers StopIteration periodically; loop back and check again
            check_stale_carts()

    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    main()