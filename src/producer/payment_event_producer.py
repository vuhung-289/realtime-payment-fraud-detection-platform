import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer

from src.common.settings import Settings


COUNTRIES = ["VN", "SG", "US", "JP", "TH", "ID"]
MERCHANTS = [f"m_{i:03d}" for i in range(1, 31)]
STATUSES = ["success", "failed"]


def build_event() -> dict:
    user_id = f"u_{random.randint(1, 200):04d}"
    is_fraud_pattern = random.random() < 0.08

    amount = round(random.uniform(5, 250), 2)
    country = random.choice(COUNTRIES[:3])
    status = random.choices(STATUSES, weights=[0.92, 0.08], k=1)[0]

    if is_fraud_pattern:
        amount = round(random.uniform(500, 3500), 2)
        country = random.choice(COUNTRIES[3:])
        status = random.choices(STATUSES, weights=[0.6, 0.4], k=1)[0]

    return {
        "transaction_id": str(uuid4()),
        "event_time": datetime.now(tz=timezone.utc).isoformat(),
        "user_id": user_id,
        "device_id": f"d_{random.randint(1, 400):04d}",
        "country": country,
        "merchant_id": random.choice(MERCHANTS),
        "amount": amount,
        "currency": "USD",
        "payment_status": status,
        "is_international": str(country != "VN").lower(),
    }


def main() -> None:
    settings = Settings()
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    eps = max(settings.producer_events_per_second, 1)
    sleep_seconds = 1.0 / eps

    print(
        f"Producing to topic '{settings.kafka_topic_payments}' "
        f"at ~{eps} events/sec on {settings.kafka_bootstrap_servers}"
    )
    while True:
        event = build_event()
        producer.send(settings.kafka_topic_payments, event)
        producer.flush()
        print(
            f"sent tx={event['transaction_id']} user={event['user_id']} "
            f"amount={event['amount']} country={event['country']} status={event['payment_status']}"
        )
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()

