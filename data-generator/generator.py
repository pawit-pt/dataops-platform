"""
Data Generator: Simulates ride-sharing trip events
Publishes JSON events to Kafka topic: raw.trip.events

Schema matches the PySpark schema from Problem 1:
  - driver_ID (String)
  - customer_id (Long)
  - transactionRecord (nested: transaction_id, trip_info, createdAt)
  - COORDINATES (3D array of doubles)

This represents the "External Source" in our architecture.
In production this would be replaced by:
  - Debezium CDC connector (for databases)
  - Kafka Connect REST source (for APIs)
  - Kafka Connect S3 source (for files)
"""

import json
import random
import time
import os
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "raw.trip.events")
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "5"))

# Bangkok-area routes with realistic coordinates
ROUTES = [
    {
        "name": "Downtown to Airport Express",
        "segments": [
            {"id": "SEG-001", "metrics": [12.5, 3.2, 45.0]},
            {"id": "SEG-002", "metrics": [8.3, 2.1, 38.0]},
            {"id": "SEG-003", "metrics": [15.1, 4.5, 52.0]},
        ],
        "coords": [
            [[13.7563, 100.5018, 0.0], [13.7469, 100.5300, 0.0], [13.7380, 100.5600, 0.0]],
        ],
    },
    {
        "name": "Sukhumvit to Silom Direct",
        "segments": [
            {"id": "SEG-010", "metrics": [5.2, 1.8, 25.0]},
            {"id": "SEG-011", "metrics": [3.7, 1.2, 18.0]},
        ],
        "coords": [
            [[13.7310, 100.5710, 0.0], [13.7280, 100.5530, 0.0]],
        ],
    },
    {
        "name": "Chatuchak to Sathorn Via Expressway",
        "segments": [
            {"id": "SEG-020", "metrics": [10.0, 3.0, 42.0]},
            {"id": "SEG-021", "metrics": [7.5, 2.5, 35.0]},
            {"id": "SEG-022", "metrics": [6.2, 2.0, 30.0]},
            {"id": "SEG-023", "metrics": [4.8, 1.5, 22.0]},
        ],
        "coords": [
            [[13.7999, 100.5533, 0.0], [13.7750, 100.5450, 0.0],
             [13.7500, 100.5350, 0.0], [13.7210, 100.5250, 0.0]],
        ],
    },
    {
        "name": "Ratchada to Bangna Route",
        "segments": [
            {"id": "SEG-030", "metrics": [9.1, 2.8, 40.0]},
            {"id": "SEG-031", "metrics": [11.3, 3.5, 48.0]},
        ],
        "coords": [
            [[13.7620, 100.5740, 0.0], [13.6700, 100.6050, 0.0]],
        ],
    },
    {
        "name": "Siam to Don Mueang Airport",
        "segments": [
            {"id": "SEG-040", "metrics": [14.0, 4.2, 55.0]},
            {"id": "SEG-041", "metrics": [12.8, 3.8, 50.0]},
            {"id": "SEG-042", "metrics": [16.5, 5.0, 60.0]},
        ],
        "coords": [
            [[13.7460, 100.5340, 0.0], [13.8000, 100.5500, 0.0], [13.9133, 100.6070, 0.0]],
        ],
    },
    {
        "name": "Thonburi to Lat Phrao Express",
        "segments": [
            {"id": "SEG-050", "metrics": [8.0, 2.4, 33.0]},
            {"id": "SEG-051", "metrics": [6.5, 2.0, 28.0]},
        ],
        "coords": [
            [[13.7220, 100.4870, 0.0], [13.7850, 100.5690, 0.0]],
        ],
    },
]

DRIVER_IDS = [f"DRV-{i:05d}" for i in range(1, 51)]  # 50 drivers


def add_jitter(coords, jitter=0.002):
    """Add small random jitter to coordinates for variety."""
    result = []
    for polygon in coords:
        new_polygon = []
        for point in polygon:
            new_polygon.append([
                round(point[0] + random.uniform(-jitter, jitter), 4),
                round(point[1] + random.uniform(-jitter, jitter), 4),
                point[2],
            ])
        result.append(new_polygon)
    return result


def generate_trip():
    """Generate a ride-sharing trip event matching the PySpark schema."""
    route = random.choice(ROUTES)
    now = datetime.now(timezone.utc)

    # Build segments with exact field names from PySpark schema
    segments = []
    for seg in route["segments"]:
        segments.append({
            "SEGMENT_ID": seg["id"],
            "_Metrics": [round(m + random.uniform(-1.0, 1.0), 1) for m in seg["metrics"]],
        })

    # Base fare: 50-500 THB range, with occasional invalid values for DQ testing
    if random.random() < 0.05:  # 5% chance of invalid fare
        base_fare = round(random.uniform(-10.0, 0.0), 2)
    else:
        base_fare = round(random.uniform(50.0, 500.0), 2)

    # Customer ID: positive long, with occasional invalid for DQ testing
    if random.random() < 0.03:  # 3% chance of invalid customer
        customer_id = random.choice([0, -1])
    else:
        customer_id = random.randint(10000, 99999)

    trip_id = f"TRIP-{uuid.uuid4().hex[:8].upper()}"
    transaction_id = f"TXN-{uuid.uuid4().hex[:8].upper()}"

    return {
        "driver_ID": random.choice(DRIVER_IDS),
        "customer_id": customer_id,
        "transactionRecord": {
            "transaction_id": transaction_id,
            "trip_info": {
                "trip_id": trip_id,
                "pricing": {
                    "base_fare": base_fare,
                    "route_data": {
                        "route_name": route["name"],
                        "segments": segments,
                    },
                },
                "createdAt": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            },
        },
        "COORDINATES": add_jitter(route["coords"]),
    }


def main():
    print(f"Starting trip data generator")
    print(f"  Kafka: {BOOTSTRAP_SERVERS}")
    print(f"  Topic: {TOPIC}")
    print(f"  Rate:  {EVENTS_PER_SECOND} events/sec")

    # Wait for Kafka to be ready
    connected = False
    while not connected:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                max_in_flight_requests_per_connection=1,
            )
            connected = True
            print("Connected to Kafka")
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

    count = 0
    sleep_time = 1.0 / EVENTS_PER_SECOND

    while True:
        try:
            trip = generate_trip()

            # Use trip_id as key so same trip always goes to same partition
            trip_id = trip["transactionRecord"]["trip_info"]["trip_id"]
            producer.send(
                TOPIC,
                key=trip_id,
                value=trip,
            )

            count += 1
            if count % 50 == 0:
                driver = trip["driver_ID"]
                route = trip["transactionRecord"]["trip_info"]["pricing"]["route_data"]["route_name"]
                print(f"Published {count} events | latest: {trip_id} | driver: {driver} | route: {route}")
                producer.flush()

            time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("Shutting down generator")
            producer.flush()
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
