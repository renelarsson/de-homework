import json
from kafka import KafkaConsumer

TOPIC_NAME = "green-trips"
BOOTSTRAP_SERVERS = ["localhost:9092"]


def json_deserializer(message):
    return json.loads(message.decode("utf-8"))


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
    value_deserializer=json_deserializer,
)

count = 0

for record in consumer:
    trip = record.value
    trip_distance = trip.get("trip_distance")

    if trip_distance is not None and float(trip_distance) > 5.0:
        count += 1

print(count)