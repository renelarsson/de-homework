import csv
import json
from kafka import KafkaProducer
from time import time

# Define the Kafka producer
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

topic_name = 'green-trips'

# Read the dataset and filter columns
with open('green_tripdata_2019-10.csv', 'r') as file:
    reader = csv.DictReader(file)
    rows = [
        {
            'lpep_pickup_datetime': row['lpep_pickup_datetime'],
            'lpep_dropoff_datetime': row['lpep_dropoff_datetime'],
            'PULocationID': row['PULocationID'],
            'DOLocationID': row['DOLocationID'],
            'passenger_count': row['passenger_count'],
            'trip_distance': row['trip_distance'],
            'tip_amount': row['tip_amount']
        }
        for row in reader
    ]

# Send data to the topic
t0 = time()
for message in rows:
    producer.send(topic_name, value=message)
producer.flush()
t1 = time()

print(f"Data sent successfully. Time taken: {t1 - t0} seconds")
