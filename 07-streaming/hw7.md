# [DRAFT] Homework

In this homework, we're going to learn about streaming with PyFlink.

Instead of Kafka, we will use Red Panda, which is a drop-in
replacement for Kafka. It implements the same interface, 
so we can use the Kafka library for Python for communicating
with it, as well as use the Kafka connector in PyFlink.

For this homework we will be using the Taxi data:
- Green 2019-10 data from [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz)


## Setup

We need:

- Red Panda (*R: Service that acts as a Kafka-compatible message broker. It manages topics, partitions, and message delivery between producers and consumers.*)
- Flink Job Manager
- Flink Task Manager
- Postgres

It's the same setup as in the [pyflink module](../../../07-streaming/pyflink/), so go there and start docker-compose:

```bash
cd ../../../07-streaming/pyflink/
docker-compose up
```

(Add `-d` if you want to run in detached mode)

Visit http://localhost:8081 to see the Flink Job Manager

Connect to Postgres with pgcli, pg-admin, [DBeaver](https://dbeaver.io/) or any other tool.

The connection credentials are:

- Username `postgres`
- Password `postgres`
- Database `postgres`
- Host `localhost`
- Port `5432`

With pgcli, you'll need to run this to connect:

```bash
pgcli -h localhost -p 5432 -u postgres -d postgres
```

Run these query to create the Postgres landing zone for the first events and windows:

```sql 
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER 
);
```


## Question 1: Redpanda version

Now let's find out the version of redpandas. 

For that, check the output of the command `rpk help` _inside the container_. The name of the container is `redpanda-1`. (*R: Redpanda Keeper, a CLI tool for managing and interacting with Redpanda clusters*)

Find out what you need to execute based on the `help` output.

What's the version, based on the output of the command you executed? (copy the entire version)

### Procedure 1

**1. Create docker-compose:**

```sh
mkdir pyflink/docker-compose.yml
cd pyflink/
nano docker-compose.yml
```

- Red Panda services in docker-compose.yml:
```yml
services:
  redpanda:
    image: redpandadata/redpanda:v25.3.9
    container_name: redpanda-1
    command:
      - redpanda
      - start
      - --smp
      - '1'
      - --reserve-memory
      - 0M
      - --overprovisioned
      - --node-id
      - '1'
      - --kafka-addr
      - PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
      - --advertise-kafka-addr
      - PLAINTEXT://redpanda:29092,OUTSIDE://localhost:9092
      - --pandaproxy-addr
      - PLAINTEXT://0.0.0.0:28082,OUTSIDE://0.0.0.0:8082
      - --advertise-pandaproxy-addr
      - PLAINTEXT://redpanda:28082,OUTSIDE://localhost:8082
      - --rpc-addr
      - 0.0.0.0:33145
      - --advertise-rpc-addr
      - redpanda:33145
    ports:
      - 8082:8082
      - 9092:9092
      - 28082:28082
      - 29092:29092
```

**2. Start Redpanda container (creates redpanda-1):**
```sh
docker compose up -d
```

**3. Check rpk help inside the container:**
```sh
docker exec -it redpanda-1 rpk help
```

- the help output provides a version command:

```version     Prints the current rpk and Redpanda version```

**4. Run the version command:**
```sh
docker exec -it redpanda-1 rpk version
```

**Answer 1: v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2**


## Question 2. Creating a topic

Before we can send data to the redpanda server, we
need to create a topic (*R: synonym for stream*). We do it also with the `rpk`
command we used previously for figuring out the version of 
redpandas.

Read the output of `help` and based on it, create a topic with name `green-trips` 

What's the output of the command for creating a topic? Include the entire output in your answer.

- **Command to create the topic/stream green-trips:**

```sh
docker exec -it redpanda-1 rpk topic create green-trips
```
```
TOPIC        STATUS
green-trips  OK
```

**Answer:TOPIC=green-trips, STATUS=OK**


## Question 3. Connecting to the Kafka server

We need to make sure we can connect to the server, so
later we can send some data to its topics

First, let's install the kafka connector (up to you if you
want to have a separate virtual environment for that)

```bash
pip install kafka-python
```

You can start a jupyter notebook in your solution folder or
create a script

Let's try to connect to our server:

```python
import json

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

Provided that you can connect to the server, what's the output
of the last command?

### Procedure 3

**1. Install the Kafka Python library:**
```sh
pip install kafka-python
```

**2. Test the connection:**
- Create above Python script `kafka_connection_test.py`:
- Run the script:
```sh
python kafka_connection_test.py
```

- The producer.bootstrap_connected() method will return True if the connection is successful.

**Answer: True**


## Question 4: Sending the Trip Data

Now we need to send the data to the `green-trips` topic

Read the data, and keep only these columns:

* `'lpep_pickup_datetime',`
* `'lpep_dropoff_datetime',`
* `'PULocationID',`
* `'DOLocationID',`
* `'passenger_count',`
* `'trip_distance',`
* `'tip_amount'`

Now send all the data using this code:

```python
producer.send(topic_name, value=message)
```

For each row (`message`) in the dataset. In this case, `message`
is a dictionary.

After sending all the messages, flush the data (*R: Step to ensure reliable and complete delivery of messages to the broker*):

```python
producer.flush()
```

Use `from time import time` to see the total time 

```python
from time import time

t0 = time()

# ... your code

t1 = time()
took = t1 - t0
```

How much time did it take to send the entire dataset and flush? 

### Procedure 4

**1. Download the dataset:**
```sh
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
gunzip green_tripdata_2019-10.csv.gz
```

**2. Filter the required columns:**
- Create a Python script `send_trip_data.py` with above required columns:
- Run the script:
```sh
python send_trip_data.py
```
- **Script flow:**

**1. Dataset Preparation:**
The Green Taxi 2019-10 dataset was downloaded and uncompressed.
The Python script read the dataset and filtered the required columns (lpep_pickup_datetime, lpep_dropoff_datetime, etc.).

**2. Kafka Producer Setup:**
* A Kafka producer was created using the kafka-python library.
* The producer connected to the Redpanda broker running on localhost:9092.

**3. Data Sending:**
* Each row of the dataset was converted into a JSON message.
* The producer sent these messages to the green-trips topic in the Redpanda broker.
* The broker stores the message in the topic's partition(s) for consumers to read.

**4. Flushing:**
* After all messages were sent, the producer.flush() method was called to ensure all messages were delivered to the broker. Without flushing, some messages might remain in the buffer and not reach the broker immediately.
    * A **consumer** is a separate application that subscribes to the topic (green-trips) and reads messages from it.
    * The producer sends data to the topic, and the consumer retrieves it for processing.

5. Timing:
* The script measured the total time taken to send and flush the dataset.

**Answer: 48.96334648132324 seconds**


## Question 5: Build a Sessionization Window (2 points)

Now we have the data in the Kafka stream. It's time to process it.

* Copy `aggregation_job.py` and rename it to `session_job.py`
* Have it read from `green-trips` fixing the schema
* Use a [session window](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/) with a gap of 5 minutes
* Use `lpep_dropoff_datetime` time as your watermark with a 5 second tolerance
* Which pickup and drop off locations have the longest unbroken streak of taxi trips?

### Procedure 5


**1. Install PyFlink:**
```sh
pip install apache-flink
python -c "import pyflink; print('PyFlink installed successfully')"
```

**2. Copy and Rename the Job File:**
```sh
cp aggregation_job.py session_job.py
```

**3. Modify and run session_job.py with above updates**
```sh
python session_job.py
```

**Answer:**


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6
- Deadline: See the website


## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 6 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 6 - Streaming with PyFlink. Learned how to:

✅ Set up Redpanda as a Kafka replacement
✅ Build streaming data pipelines
✅ Create topics and produce/consume messages
✅ Implement sessionization windows
✅ Process real-time taxi trip data

Streaming data in real-time - the future of data engineering!

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
🌊 Module 6 of Data Engineering Zoomcamp done!

- Streaming with PyFlink
- Redpanda & Kafka concepts
- Sessionization windows
- Real-time data processing

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```
