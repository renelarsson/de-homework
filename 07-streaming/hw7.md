# Homework

In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same
protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

- [green_tripdata_2025-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet)

## Setup

We'll use the same infrastructure from the [workshop](../../../07-streaming/workshop/).

Follow the setup instructions: build the Docker image, start the services:

```bash
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

This gives us:

- Redpanda (Kafka-compatible broker) on `localhost:9092` (*R: The message broker manages topics, partitions, and message delivery between producers and consumers.*)
- Flink Job Manager at http://localhost:8081
- Flink Task Manager
- PostgreSQL on `localhost:5432` (user: `postgres`, password: `postgres`)

If you previously ran the workshop and have old containers/volumes,
do a clean start:

```bash
docker compose down -v
docker compose build
docker compose up -d
```

Note: the container names (like `workshop-redpanda-1`) assume the
directory is called `workshop`. If you renamed it, adjust accordingly.


## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

### Procedure 1

**1. Confirm the infrastructure files are present:**

```sh
cd 07-streaming/workshop/
ls
```

- You should at least see:

```Dockerfile.flink docker-compose.yml  flink-config.yaml pyproject.flink.toml  src```

**2. Clean out any old containers, networks, and Postgres volumes:**

```sh
docker compose down -v --remove-orphans
```

**3. Build the Flink image with services and start the stack:**

```sh
docker compose build
docker compose up -d
```

- Running `docker compose ps` should show services for Redpanda, Postgres, JobManager, and TaskManager.

**4. Create a host-side Python environment for the producer and consumer scripts:**

This host environment is separate from the Flink container environment.

```sh
# Creates a host Python project for local scripts
uv init -p 3.12
uv add kafka-python pandas pyarrow
source .venv/bin/activate
```

- .venv/, pyproject.toml, main.py, and uv.lock should appear in the workshop/ folder.

**5. Run the Redpanda version command:**

```sh
# 'docker exec -it' runs the command inside the Redpanda container
docker exec -it workshop-redpanda-1 rpk version
```

**Answer 1: v25.3.9**


## Question 2. Sending data to Redpanda

Create a topic (*R: synonym for stream*) called `green-trips`:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic.
You'll need to handle the datetime columns - convert them to strings
before serializing to JSON.

Measure the time it takes to send the entire dataset and flush (*R: Step to ensure reliable and complete delivery of messages to the broker*):

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

- **10 seconds**
- 60 seconds
- 120 seconds
- 300 seconds

### Procedure 2 

**1. Download the dataset and create the topic:**

```sh
wget -O green_tripdata_2025-10.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet

# Creates the Kafka/Redpanda topic the producer will write to
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

**2. Create a producer script:**
- Create a Python script `send_trip_data.py` with above required columns:
- Run the producer script:
```sh
python send_trip_data.py
```

### Script flow:

- **pd.read_parquet(...)** reads the parquet file and keeps only the required columns.

- **A Kafka producer** is created and connects to the Redpanda broker running on localhost:9092.

- **producer.send(...)** publishes each trip as a JSON message to the green-trips topic. 

- **producer.flush()** ensures all buffered messages are actually delivered to Redpanda before timing stops. 

**Answer 2: took 6.74 seconds**


## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- **8506**
- 9506

### Procedure 3

**Create a consumer script:**
- Create a Python script `count_long_trips.py` and run it:
```sh
python count_long_trips.py
```

### Script flow:

- **auto_offset_reset="earliest"** makes the consumer start from the beginning of the topic.

- **enable_auto_commit=False** avoids persisting offsets for this counting script.

- **consumer_timeout_ms=5000** makes the script stop after it finishes reading the available messages.

- **value_deserializer=...** converts the JSON bytes back into Python dictionaries.

**Answer 3: 8506**


## Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, you'll adapt the workshop code to work with
the green taxi data. The key differences from the workshop:

- Topic name: `green-trips` (instead of `rides`)
- Datetime columns use `lpep_` prefix (instead of `tpep_`)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables
for your results.

Important notes for the Flink jobs:

- Place your job files in `workshop/src/job/` - this directory is
  mounted into the Flink containers at `/opt/src/job/`
- Submit jobs with:
  `docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/your_job.py`
- The `green-trips` topic has 1 partition, so set parallelism to 1
  in your Flink jobs (`env.set_parallelism(1)`). With higher parallelism,
  idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute
  or two until results appear in PostgreSQL, then query the results.
  You can cancel the job from the Flink UI at http://localhost:8081
- If you sent data to the topic multiple times, delete and recreate
  the topic to avoid duplicates:
  `docker exec -it workshop-redpanda-1 rpk topic delete green-trips`


## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute
tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns:
`window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- **74**
- 75
- 166

### Procedure 4
Goal: count trips per PULocationID in 5 minute tumbling windows and find which location had the highest single window count.

**1. Create the Postgres results table:**

```sh
docker exec -i workshop-postgres-1 psql -U postgres -d postgres <<'SQL'
DROP TABLE IF EXISTS green_trips_pickup_5min;
CREATE TABLE green_trips_pickup_5min (
    window_start TIMESTAMP,
    pulocationid INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, pulocationid)
);
SQL
```

**2. Create the Flink job file inside workshop/src/job/:**
- Create a Python script `aggregation_job.py` that

1. reads from green-trips.
2. converts the pickup timestamp string to a Flink timestamp with TO_TIMESTAMP(...).
3. defines a 5-second watermark tolerance.
4. uses a 5-minute tumbling window.
5. writes the aggregated result to Postgres.

- Submit the job and verify it's running:
```sh
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/aggregation_job.py
docker exec -it workshop-jobmanager-1 flink list
```

Jobs in workshop/src/job/ are mounted at /opt/src/job/ inside the Flink containers and
`flink run -py` is the correct way to submit the PyFlink job to the cluster. 

**3. Query the results:**

- **NB:** Wait a bit before querying the results:
```sh
docker exec -i workshop-postgres-1 psql -U postgres -d postgres -c "
SELECT PULocationID, num_trips
FROM green_trips_pickup_5min
ORDER BY num_trips DESC, PULocationID ASC
LIMIT 3;
"
```

**Answer 4: 74**


## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time
with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID`
with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- **81**

### Procedure 5
Goal: create a Flink session-window job and find the PULocationID session with the most trips.

**1. Stop the Q4 job before starting Q5:**

```sh
docker exec -it workshop-jobmanager-1 flink cancel fe9f2be489ed04db16b6ec1ebdab4746
```

**2. Create the Postgres sink table for session results:**

This stores one row per session per PULocationID.

```sh
docker exec -i workshop-postgres-1 psql -U postgres -d postgres <<'SQL'
DROP TABLE IF EXISTS green_trips_sessions;
CREATE TABLE green_trips_sessions (
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    pulocationid INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (session_start, session_end, pulocationid)
);
SQL
```

**3. Update the contents of session_job.py:**
- Update `session_job.py` such that

1. event time uses lpep_pickup_datetime.
2. watermark is 5 seconds.
3. session gap is 5 minutes.
4. it uses a 5-minute tumbling window.
5. it groups only by PULocationID, which matches the updated homework wording.

- Submit the job and verify it's running:
```sh
docker exec -it workshop-jobmanager-1 flink run -d -py /opt/src/job/session_job.py
docker exec -it workshop-jobmanager-1 flink list
```

**4. Query the results:**

- **NB:** Wait a bit before querying the results:
```sh
docker exec -i workshop-postgres-1 psql -U postgres -d postgres -c "
SELECT pulocationid, num_trips, session_start, session_end
FROM green_trips_sessions
ORDER BY num_trips DESC, pulocationid ASC
LIMIT 10;
"
```

**Answer 5: 81**


## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the
total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- **2025-10-16 18:00:00**
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

### Procedure 6
Goal: compute hourly total tip_amount across all locations and find the hour with the maximum sum. 

**1. Stop the Q5 job first:**
```sh
docker exec -it workshop-jobmanager-1 flink cancel 9f80389165f35a705be317e44ef0685a
```

**2. Create the Postgres sink table:**

This table stores one row per 1-hour window with the total tips for that hour.

```sh
docker exec -i workshop-postgres-1 psql -U postgres -d postgres <<'SQL'
DROP TABLE IF EXISTS green_trips_tips_1h;
CREATE TABLE green_trips_tips_1h (
    window_start TIMESTAMP,
    total_tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
SQL
```

**3. Create the Flink job file inside workshop/src/job/:**
- Create a Python script `tips_job.py` that

1. uses lpep_pickup_datetime as event time
2. uses a 1-hour tumbling window
3. sums tip_amount across all locations
4. writes one row per hour to Postgres

- Submit the job and verify it's running:
```sh
docker exec -it workshop-jobmanager-1 flink run -d -py /opt/src/job/tips_job.py
docker exec -it workshop-jobmanager-1 flink list
```

**4. Query the results:**

- **NB:** Wait a bit before querying the results:

```sh
docker exec -i workshop-postgres-1 psql -U postgres -d postgres -c "
SELECT window_start, total_tip_amount
FROM green_trips_tips_1h
ORDER BY total_tip_amount DESC, window_start ASC
LIMIT 10;
"
```

**Answer 6: 2025-10-16 18:00:00**


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7


## Learning in public

We encourage everyone to share what they learned.
Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

## Example post for LinkedIn

```
Week 7 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 7 - Streaming with PyFlink. Learned how to:

- Set up Redpanda as a Kafka replacement
- Build Kafka producers and consumers in Python
- Create tumbling and session windows in Flink
- Analyze real-time taxi trip data with stream processing

Here's my homework solution: <LINK>

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

## Example post for Twitter/X

```
Module 7 of Data Engineering Zoomcamp done!

- Kafka producers and consumers
- PyFlink tumbling and session windows
- Real-time taxi data analysis
- Redpanda as Kafka replacement

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```