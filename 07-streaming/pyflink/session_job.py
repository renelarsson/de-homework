from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import Time, SessionWindow
from pyflink.table import StreamTableEnvironment
from pyflink.table.window import Session

# Set up the environment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# Define the source (Kafka topic)
table_env.execute_sql("""
    CREATE TABLE green_trips (
        lpep_dropoff_datetime TIMESTAMP(3),
        PULocationID INT,
        DOLocationID INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
""")

# Define the session window
result = table_env.sql_query("""
    SELECT
        PULocationID,
        DOLocationID,
        SESSION_START(lpep_dropoff_datetime, INTERVAL '5' MINUTE) AS session_start,
        SESSION_END(lpep_dropoff_datetime, INTERVAL '5' MINUTE) AS session_end,
        COUNT(*) AS trip_count
    FROM green_trips
    GROUP BY SESSION(lpep_dropoff_datetime, INTERVAL '5' MINUTE), PULocationID, DOLocationID
""")

# Define the sink (console or Postgres)
table_env.execute_sql("""
    CREATE TABLE session_results (
        PULocationID INT,
        DOLocationID INT,
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        trip_count BIGINT
    ) WITH (
        'connector' = 'print'
    )
""")

# Insert the results into the sink
result.execute_insert("session_results")
