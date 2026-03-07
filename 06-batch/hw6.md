# Module 6 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/pyspark.md)

**a) Download parquet files:**

```sh
cd 06-batch/
mkdir data/ && cd data/
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

**b) Install Spark:**

*The linux.md file explicitly says Spark 4.x requires Java 17/21 and that installing pyspark is enough because it bundles Spark.*

```sh
# Install Java 21 (Spark 4.x-compatible) 
sudo apt-get update && sudo apt-get install -y openjdk-21-jdk

# Set Java for the shell (Codespace uses Java 25, but Spark 4.x expects Java 17/21)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

# Install PySpark into the user site-packages 
python3 -m pip install --user pyspark

# Verify installation
python3 -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').getOrCreate(); print(s.version); s.stop()"
```

**c) Use SparkSession (alternative for Codespaces and used below):**

*Pyspark ships with Spark, so creating SparkSession is enough (run before each session). spark.version returns the current session's Spark version.*

```sh
# Use uv to run a one-off script with PySpark (not permanently installed)
cd 06-batch/
uv run --with pyspark python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('hw6').getOrCreate()
print(spark.version)
spark.stop()
"
```
**Answer: 4.1.1**

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- **25MB**
- 75MB
- 100MB

### Using the command line 

**a) Start a PySpark REPL with forced Java 21:**

```sh
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

uv run --with pyspark pyspark
```

**b) In the PySpark prompt, run these steps:**

```sh
# 1) Create SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("hw6-q2") \
    .master("local[*]") \
    .getOrCreate()

# 2) Read parquet
df = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")
df.columns

# 3) Repartition to 4
df4 = df.repartition(4)

# 4) Write parquet (creates a folder with 4 .parquet part files)
df4.write.parquet("out_yellow_2025_11_p4", mode="overwrite")
```

**c) Stop Spark and exit PySpark**:
```sh
spark.stop()
exit()

**d) Compute the average parquet part-file size:**

```sh
# List the produced parquet files and their sizes in pwd
find out_yellow_2025_11_p4 -maxdepth 1 -type f -name '*.parquet' -printf '%f\t%s\n' | sort

# Compute the average size in MB:
## glob.glob('out_yellow_2025_11_p4/*.parquet') collects the 4 parquet part files
## os.path.getsize(f) gets each file’s size in bytes
## avg = sum(sizes)/len(sizes) averages bytes per file
## avg/1_000_000 converts bytes to decimal MB
python3 -c "import glob,os; fs=glob.glob('out_yellow_2025_11_p4/*.parquet'); sizes=[os.path.getsize(f) for f in fs]; avg=sum(sizes)/len(sizes); print('files',len(fs)); print('avg_MB',avg/1_000_000)"
```

### Views in the UI

Open the URL:

* **Jobs tab**: see the job(s) created by your actions (read.parquet, repartition, write.parquet trigger jobs/stages).
* **Stages tab**: details per stage (tasks, shuffle, time).
* **SQL / DataFrame tab (if enabled)**: query plans and execution details for SQL/DataFrame operations.
* **Storage tab**: will show cached/persisted datasets (you didn’t cache anything, so it may be empty).
* **Environment tab**: Spark properties, classpath, etc.
* **Executors tab**: executor memory/CPU (local mode will show local executor).

**Answer: 26.5617 MB**


## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- **162,604**
- 225,768

**1. Start PySpark**:
```sh
uv run --with pyspark pyspark
```
**2. Create a SparkSession in the PySpark prompt**:
```sh
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("hw6-q3") \
    .master("local[*]") \
    .getOrCreate()
```
**3. Read the parquet**:
```sh
# This confirms the pickup timestamp column name (tpep_pickup_datetime for Yellow taxi)
df = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")
df.columns
```
**4: Filter pickups on 2025-11-15 and count**:
```sh
from pyspark.sql import functions as F

# to_date(tpep_pickup_datetime) extracts the date portion (ignores time)
# Filter keeps only rows with pickup date exactly Nov 15
df_15 = df.filter(F.to_date(F.col("tpep_pickup_datetime")) == F.lit("2025-11-15"))
df_15.count()
```
**5. Stop Spark and exit PySpark**:
```sh
spark.stop()
exit()
```

**Answer: 162604**

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- **90.6**
- 134.5

**1. Create a SparkSession**:
```sh
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("hw6-q4") \
    .master("local[*]") \
    .getOrCreate()
```
**2. Read the parquet**:
```sh
# Confirm column names tpep_pickup_datetime and tpep_dropoff_datetime for Yellow data
df = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")
df.columns
```
**3: Compute trip duration hours and get the maximum**:
```sh
from pyspark.sql import functions as F

df_dur = df.select(
    F.col("tpep_pickup_datetime").alias("pickup"),
    F.col("tpep_dropoff_datetime").alias("dropoff")
).filter(
    # Remove bad/negative/zero durations
    F.col("pickup").isNotNull() &
    F.col("dropoff").isNotNull() &
    (F.col("dropoff") > F.col("pickup"))
).withColumn(
    "duration_hours",
    # Convert timestamps for reliable Spark 4 subtraction
    # 1. cast("timestamp") treats dropoff as a normal timestamp
    # 2. cast("long") treats the timestamp as a plain integer count of seconds
    (F.col("dropoff").cast("timestamp").cast("long")
    - F.col("pickup").cast("timestamp").cast("long")) / F.lit(3600.0)
)

df_dur.agg(F.max("duration_hours").alias("max_duration_hours")).show(truncate=False)
```

**Answer: 90.6467**

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- **4040**
- 8080

The local port is returned by the ```spark.sparkContext.uiWebUrl``` command:


`'http://f381029c-0317-48cc-83d0-30891c792435.internal.cloudapp.net:4040'`

**Answer: 4040**


## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- **Governor's Island/Ellis Island/Liberty Island**
- **Arden Heights**
- Rikers Island
- Jamaica Bay

If multiple answers are correct, select any

**1. Download the zone lookup CSV**:
```sh
cd data/
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```
**2. Create a SparkSession**:
```sh
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("hw6-q6") \
    .master("local[*]") \
    .getOrCreate()
```
**3. Find the least frequent**:
- We need to join the November 2025 Yellow trips with the zone lookup, count pickups per zone, and find the least frequent.
```sh
# a) Read trips and zones (lookup table):
trips = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")
zones = spark.read.option("header", True).csv("data/taxi_zone_lookup.csv")

# b) Keep only pickup location ID from trips:
trips = trips.select("PULocationID")

# c) Join trips with zones on pickup location (add a Zone from the lookup column to each trip row):
joined = trips.join(
    zones,
    trips.PULocationID == zones.LocationID,
    "left" # Keep all trips even if some PULocationID has no matching zone 
)

# d) Group by zone and count pickups, then sort ascending:
by_zone = (
    joined.groupBy("Zone") # one row per zone
    .agg(F.count("*").alias("pickup_count")) # count pickups 
    .orderBy(F.col("pickup_count").asc(), F.col("Zone").asc()) # sort ascending + alphabetical
)

by_zone.show(10, truncate=False) # display the 10 least-frequent zones

# e) Clean shutdown:
spark.stop()
exit()
```

**Answer: Governor's Island/Ellis Island/Liberty Island and Arden Heights**

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6
- Deadline: See the website


## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 6 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 6 - Batch Processing with Spark. Learned how to:

✅ Set up PySpark and create Spark sessions
✅ Read and process Parquet files at scale
✅ Repartition data for optimal performance
✅ Analyze millions of taxi trips with DataFrames
✅ Use Spark UI for monitoring jobs

Processing 4M+ taxi trips with Spark - distributed computing is powerful! 💪

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
⚡ Module 6 of Data Engineering Zoomcamp done!

- Batch processing with Spark 🔥
- PySpark & DataFrames
- Parquet file optimization
- Spark UI on port 4040

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```