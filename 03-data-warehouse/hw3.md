# Module 3 Homework: Data Warehousing & BigQuery

In this homework we'll practice working with BigQuery and Google Cloud Storage.

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.

## Data

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data).

Parquet Files are available from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Loading the data

You can use the following scripts to load the data into your GCS bucket:

- Python script: [load_yellow_taxi_data.py](./load_yellow_taxi_data.py)
- Jupyter notebook with DLT: [DLT_upload_to_GCP.ipynb](./DLT_upload_to_GCP.ipynb)

You will need to generate a Service Account with GCS Admin privileges or be authenticated with the Google SDK, and update the bucket name in the script.

If you are using orchestration tools such as Kestra, Mage, Airflow, or Prefect, do not load the data into BigQuery using the orchestrator.

Make sure that all 6 files show in your GCS bucket before beginning (the 6 Yellow Taxi parquet files for Jan–Jun 2024).

Note: You will need to use the PARQUET option when creating an external table.


## BigQuery Setup

Create an external table using the Yellow Taxi Trip Records. 

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 

### BigQuery Setup Procedures

#### Using the console

1. Open BigQuery console

    - Go to the BigQuery console (https://console.cloud.google.com/bigquery).
    - Ensure your project is selected in the top-left corner.

2. Create an external table

    ```sql
    CREATE OR REPLACE EXTERNAL TABLE `<PROJECT_ID>.yellow_taxi.external_yellow_tripdata`
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://<your_bucket>/yellow_tripdata_2024-*.parquet']
    );
    ```

    - Replace placeholders with your project ID and bucket name.
    - Click Run to create the external table.

3. Create a regular table

    ```sql
    CREATE OR REPLACE TABLE `<PROJECT_ID>.yellow_taxi.yellow_tripdata` AS
    SELECT * FROM `<PROJECT_ID>.yellow_taxi.external_yellow_tripdata`;
    ```

    - This query copies the data from the external table into a regular BigQuery table.
    - Click Run to create the regular table.

4. Verify the tables

    - Expand your dataset (yellow_taxi) in the menu.
    - You should see two tables:
        - `external_yellow_tripdata` (external table)
        - `yellow_tripdata` (regular table)

#### Using the CLI

1. Log in and set the project:

    ```sh
    gcloud auth login 
    gcloud config set project <PROJECT_ID>
    ```

2. Create and verify the dataset:

    ```sh
    bq --location=<REGION> mk --dataset <PROJECT_ID>:yellow_taxi
    bq ls --datasets
    ```

3. Create an external table:

    ```sh
    bq query --use_legacy_sql=false \
    "CREATE OR REPLACE EXTERNAL TABLE `<PROJECT_ID>.yellow_taxi.external_yellow_tripdata` \
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://<your_bucket>/yellow_tripdata_2024-*.parquet']
    );"
    ```

4. Create a regular table:

    ```sh
    bq query --use_legacy_sql=false '
    CREATE OR REPLACE TABLE `<PROJECT_ID>.yellow_taxi.yellow_tripdata` AS
    SELECT * FROM `<PROJECT_ID>.yellow_taxi.external_yellow_tripdata`;
    '
    ```

5. List the tables in your dataset:

    ```sh
    bq ls --dataset_id=yellow_taxi
    ```

    - You should see both `external_yellow_tripdata` and `yellow_tripdata` in the output.


## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- **`20,332,093`**
- 85,431,289

#### Run in CLI:

```sh
bq query --use_legacy_sql=false 'SELECT COUNT(*) AS record_count FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`'

```
* Output:
```sql
+--------------+
| record_count |
+--------------+
|     20332093 |
+--------------+
```

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- **`0 MB for the External Table and 155.12 MB for the Materialized Table`**
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

1. Inspect the table:

```sh
bq query --use_legacy_sql=false '
SELECT * 
FROM `<PROJECT_ID>.yellow_taxi.external_yellow_tripdata`
LIMIT 10;
'
```
2. Count the number of unique PULocationID values in the dataset:

- External:

```sh
bq query --use_legacy_sql=false '
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_ids
FROM `<PROJECT_ID>.yellow_taxi.external_yellow_tripdata`;
'
```

- External output:
```sql
+-------------------------+
| distinct_pulocation_ids |
+-------------------------+
|                     262 |
+-------------------------+
```

- Regular:

```sh
bq query --use_legacy_sql=false '
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_ids
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`;
'
```

- Regular output:
```sql
+-------------------------+
| distinct_pulocation_ids |
+-------------------------+
|                     262 |
+-------------------------+
```

2. Get evaluation from the Google Cloud Console's BigQuery editor:

- External:

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_ids
FROM `<PROJECT_ID>.yellow_taxi.external_yellow_tripdata`;
```
`This query will process 0 B when run.`

- Regular:

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_ids
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`;
```
`This query will process 155.12 MB when run.`

**Answer**: `0 MB for the External Table and 155.12 MB for the Materialized Table`. The external table reads less data since it references data in GCS and may only scan the required column. The materialized table reads more data because it is fully stored in BigQuery, and the query scans the PULocationID column.


## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?
- **`BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed`**.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

1. Get evaluation from the Google Cloud Console's BigQuery editor:

- External:

```sql
SELECT PULocationID
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`;
```
`This query will process 155.12 MB when run.`

- Regular:

```sql
SELECT PULocationID, DOLocationID
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`;
```
`This query will process 310.24 MB when run.`

## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- **`8,333`**

### Run in CLI:

```sh
bq query --use_legacy_sql=false '
SELECT COUNT(*) AS zero_fare_trips
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`
WHERE fare_amount = 0;
'
```
```sql
+-----------------+
| zero_fare_trips |
+-----------------+
|            8333 |
+-----------------+
```

## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- **`Partition by tpep_dropoff_datetime and Cluster on VendorID`**
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

### Run in CLI:

```sh
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE `<PROJECT_ID>.yellow_taxi.optimized_yellow_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`;
'
```

**Answer**:
Partitioning is useful when queries frequently (or always) filter on that column, as it reduces the amount of data scanned. Clustering only scans the relevant/similar blocks of data, reducing the amount of data read and improving query performance.

## Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 

Choose the answer which most closely matches.

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- **`310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`**
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

1. Using the Non-Partitioned Table from the Google Cloud Console:
```sql
SELECT DISTINCT VendorID
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```
`This query will process 310.24 MB when run.`

2. Using the Partitioned Table from the Google Cloud Console:
```sql
SELECT DISTINCT VendorID
FROM `<PROJECT_ID>.yellow_taxi.optimized_yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```
`This query will process 26.84 MB when run.`

**Answer**: 
BigQuery scans the entire non-partitioned table to find rows matching the WHERE clause.
This results in a higher number of bytes processed. On the other hand, only scans the partitions in the partitioned table that match the tpep_dropoff_datetime filter. This significantly reduces the amount of data scanned, as only relevant partitions are read.

## Question 7. External table storage

Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- **`GCP Bucket`**
- Big Table

**Answer:** An external table in BigQuery references data stored outside of BigQuery, such as in a GCS bucket. The data is not ingested into BigQuery, but BigQuery queries the data directly from the external source.

## Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data:
- True
- **`False`**

**Answer**: Best practice is to use clustering when your queries frequently filter or sort by specific columns. On should avoid clustering if queries do not benefit from it, as it adds complexity.

## Question 9. Understanding table scans

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

* Using the Google Cloud Console:
```sql
SELECT COUNT(*) AS total_rows
FROM `<PROJECT_ID>.yellow_taxi.yellow_tripdata`;
```
`This query will process 0 B when run.`

**Answer**: The query processes 0 bytes because BigQuery retrieves the row count from the table's metadata, avoiding a full table scan. This is an optimization specific to COUNT(*) queries without filters or transformations.

#### Log out
```sh
gcloud auth revoke
```

## Submitting the solutions

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3


## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 3 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 3 - Data Warehousing with BigQuery. Learned how to:

✅ Create external tables from GCS bucket data
✅ Build materialized tables in BigQuery
✅ Partition and cluster tables for performance
✅ Understand columnar storage and query optimization
✅ Analyze NYC taxi data at scale

Working with 20M+ records and learning how partitioning reduces query costs!

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
📊 Module 3 of Data Engineering Zoomcamp done!

- BigQuery & GCS
- External vs materialized tables
- Partitioning & clustering
- Query optimization

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```
