## Module 2 Homework

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format, please include these directly in the README file of your repository.

> In case you don't get one option exactly, select the closest one 

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

To get a `wget`-able link, use this prefix (note that the link itself gives 404):

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`

### Assignment

So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021.

![homework datasets](../../../02-workflow-orchestration/images/homework.png)

As a hint, Kestra makes that process really easy:
1. You can leverage the backfill functionality in the [scheduled flow](../../../02-workflow-orchestration/flows/05_gcp_taxi_scheduled.yaml) to backfill the data for the year 2021. Just make sure to select the time period for which data exists i.e. from `2021-01-01` to `2021-07-31`. Also, make sure to do the same for both `yellow` and `green` taxi data (select the right service in the `taxi` input).
2. Alternatively, run the flow manually for each of the seven months of 2021 for both `yellow` and `green` taxi data. Challenge for you: find out how to loop over the combination of Year-Month and `taxi`-type using `ForEach` task which triggers the flow for each combination using a `Subflow` task.

### Quiz Questions

Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.

#### **Question 1:** 
Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- **`128.3 MiB`**
- 134.5 MiB
- 364.7 MiB
- 692.6 MiB

#### **Procedure:**

1. Start services:
```sh
# We use official images (postgres, kestra) directly in Docker Compose (Dockerfile not needed)
cd 02-workflow-orchestration
docker compose up -d
```

2. Open Kestra at http://localhost:8080 and log in with `admin@kestra.io`/`Admin1234` (from the compose file).

3. Import the homework flow via the API (or paste the YAML in the UI’s Flow editor):

```sh
curl -X POST -u 'admin@kestra.io:Admin1234' \
  http://localhost:8080/api/v1/flows/import \
  -F fileUpload=@hw2_kestra_1.yaml
```

This imports the homework-specific flow (same logic as 04_postgres_taxi, but with purge disabled and allowCustomValue enabled for `year`).
You don’t need to run wget - the extract task in this flow already does the download and gunzip (see the YAML file).

4. In Kestra UI, open the flow hw2_kestra_1 and run an execution by selecting 'taxi: yellow', 'year: 2020', 'month: 12'

5. Open the execution details to get the file size (extract > Outputs > yellow_tripdata_2020-12.csv). 


#### **Question 2:** 
What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- **`green_tripdata_2020-04.csv`**
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

#### **Answer:** 
- Run the execution via Kestra UI as in Q1.

#### **Question 3:**
How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
- **`13,537.299`**
- 24,648,499
- 18,324,219
- 29,430,127

#### **Procedure:** 

1. Run months 1-12 via Kestra UI.

3. Query the total in Postgres (filter rows by the timestamp column, not by filename):

```sh
docker compose exec pgdatabase psql -U root -d ny_taxi -c \
"SELECT COUNT(*) AS total_rows_2020 FROM public.yellow_tripdata WHERE tpep_pickup_datetime >= '2020-01-01' AND tpep_pickup_datetime < '2021-01-01';"
```

- Result: **`12951943`**

#### **Question 4:**
How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
- 5,327,301
- 936,199
- **`1,734,051`**
- 1,342,034

#### **Procedure:** 

1. Run months via Kestra UI.

2. Query the total in Postgres:

```sh
docker compose exec pgdatabase psql -U root -d ny_taxi -c \
"SELECT COUNT(*) AS total_rows_2020 FROM public.green_tripdata WHERE lpep_pickup_datetime >= '2020-01-01' AND lpep_pickup_datetime < '2021-01-01';"
```

- Result: **`1733998`**

#### **Question 5:**
How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- **`1,925,152`**
- 2,561,031

#### **Procedure A:** 

1. Extend the existing flows to include 2021 (add `allowCustomValue` to the YAML file):
```yml
- id: year
  type: SELECT
  displayName: Select year
  values: ["2019", "2020"]
  defaults: "2019"
  allowCustomValue: true
  ```
2. Re-import the flow into Kestra:
```sh
cd 02-workflow-orchestration

curl -X POST -u 'admin@kestra.io:Admin1234' \
  http://localhost:8080/api/v1/flows/import \
    -F fileUpload=@hw2_kestra_1.yaml
```

3. Run March 2021 via Kestra UI (type in 2021 manually).

4. Query the total in Postgres:

```sh
docker compose exec pgdatabase psql -U root -d ny_taxi -c \
"SELECT COUNT(*) AS march_rows_2021 FROM public.yellow_tripdata WHERE tpep_pickup_datetime >= '2021-03-01' AND tpep_pickup_datetime < '2021-04-01';"
```

- Result: **`1925119`**

#### **Procedure B:**

1. Import the alternate flow into Kestra:
```sh
cd 02-workflow-orchestration

curl -X POST -u 'admin@kestra.io:Admin1234' \
  http://localhost:8080/api/v1/flows/import \
  -F fileUpload=@hw2_kestra_2.yaml
```
2. Open Flows → zoomcamp / hw2_kestra_2. 
3. Configure by clicking the Backfill button under the triggers tab.
4. Query the total in Postgres (same as above)

- Result: **`1925130`**

#### **Question 6:**
How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- **Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  

#### **Answer:** 
In Kestra the field is called `timezone` and set to `America/New_York` (see https://kestra.io/docs/workflow-components/triggers/schedule-trigger).

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw2
* Check the link above to see the due date

## Solution

Will be added after the due date