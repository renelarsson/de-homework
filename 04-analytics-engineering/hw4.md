# Module 4 Homework: Analytics Engineering with dbt

In this homework, we'll use the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. Set up your dbt project following the [setup guide](../../../04-analytics-engineering/setup/)
2. Load the Green and Yellow taxi data for 2019-2020 into your warehouse
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

### Which setup path did we use?

The course presents two “official” stacks:

- **Cloud setup:** BigQuery (warehouse) + dbt Cloud Studio (execution environment)
- **Local setup:** DuckDB (warehouse) + dbt Core (execution environment)

For this homework, we used the **Cloud setup**:

- Warehouse: **BigQuery**
- Where `dbt deps` / `dbt build` were run: **dbt Cloud Studio (web UI)**
- Where raw loads + ad-hoc checks were run: **Codespace terminal using `bq` (BigQuery CLI)**

There is also a common *third* variant that can look like a “blend”:

- **BigQuery + dbt Core (terminal)**

This is not DuckDB/local-warehouse, and it’s not dbt Cloud Studio either — it’s simply running dbt from your terminal (dbt Core) while still using BigQuery as the warehouse. It’s a valid way to run the same project, but it’s optional.


### BigQuery Setup Procedures (CLI)

1) Authenticate and set your project:

```sh
gcloud auth login
gcloud config set project <PROJECT_ID>
```

2) Create (or verify) the datasets.

- `<RAW_DATASET>` is the *raw* dataset (yellow/green/fhv live here).
- `dbt_<your_name>` is where dbt models will be materialized.

```sh
# IMPORTANT: keep the dataset location consistent (e.g. europe-north2).
bq --location=<REGION> mk --dataset <PROJECT_ID>:<RAW_DATASET>

# This is where dbt will write models (tables/views). Use your own name.
# Example used in this repo: <DBT_DATASET>
bq --location=<REGION> mk --dataset <PROJECT_ID>:dbt_<your_name>

bq ls --datasets
```

3) Load raw data tables into `nytaxi`.

3) Load raw data tables into `<RAW_DATASET>`.

- `<RAW_DATASET>.yellow_tripdata` (2019–2020)
- `<RAW_DATASET>.green_tripdata` (2019–2020)

There are multiple ways to do this. A practical CLI flow (similar to Module 3) is:

1) Download the Parquet files (or otherwise obtain them).
2) Upload them to a GCS bucket.
3) Create **external** tables in BigQuery pointing at the Parquet files.
4) Create **native** BigQuery tables from the external tables.

Below is an example (replace placeholders):

```sh
# Set these once
export PROJECT_ID=<PROJECT_ID>
export REGION=<REGION>
export BUCKET=<YOUR_BUCKET_NAME>
export RAW_DATASET=<RAW_DATASET>

# (A) Upload parquet files to GCS
gsutil -m cp yellow_tripdata_2019-*.parquet gs://$BUCKET/nyc_tlc/yellow/
gsutil -m cp yellow_tripdata_2020-*.parquet gs://$BUCKET/nyc_tlc/yellow/
gsutil -m cp green_tripdata_2019-*.parquet  gs://$BUCKET/nyc_tlc/green/
gsutil -m cp green_tripdata_2020-*.parquet  gs://$BUCKET/nyc_tlc/green/

# (B) External tables
bq query --location=$REGION --use_legacy_sql=false \
"create or replace external table \`${PROJECT_ID}.${RAW_DATASET}.external_yellow_tripdata\`
 options (format='PARQUET', uris=['gs://${BUCKET}/nyc_tlc/yellow/yellow_tripdata_2019-*.parquet','gs://${BUCKET}/nyc_tlc/yellow/yellow_tripdata_2020-*.parquet']);"

bq query --location=$REGION --use_legacy_sql=false \
"create or replace external table \`${PROJECT_ID}.${RAW_DATASET}.external_green_tripdata\`
 options (format='PARQUET', uris=['gs://${BUCKET}/nyc_tlc/green/green_tripdata_2019-*.parquet','gs://${BUCKET}/nyc_tlc/green/green_tripdata_2020-*.parquet']);"

# (C) Native (materialized) tables that dbt will read from
bq query --location=$REGION --use_legacy_sql=false \
"create or replace table \`${PROJECT_ID}.${RAW_DATASET}.yellow_tripdata\` as
 select * from \`${PROJECT_ID}.${RAW_DATASET}.external_yellow_tripdata\`;"

bq query --location=$REGION --use_legacy_sql=false \
"create or replace table \`${PROJECT_ID}.${RAW_DATASET}.green_tripdata\` as
 select * from \`${PROJECT_ID}.${RAW_DATASET}.external_green_tripdata\`;"
```


4) Sanity-check that the raw tables exist:

```sh
bq ls nytaxi

(Replace `nytaxi` with `<RAW_DATASET>` if you used a different raw dataset name.)
```

### Getting the dbt project code

```sh
git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git
cd data-engineering-zoomcamp/04-analytics-engineering/taxi_rides_ny
```

### dbt Cloud Studio setup (recommended for this homework)

This aligns with `tutorials/04-analytics-engineering/cloud_setup.md`.

1) Create a dbt Cloud project and connect it to BigQuery.
2) Connect your repository (GitHub) so Studio can see your dbt code.

In dbt Cloud UI (high level):

- Create/choose the `taxi_rides_ny` project.
- Set up the BigQuery connection using your service account.
- **Link GitHub:** Project settings → Repository → authenticate GitHub → select your repo/branch.

Then, in **dbt Cloud Studio** (command bar):

```sh
dbt deps
dbt build --target prod
```

What those commands do:

- `dbt deps`: downloads packages defined in `packages.yml` into `dbt_packages/`.
- `dbt build`: runs models + tests in DAG order (it’s effectively `run + test` for the selection).

### Optional: Running dbt from terminal (dbt Core) against BigQuery

This section is a **dbt Core CLI** way to run from the Codespace terminal while still using **BigQuery**.

In this homework, we ran `dbt deps` / `dbt build` from **dbt Cloud Studio** (web UI) for the main build steps, and used the **BigQuery CLI** (`bq`) from the Codespace terminal for loading/querying.

If you prefer (or need) to run dbt from the terminal instead of Studio, use the local instructions below.

1) Install dbt in a Python environment.

Note: dbt-core versions older than ~1.7 may crash on Python 3.12. If you see `ModuleNotFoundError: No module named 'distutils'`, upgrade `dbt-bigquery` (or use Python 3.11).

```sh
cd /workspaces/de-homework/04-analytics-engineering/taxi_rides_ny

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip

# Install dbt adapter (includes dbt-core)
pip install -U dbt-bigquery
```

2) Point dbt to the repo’s `profiles.yml` and verify connectivity:

```sh
export DBT_PROFILES_DIR=$PWD
dbt debug --target prod
```

3) Build models + run tests:

```sh
dbt deps
dbt build --target prod
```

Where you can run these commands:

- **dbt Cloud Studio:** run `dbt deps`, `dbt build` in the Studio command bar (executes in the dbt Cloud environment).
- **Codespace terminal (dbt Core):** run the commands above after setting `DBT_PROFILES_DIR=$PWD` (executes locally, using `profiles.yml`).

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)`
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- **`int_trips_unioned` only**
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)


**Answer:** `dbt run --select int_trips_unioned` selects *only* that node.

**What happened programmatically:**

1) dbt parses your project and builds the DAG (the dependency graph from `ref()`/`source()` calls).
2) The selector `int_trips_unioned` picks exactly that model node.
3) dbt executes models in dependency order *within the selected set*.

Because upstream models are **not** part of the selection, dbt will **not** build `stg_green_tripdata` / `stg_yellow_tripdata` unless you explicitly include them.

Useful selector patterns:

- Build upstream dependencies too: `dbt run --select +int_trips_unioned`
- Build downstream dependents too: `dbt run --select int_trips_unioned+`
- Build both sides: `dbt run --select +int_trips_unioned+`

---

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- **`dbt will fail the test, returning a non-zero exit code`**
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value


**Answer:** `dbt will fail the test, returning a non-zero exit code`.

**What happened programmatically (why the test fails):**

1) dbt reads your `schema.yml` and sees a generic test configuration for `payment_type`.
2) dbt creates a *test node* in the DAG (tests are nodes too).
3) `dbt test --select fct_trips` resolves the selection, then runs all tests associated with that model.
4) The `accepted_values` test compiles into a SQL query that returns **the failing rows**.

Conceptually the compiled test query looks like:

```sql
select payment_type
from `<PROJECT_ID>.<DBT_DATASET>.fct_trips`
where payment_type not in (1, 2, 3, 4, 5)
```

- If the query returns **0 rows**, the test passes.
- If the query returns **1+ rows** (e.g. because `payment_type = 6` exists), the test fails.

When a test fails, dbt marks the run as failed and exits with a **non-zero** status code. dbt does not auto-fix your test configuration; you would need to update the allowed values (or adjust upstream logic) yourself.

---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- **`12,184`**
- 15,421

**Procedure:**

1) In **dbt Cloud Studio** (command bar), run dbt so the model is materialized as a table in your dbt output dataset.

Option A (direct):

```sh
dbt run --select fct_monthly_zone_revenue --target prod
```

Option B (build from an upstream node and include downstream dependents):

```sh
dbt run --select int_trips_unioned+ --target prod
```

2) In **BigQuery SQL editor** (or via `bq query`), count rows in the model table:

```sql
select count(*) as row_count
from `<PROJECT_ID>.<DBT_DATASET>.fct_monthly_zone_revenue`;
```

**Answer:** This environment returned `12,192` rows.

**What happened programmatically:**

1) dbt reads the project files and builds a dependency graph (DAG) from `ref()`/`source()` calls.
2) dbt resolves the selector:
  - Option A selects only the `fct_monthly_zone_revenue` node.
  - Option B selects `int_trips_unioned` plus all downstream nodes because of the trailing `+`.
3) dbt compiles each selected model into a BigQuery SQL statement by:
  - replacing `{{ ref('...') }}` with the correct relation name in BigQuery, and
  - applying the project’s materialization config (this project uses `table`).
4) dbt executes the compiled SQL in DAG order (upstream first). For this model chain that means it will create/replace the tables needed so that `fct_monthly_zone_revenue` can be built.
5) Once `fct_monthly_zone_revenue` is materialized as a physical BigQuery table in `<DBT_DATASET>`, the `COUNT(*)` query is just a normal BigQuery scan of that table.

Note: the multiple-choice options may differ slightly depending on exactly which raw 2019–2020 TLC files were loaded, and whether there were small variations in the upstream raw tables.

---

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- **`East Harlem North`**
- Morningside Heights
- East Harlem South
- Washington Heights South

**Procedure:**

1) In **BigQuery SQL editor**, aggregate total revenue by pickup zone for 2020 Green trips.

Note: `service_type` is stored in lowercase (use `green`, not `Green`).

```sql
select
  pickup_zone,
  sum(revenue_monthly_total_amount) as total_revenue
 from `<PROJECT_ID>.<DBT_DATASET>.fct_monthly_zone_revenue`
where service_type = 'green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by total_revenue desc
limit 5;
```

2) Take the `pickup_zone` in the first row (highest `total_revenue`).

**Answer:** 2,069,513 → `East Harlem North`

**Breakdown:**

1) Query a dbt-produced model table: `<PROJECT_ID>.<DBT_DATASET>.fct_monthly_zone_revenue`.
2) That model is pre-aggregated by `service_type`, `revenue_month`, and `pickup_location_id` (then joined to zones), so the query is aggregating *monthly* totals into a single 2020 total per `pickup_zone`.
3) `extract(year from revenue_month) = 2020` filters the model down to the 12 monthly rows per zone in 2020.
4) `sum(revenue_monthly_total_amount)` computes total 2020 revenue per pickup zone.
5) `order by total_revenue desc limit 5` sorts zones by total revenue and returns the top candidates; the first row is the correct multiple-choice zone.

---

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- **`500,234`**
- 350,891
- 384,624
- 421,509

**Procedure:**

1) In **BigQuery SQL editor**, compute total Green trips for October 2019.

```sql
select
  sum(total_monthly_trips) as total_trips
from `<PROJECT_ID>.<DBT_DATASET>.fct_monthly_zone_revenue`
where service_type = 'green'
  and extract(year from revenue_month) = 2019
  and extract(month from revenue_month) = 10;
```

2) Match the returned `total_trips` value to the multiple-choice options.

**Answer:** Query result: `476,385` → closest option: `500,234`

**Breakdown:**

1) Query the dbt model table `<PROJECT_ID>.<DBT_DATASET>.fct_monthly_zone_revenue`, which contains one row per:
  - `service_type`
  - `revenue_month` (month bucket)
  - `pickup_location_id` (joined to `pickup_zone`)
2) The `where` clause filters that table down to:
  - `service_type = 'green'`
  - `revenue_month = 2019-10-01` (because year=2019 and month=10)
3) `sum(total_monthly_trips)` then adds up the per-zone October totals into a single October 2019 trip count for Green taxis.


---

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- **`43,244,693`**
- 22,998,722
- 44,112,187

**Procedure**:
1) Download the 2019 FHV files (`fhv_tripdata_2019-01.csv.gz` … `fhv_tripdata_2019-12.csv.gz`) and unzip them to `fhv_tripdata_2019-*.csv`.

2) Combine all 12 CSVs into one file:

```sh
cat fhv_tripdata_2019-*.csv > fhv_tripdata_2019_all.csv
```

3) Load the combined CSV into BigQuery:

```sh
bq load \
  --location=<REGION> \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --autodetect \
  --max_bad_records=100 \
  <RAW_DATASET>.fhv_tripdata_2019 \
  fhv_tripdata_2019_all.csv
```

4) Create a dbt source for `fhv_tripdata_2019` (add it under the existing `sources:` definition).

5) Create `stg_fhv_tripdata.sql` so it:

- filters `dispatching_base_num is not null`
- renames columns like `PUlocationID` → `pickup_location_id`

The model should read from:

```sql
from {{ source('nytaxi', 'fhv_tripdata_2019') }}
```

6) Count rows after applying the staging filter.

Raw-table count (matches the staging filter requirement):

```sql
select count(*) as row_count
from `<PROJECT_ID>.<RAW_DATASET>.fhv_tripdata_2019`
where dispatching_base_num is not null;
```

If you also want to count the dbt model (after running `dbt run --select stg_fhv_tripdata --target prod`):

```sql
select count(*) as row_count
from `<PROJECT_ID>.<DBT_DATASET>.stg_fhv_tripdata`;
```

**Answer:** 43,244,693

**Breakdown:**

1) The `bq load` command creates a native BigQuery table `<PROJECT_ID>.<RAW_DATASET>.fhv_tripdata_2019` by parsing the CSV and inferring column types via `--autodetect`.
2) Because of the concatenated 12 monthly CSVs, the combined file contains repeated header rows inside the data. Those header lines can’t be parsed as timestamps, so BigQuery would normally fail the load.
3) `--max_bad_records=100` tells BigQuery to tolerate/skip a small number of bad lines, allowing the load to succeed while dropping the repeated header rows.
4) The staging requirements for `stg_fhv_tripdata` are:
  - a filter (`dispatching_base_num is not null`), and
  - renaming columns to match conventions.
  Renames don’t change row counts; the filter does.
5) That’s why the “raw-table count with the filter” query and the “dbt model count” query are expected to return the same number: the dbt model is essentially applying that same filter on top of the loaded raw table.


---

## Submitting the solutions

- Form for submitting: <https://courses.datatalks.club/de-zoomcamp-2026/homework/hw4>

=======

## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 4 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 4 - Analytics Engineering with dbt. Learned how to:

✅ Build transformation models with dbt
✅ Create staging, intermediate, and fact tables
✅ Write tests to ensure data quality
✅ Understand lineage and model dependencies
✅ Analyze revenue patterns across NYC zones

Transforming raw data into analytics-ready models - the T in ELT!

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
📈 Module 4 of Data Engineering Zoomcamp done!

- Analytics Engineering with dbt
- Transformation models & tests
- Data lineage & dependencies
- NYC taxi revenue analysis

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```
