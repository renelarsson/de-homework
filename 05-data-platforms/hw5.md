# Module 5 Homework: Data Platforms with Bruin

In this homework, we'll use Bruin to build a complete data pipeline, from ingestion to reporting.

## Setup

1. Install Bruin CLI: `curl -LsSf https://getbruin.com/install/cli | sh`
2. Initialize the zoomcamp template: `bruin init zoomcamp my-pipeline`
3. Configure your `.bruin.yml` with a DuckDB connection
4. Follow the tutorial in the [main module README](../../../05-data-platforms/)

After completing the setup, you should have a working NYC taxi data pipeline.

## Quick start (clone → run)

These steps assume installation, extention, and initialization is done, and the existing pipeline can be tested directly at `05-data-platforms/my-pipeline/`.

1) Install Bruin CLI (once):

```sh
curl -LsSf https://getbruin.com/install/cli | sh
export PATH="$HOME/.local/bin:$PATH"
bruin version
```

2) Validate the pipeline:

```sh
bruin validate 05-data-platforms/my-pipeline/pipeline
```

3) Pick a historical window (reproducible). Example: January 2022:

```sh
export START_DATE=2022-01-01
export END_DATE=2022-02-01
```

4) (Optional) Reset local state for reproducibility:

```sh
# Delete the DuckDB file and Bruin logs to start from a clean slate
rm -f 05-data-platforms/my-pipeline/duckdb.db
rm -rf logs/runs logs/queries
```

### Run step-by-step (seed → ingestion → staging → report)

```sh
# 1) Seed lookup table (loads the CSV into DuckDB)
bruin run 05-data-platforms/my-pipeline/pipeline/assets/ingestion/payment_lookup.asset.yml

# 2) Ingest trips (downloads TLC parquet for the window)
bruin run \
  --start-date "$START_DATE" --end-date "$END_DATE" \
  --var '{"taxi_types":["yellow"]}' \
  05-data-platforms/my-pipeline/pipeline/assets/ingestion/trips.py

# 3) First time only: build incremental tables cleanly
bruin run --full-refresh \
  --start-date "$START_DATE" --end-date "$END_DATE" \
  05-data-platforms/my-pipeline/pipeline/assets/staging/trips.sql

bruin run --full-refresh \
  --start-date "$START_DATE" --end-date "$END_DATE" \
  05-data-platforms/my-pipeline/pipeline/assets/reports/trips_report.sql
```

### Run end-to-end (entire pipeline)

First run on a fresh DuckDB database:

```sh
bruin run 05-data-platforms/my-pipeline/pipeline --full-refresh \
  --start-date "$START_DATE" --end-date "$END_DATE" \
  --var '{"taxi_types":["yellow"]}'
```

Subsequent runs for the same window (incremental):

```sh
bruin run 05-data-platforms/my-pipeline/pipeline \
  --start-date "$START_DATE" --end-date "$END_DATE" \
  --var '{"taxi_types":["yellow"]}'
```

Confirm row counts:

```sh
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM ingestion.trips"
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM staging.trips"
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM reports.trips_report"
```

## Installation and initialization


Install Bruin CLI:

```sh
curl -LsSf https://getbruin.com/install/cli | sh
export PATH="$HOME/.local/bin:$PATH"
bruin version
```

Install the VS Code Bruin extension:

- Extensions → search “Bruin” (publisher: bruin) → install → reload window
- In Settings → “Bruin Render”, confirm “Bruin CLI Status” is healthy (this means the extension can find your `bruin` binary).

Note on MCP:

- Bruin MCP is started via `bruin mcp` (it’s mainly for Cursor integration).
- VS Code extension usage does not require an `mcp.json` file.

Initialize the Zoomcamp template:

```sh
cd 05-data-platforms/
rm -rf my-pipeline
bruin init zoomcamp my-pipeline
cd my-pipeline
```

## Configure DuckDB connection (module-friendly)

Bruin typically resolves config (`.bruin.yml`) from the **git root**. Since this repo contains multiple modules, keep **one** `.bruin.yml` at the repo root and point it to the DuckDB file for this module.

From the repo root (`/workspaces/de-homework`), create/update `.bruin.yml` like this:

```yml
default_environment: default

environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: 05-data-platforms/my-pipeline/duckdb.db
```

This ensures:

- You always have a single place to configure connections.
- When you run Bruin from anywhere inside the repo, the `duckdb-default` connection points to the same DB.

Fix all TODO placeholders (follow tutorials/05-data-platforms/03-nyc-taxi-pipeline.md):

- pipeline/pipeline.yml
- pipeline/assets/ingestion/trips.py
- pipeline/assets/ingestion/payment_lookup.asset.yml
- pipeline/assets/staging/trips.sql
- pipeline/assets/reports/trips_report.sql

Validate the pipeline (this template keeps pipeline.yml under ./pipeline/):

```sh
bruin validate ./pipeline
```

## Run a single asset (seed) to load the lookup table

Seed assets ingest a local CSV into the database. This is how the payment lookup CSV gets loaded:

```sh
# Loads the CSV into DuckDB (small static lookup)
bruin run ./pipeline/assets/ingestion/payment_lookup.asset.yml
```

You can also run the Python ingestion asset only:

```sh
# Loads trip records into a different table (ingestion.trips, large fact-like trips data)
bruin run ./pipeline/assets/ingestion/trips.py
```

Or run an asset and everything downstream of it:

```sh
bruin run ./pipeline/assets/ingestion/trips.py --downstream
```

First run on a fresh DuckDB database (creates tables cleanly):

```sh
bruin run ./pipeline --full-refresh \
  --start-date 2022-01-01 --end-date 2022-02-01 \
  --var '{"taxi_types":["yellow"]}'
```

Subsequent runs for the same window (incremental):

```sh
bruin run ./pipeline \
  --start-date 2022-01-01 --end-date 2022-02-01 \
  --var '{"taxi_types":["yellow"]}'
```

Query DuckDB to confirm data loaded:

```sh
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM ingestion.trips"
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM staging.trips"
bruin query --connection duckdb-default --query "SELECT COUNT(*) AS n FROM reports.trips_report"
```

## View the pipeline in Lineage

In VS Code:

1. Open `pipeline/pipeline.yml`.
2. Open the Bruin panel (Activity Bar → Bruin).
3. Use the **Lineage** tab to see the dependency graph.

Note: The VS Code Bruin “Run” button often defaults to **today’s date**. If the NYC TLC parquet for the current month isn’t published yet, ingestion may load 0 rows. For reproducible runs, use a historical interval like `--start-date 2022-01-01 --end-date 2022-02-01`.

CLI alternative:

```sh
bruin lineage --full ./pipeline/assets/reports/trips_report.sql
```

### Question 1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

- `bruin.yml` and `assets/`
- `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
- **ANSWER:** **`.bruin.yml`** **and** **`pipeline/`** **with** **`pipeline.yml`** **and** **`assets/`**
- `pipeline.yml` and `assets/` only

---

### Question 2. Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

- `append` - always add new rows
- `replace` - truncate and rebuild entirely
- **ANSWER: `time_interval` - incremental based on a time column** 
- `view` - create a virtual table only

---

### Question 3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

- `bruin run --taxi-types yellow`
- `bruin run --var taxi_types=yellow`
- **ANSWER: `bruin run --var 'taxi_types=["yellow"]'`**
- `bruin run --set taxi_types=["yellow"]`

---

### Question 4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

- `bruin run ingestion.trips --all`
- `bruin run ingestion/trips.py --downstream`
- `bruin run pipeline/trips.py --recursive`
- **ANSWER: `bruin run --select ingestion.trips+`**

---

### Question 5. Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

- `name: unique`
- **ANSWER: `name: not_null`**
- `name: positive`
- `name: accepted_values, value: [not_null]`

---

### Question 6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

- `bruin graph`
- `bruin dependencies`
- **ANSWER: `bruin lineage`**
- `bruin show`

---

### Question 7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

- `--create`
- `--init`
- **ANSWER: `--full-refresh`**
- `--truncate`

---

## Submitting the solutions

- Form for submitting: <https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5>

=======

## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 5 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 5 - Data Platforms with Bruin. Learned how to:

✅ Build end-to-end ELT pipelines with Bruin
✅ Configure environments and connections
✅ Use materialization strategies for incremental processing
✅ Add data quality checks to ensure data integrity
✅ Deploy pipelines from local to cloud (BigQuery)

Modern data platforms in a single CLI tool - no vendor lock-in!

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
📊 Module 5 of Data Engineering Zoomcamp done!

- Data Platforms with Bruin
- End-to-end ELT pipelines
- Data quality & lineage
- Deployment to BigQuery

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```
