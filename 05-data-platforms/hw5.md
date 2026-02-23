# Module 5 Homework: Data Platforms with Bruin

In this homework, we'll use Bruin to build a complete data pipeline, from ingestion to reporting.

## Setup

1. Install Bruin CLI: `curl -LsSf https://getbruin.com/install/cli | sh`
2. Initialize the zoomcamp template: `bruin init zoomcamp my-pipeline`
3. Configure your `.bruin.yml` with a DuckDB connection
4. Follow the tutorial in the [main module README](../../../05-data-platforms/)

After completing the setup, you should have a working NYC taxi data pipeline.

# FROM HERE

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

Configure DuckDB connection in .bruin.yml (create/update this file in the my-pipeline folder):

```yml
default_environment: default

environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: duckdb.db
```

Important config gotcha:

- Bruin resolves `.bruin.yml` from the git root by default.
- To force Bruin to use THIS pipeline’s `.bruin.yml`, set:

```sh
export BRUIN_CONFIG_FILE="$PWD/.bruin.yml"
```

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
# TO HERE
---

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
