# taxi_rides_ny (dbt project)

This repo can be run in two different ways:

1) **dbt Cloud (recommended if you're following `tutorials/04-analytics-engineering/cloud_setup.md`)**
	 - You develop/run models in dbt Cloud Studio, OR
	 - You run commands from this Codespace using the **dbt Cloud CLI** (executes in the Cloud dev environment).

2) **dbt Core (local fallback)**
	 - You run dbt in this Codespace using `dbt-bigquery` + `profiles.yml`.

Important: there are **two different “materializations”** that are easy to confuse:

- **Raw-data materialization (BigQuery load step):** creating native BigQuery tables like
	`nytaxi.yellow_tripdata` and `nytaxi.green_tripdata` from Parquet/external tables.
- **dbt model materialization:** whether dbt creates *models* (e.g. `fct_monthly_zone_revenue`) as tables or views.
	That is controlled in `dbt_project.yml` (this project sets them to tables).

## Run from Codespace via dbt Cloud CLI (Cloud setup)

This matches the dbt Cloud docs flow:

1) Install the dbt Cloud CLI (follow dbt's Linux instructions in the Cloud UI/docs).
2) Download your Cloud CLI credentials to `~/.dbt/dbt_cloud.yml` (done from dbt Cloud UI).
3) This repo is already linked to your Cloud project via the `dbt-cloud:` block in `dbt_project.yml`.

Then, from this folder, run for example:

```bash
dbt compile
dbt build
```

These commands execute in your **dbt Cloud development environment** (not using `profiles.yml`).

## Run in this Codespace via dbt Core (local fallback)

From this folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install dbt-bigquery

autopep8 --version >/dev/null 2>&1 || true

export DBT_PROFILES_DIR=$PWD

dbt --version

dbt deps

dbt debug --target prod

dbt build --target prod
```

Notes:
- `DBT_PROFILES_DIR=$PWD` makes dbt use `profiles.yml` in this folder.
- BigQuery dataset is set to `nytaxi` in `profiles.yml`.
