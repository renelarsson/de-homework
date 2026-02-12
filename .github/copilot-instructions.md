# AI Coding Agent Instructions for this Repository

## Overview
- **Repo State:** Minimal repository with only [.gitignore](.gitignore); no source code or README yet.
- **Language Signal:** [.gitignore](.gitignore) strongly indicates a Python-oriented setup (e.g., `__pycache__/`, `.pytest_cache/`, `.ruff_cache/`, `.ipynb_checkpoints`).
- **Default Branch:** `main` (per repository metadata); open PRs against `main` with small, focused changes.
- **Environment:** Dev container on Ubuntu 24.04.3 LTS; common CLI tools available (e.g., `apt`, `git`). Treat tasks as greenfield unless user specifies otherwise.

## Conventions & Implications from .gitignore
- **Python Artifacts:** Caches and build outputs are ignored (e.g., `__pycache__/`, `build/`, `dist/`). Prefer virtual environments in `.venv/`.
- **Testing:** `.pytest_cache/` is ignored → adopt `pytest` if adding tests.
- **Linting:** `.ruff_cache/` is ignored → use `ruff` for linting if needed.
- **Notebooks:** `.ipynb_checkpoints` is ignored → notebooks allowed but considered ephemeral; commit only curated notebooks.

## When Implementing New Python Code
- **Structure:** Create `src/` for application code and `tests/` for unit tests. Use `pytest` discovery (test files named `test_*.py`).
- **Dependencies:** Prefer `requirements.txt` or `pyproject.toml` (Poetry/PDM acceptable). Keep lockfiles if the chosen tool recommends committing them.
- **Entrypoints:** Provide a simple CLI or `main.py` under `src/` and document run commands in `README.md`.
- **Quality:** Run `ruff` for linting and (optionally) `pytest -q` for tests before commits.

## Suggested Developer Commands (Python)
```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip (optional)
pip install -U pip

# Install project deps (once requirements or pyproject exists)
pip install -r requirements.txt
# or, for Poetry: poetry install
# or, for PDM: pdm install

# Run tests (when tests/ exists)
pytest -q

# Lint (if ruff is used)
ruff check .
```

## Pull Requests & Commits
- **Small PRs:** Keep PRs narrowly scoped and reference affected files.
- **Artifacts:** Do not commit virtualenvs, caches, or compiled artifacts (see [.gitignore](.gitignore)).
- **Docs:** When adding functionality, update `README.md` with setup and run steps.

## Collaboration Expectations
- **Clarify First:** If the task is ambiguous (stack, goals, data sources), ask for specifics before scaffolding.
- **Greenfield Start:** With no existing code, propose a minimal Python scaffold only after confirming requirements (data engineering, ETL, API, notebooks, etc.).
- **Traceability:** Include rationale in PR descriptions (why chosen structure/tools) and sample run commands.

## Examples to Model (once code exists)
- **Tests:** Place unit tests under `tests/` mirroring modules in `src/`.
- **Data/ETL:** Keep configs (e.g., YAML/TOML) out of code where possible and document input/output locations.
- **Notebooks:** Commit curated notebooks with clear cell ordering; avoid large raw outputs.

---
If any of the above doesn’t match intended direction (e.g., not Python, different tooling), please specify preferred stack and workflows so I can tailor these instructions immediately.

# Local AI Coding Agent Instructions

## Repo layout (modules are intentionally independent)
- `01-docker-terraform/`: local Postgres+pgAdmin for SQL homework, a small Python ingest CLI, plus Terraform for GCS/BQ.
- `02-workflow-orchestration/`: Kestra + Postgres + flows that download NYC TLC CSVs and load them into `ny_taxi`.
- `03-data-warehouse/`: scripts/notebooks for uploading 2024 Yellow parquet files to GCS and doing BigQuery exercises.
- `tutorials/`: reference material and examples (not the graded “happy path”).

## Quick gotchas
- Port clash: `01-docker-terraform` exposes pgAdmin on `localhost:8080`, and `02-workflow-orchestration` exposes Kestra UI on `localhost:8080`. Run one stack at a time (or change ports).
- Postgres ports differ: module 1 maps to `localhost:5433` (see `01-docker-terraform/docker-compose.yaml`); module 2 maps to `localhost:5432` (see `02-workflow-orchestration/docker-compose.yaml`).

## Module 1 (Docker + SQL + ingest)
- Start DB/UI: `cd 01-docker-terraform && docker compose up -d`
- Ingest Parquet into Postgres: `uv run python ingest_green_parquet.py --host localhost --port 5433 --user postgres --password postgres --db ny_taxi --table green_taxi_trips --file green_tripdata_2025-11.parquet` (see `01-docker-terraform/ingest_green_parquet.py`).
- Python deps are declared in `01-docker-terraform/pyproject.toml` (requires Python `>=3.13`). The Docker image uses `uv sync --locked` (see `01-docker-terraform/Dockerfile`).

## Terraform (GCP infra)
- Terraform config in `01-docker-terraform/main.tf` creates a GCS bucket + BigQuery dataset using variables from `01-docker-terraform/variables.tf`.
- Credentials default to `01-docker-terraform/keys/my-creds.json` via the `credentials` variable; prefer editing variables or passing `-var` rather than hardcoding.

## Module 2 (Kestra orchestration)
- Start services: `cd 02-workflow-orchestration && docker compose up -d` (Kestra UI at `http://localhost:8080`, login from compose: `admin@kestra.io` / `Admin1234`).
- Import flows: `curl -X POST -u 'admin@kestra.io:Admin1234' http://localhost:8080/api/v1/flows/import -F fileUpload=@hw2_kestra_1.yaml`
- Flow pattern (both `02-workflow-orchestration/hw2_kestra_1.yaml` and `02-workflow-orchestration/hw2_kestra_2.yaml`):
  `extract` (wget+gunzip) → `CopyIn` to `public.<taxi>_tripdata_staging` → compute `unique_row_id` + `filename` → `MERGE` into `public.<taxi>_tripdata`.

## Module 3 (GCS + BigQuery homework)
- Upload Yellow 2024-01..06 parquet files to GCS with `03-data-warehouse/load_yellow_taxi_data.py`: update `BUCKET_NAME` and provide a local `gcs.json` service account key (script currently loads credentials from that filename).
- `03-data-warehouse/DLT_upload_to_GCP.ipynb` is Colab-oriented (sets `DESTINATION__CREDENTIALS` and `BUCKET_URL`).

## Conventions
- Keep changes scoped to a module (compose files, ports, and credentials are module-specific).
- Don’t commit secrets: expect local GCP keys under `01-docker-terraform/keys/` and `gcs.json` for module 3.
