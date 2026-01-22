# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.


## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

- **`25.3`**
- 24.3.1
- 24.2.1
- 23.3.1

```sh
docker run -it \
    --rm \
    --entrypoint=bash \
    python:3.13-slim
```
```docker
pip -V
```
## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres # hostname inside the Docker network
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432' # maps host port 5433 to container port 5432
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- **`postgres:5432`**
- db:5432

If multiple answers are correct, select any 


## Prepare the Data

Download the green taxi trips data for November 2025:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

## Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

- 7,853
- **`8,007`**
- 8,254
- 8,421

### Option A — CLI (uv + Docker + pgcli) Procedure

1) Prepare Python environment and packages

```bash
cd 01-docker-terraform

# Check or install uv
uv -V || pipx install uv || pip install --user uv

# Initialize project and install runtime + dev tools
uv init --python 3.13
uv add pandas pyarrow sqlalchemy psycopg2-binary click
uv add --dev pgcli

# Download datasets
wget -nc https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
wget -nc https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

2) Start Postgres and pgAdmin with Docker Compose

```bash
docker compose up -d
docker compose ps
```

3) Ingest the Parquet file into Postgres

```bash
uv run python ingest_green_parquet.py \
  --user postgres \
  --password postgres \
  --host localhost \
  --port 5433 \
  --db ny_taxi \
  --table green_taxi_trips \
  --file green_tripdata_2025-11.parquet
```

4) Query the short trips (<= 1 mile) for Nov 2025

```bash
uv run pgcli -h localhost -p 5433 -u postgres -d ny_taxi
```

Run the SQL inside `pgcli`:

```sql
SELECT COUNT(*) AS short_trips
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

* **Answer: 8,007**

Exit `pgcli` with `\\q` or Ctrl+D.


### Option B — pgAdmin UI Procedure

1) Open pgAdmin and login

```bash
# Open in browser
http://localhost:8080
```

- Email: `pgadmin@pgadmin.com`
- Password: `pgadmin`

2) Register the Postgres server

- Right-click “Servers” → Register → Server
- General → Name: any label (e.g., "pg")
- Connection tab:
  - Host name/address: `postgres`
  - Port: `5432`
  - Maintenance database: `ny_taxi`
  - Username: `postgres`
  - Password: `postgres`
  - Save

3) Open Query Tool and run the SQL

- In the tree: Databases → `ny_taxi` → Schemas → `public`
- Right-click “Tables” → Query Tool

Run:

```sql
SELECT COUNT(*) AS short_trips
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

* **Answer: 8,007**


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

Use the pick up time for your calculations.

- **`2025-11-14`**
- 2025-11-20
- 2025-11-23
- 2025-11-25

1) Create and load zones once (pgAdmin Query Tool):
```sql
CREATE TABLE IF NOT EXISTS zones (
  "LocationID" INTEGER PRIMARY KEY,
  "Borough" TEXT,
  "Zone" TEXT,
  "service_zone" TEXT
);
```

2) Refresh the pgAdmin’s tree and use the 'zones' table: 
`right-click Tables → Import/Export → Import → General tab: taxi_zone_lookup.csv → Options tab: Header`

3) Longest trip per day script:
```sql
WITH per_day AS (
  SELECT
    CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
    MAX(trip_distance) AS max_trip_distance
  FROM green_taxi_trips
  WHERE trip_distance < 100
  GROUP BY 1
)
SELECT pickup_day, max_trip_distance
FROM per_day
ORDER BY max_trip_distance DESC
LIMIT 1;
```
**Answer: 2025-11-14**

## Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

- **`East Harlem North`**
- East Harlem South
- Morningside Heights
- Forest Hills

 Biggest pickup zone by total_amount on 2025‑11‑18:
```sql
SELECT
  zpu."Zone" AS pickup_zone,
  SUM(t.total_amount) AS total_amount
FROM green_taxi_trips t
JOIN zones zpu
  ON t."PULocationID" = zpu."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-18'
  AND t.lpep_pickup_datetime < '2025-11-19'
GROUP BY zpu."Zone"
ORDER BY total_amount DESC
LIMIT 1;
```
**Answer: East Harlem North**

## Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

- JFK Airport
- **`Yorkville West`**
- East Harlem North
- LaGuardia Airport

- Largest single tip for passengers picked up in “East Harlem North” during Nov 2025:
```sql
SELECT
  zdo."Zone" AS dropoff_zone,
  MAX(t.tip_amount) AS max_tip
FROM green_taxi_trips t
JOIN zones zpu
  ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo
  ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY zdo."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```
**Answer: Yorkville West**

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform.
Copy the files from the course repo
[here](../../../01-docker-terraform/terraform/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

## Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- **`terraform init, terraform apply -auto-approve, terraform destroy`**
- terraform import, terraform apply -y, terraform rm

### Setup Procedure (GCP + Terraform)

1) Install Terraform CLI (Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y wget gnupg software-properties-common
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update
sudo apt-get install -y terraform
terraform -version
```

2) Prepare GCP project & service account (Console)
- Create a GCP project and note its Project ID.
- Create a service account; grant roles: Storage Admin, Storage Object Admin, BigQuery Admin.
- Create a JSON key for the service account and download it to your workspace (e.g., `01-docker-terraform/keys/my-creds.json`).
- Enable APIs: IAM API and IAM Credentials API for the project.

Optional (SDK):
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/de-homework/01-docker-terraform/keys/my-creds.json"
# If gcloud SDK is available, you can verify:
# gcloud auth application-default login
```

3) Configure Terraform variables
- Edit `01-docker-terraform/variables.tf` defaults or pass `-var` flags.
- Ensure:
  - `credentials` points to your JSON key path
  - `project` is your Project ID
  - `gcs_bucket_name` is globally unique (e.g., `dtc-zoomcamp-<your-id>-bucket`)
  - `region` and `location` match your desired region/location

Example with flags (without editing files):
```bash
cd 01-docker-terraform
terraform init
terraform validate
terraform plan \
  -var "credentials=/workspaces/de-homework/01-docker-terraform/keys/my-creds.json" \
  -var "project=<YOUR_PROJECT_ID>" \
  -var "gcs_bucket_name=dtc-zoomcamp-<YOUR_ID>-bucket"
terraform apply -auto-approve \
  -var "credentials=/workspaces/de-homework/01-docker-terraform/keys/my-creds.json" \
  -var "project=<YOUR_PROJECT_ID>" \
  -var "gcs_bucket_name=dtc-zoomcamp-<YOUR_ID>-bucket"
```

4) Verify resources
- In GCP Console → Cloud Storage: bucket exists.
- In GCP Console → BigQuery: dataset exists.

5) Cleanup
```bash
terraform destroy -auto-approve \
  -var "credentials=/workspaces/de-homework/01-docker-terraform/keys/my-creds.json" \
  -var "project=<YOUR_PROJECT_ID>" \
  -var "gcs_bucket_name=dtc-zoomcamp-<YOUR_ID>-bucket"
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw1


## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

### Why learn in public?

- Accountability: Sharing your progress creates commitment and motivation to continue
- Feedback: The community can provide valuable suggestions and corrections
- Networking: You'll connect with like-minded people and potential collaborators
- Documentation: Your posts become a learning journal you can reference later
- Opportunities: Employers and clients often discover talent through public learning

You can read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

Don't worry about being perfect. Everyone starts somewhere, and people love following genuine learning journeys!

### Example post for LinkedIn

```
🚀 Week 1 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 1 - Docker & Terraform. Learned how to:

✅ Containerize applications with Docker and Docker Compose
✅ Set up PostgreSQL databases and write SQL queries
✅ Build data pipelines to ingest NYC taxi data
✅ Provision cloud infrastructure with Terraform

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X


```
🐳 Module 1 of Data Engineering Zoomcamp done!

- Docker containers
- Postgres & SQL
- Terraform & GCP
- NYC taxi data pipeline

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

