# Real-Time Kafka to Databricks Lakehouse Pipeline

## Project Overview

This project demonstrates an end-to-end real-time data engineering pipeline using **Confluent Kafka**, **Databricks Lakeflow Declarative Pipelines**, **Delta Lake**, **Databricks Asset Bundles / Declarative Automation Bundles**, **Databricks Jobs**, and **BI visualization**.

The goal of this project is to practice and demonstrate a realistic streaming data engineering workflow:

1. Generate sample event messages with a Python Kafka producer.
2. Send the messages to a Confluent Kafka topic.
3. Ingest Kafka messages into Databricks using Structured Streaming.
4. Store raw Kafka data in a Bronze table.
5. Parse and clean the raw JSON messages into a Silver table.
6. Enrich the Silver data with a static lookup table loaded from CSV.
7. Aggregate the enriched data into a Gold table.
8. Orchestrate the full workflow with a Databricks Job.
9. Visualize the Gold table using a Databricks Dashboard and Power BI.

This project is designed as a practical portfolio project for data engineering roles. It covers streaming ingestion, Medallion architecture, data quality, enrichment, orchestration, deployment, and dashboarding.

---

## Architecture

```text
Python Kafka Producer
        |
        v
Confluent Kafka Topic
        |
        v
Databricks Lakeflow Declarative Pipeline
        |
        +--> Bronze Table
        |       Raw Kafka records
        |
        +--> Silver Table
        |       Parsed and cleaned JSON messages
        |
        +--> Silver Enriched Table
        |       Silver events joined with id_lookup_table
        |
        +--> Gold Table
                Aggregated business-ready output
                        |
                        v
          Databricks Dashboard / Power BI
```

The full workflow is orchestrated using a Databricks Job:

```text
Databricks Job
│
├── Task 1: Create / refresh id_lookup_table from CSV
│
├── Task 2: Run Lakeflow pipeline
│       Kafka → Bronze → Silver → Silver Enriched → Gold
│
└── Task 3: Databricks Dashboard
```

Power BI is used as an external reporting layer connected to the Gold table through Databricks SQL. It is not part of the Databricks Job DAG because Power BI is a downstream BI consumer rather than an internal ETL task.

---

## Tech Stack

* Python
* Confluent Kafka
* Databricks
* Spark Structured Streaming
* Lakeflow Declarative Pipelines
* Delta Lake
* Databricks Asset Bundles / Declarative Automation Bundles
* Databricks Jobs
* Azure Blob Storage / Databricks Volume
* Databricks Dashboard
* Power BI
* GitHub

---

## Repository Structure

```text
real-time-kafka-databricks-pipeline/
│
├── databricks.yml
├── pyproject.toml
├── README.md
├── .gitignore
│
├── resources/
│   ├── sample_job.job.yml
│   └── asset_bundle_project_etl.pipeline.yml
│
├── src/
│   ├── asset_bundle_project/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   └── taxi.py
│   │
│   ├── asset_bundle_project_etl/
│   │
│   ├── transformations/
│   │
│   ├── bronze/
│   │   └── kafka_ingestion.py
│   │
│   ├── silver/
│   │   └── parse_and_clean.sql
│   │
│   ├── Silver_Enriched/
│   │   └── join_silver_id_lookup_table.sql
│   │
│   ├── gold/
│   │   └── aggregations.sql
│   │
│   ├── ingest_from_blob.py
│   └── pipeline.py
│
├── producer/
│   └── sample_producer.py
```

Recommended future cleanup:

```text
Silver_Enriched/  →  silver_enriched/
```

Using lowercase folder names is cleaner and reduces path-related mistakes.

---

## Data Flow

### 1. Kafka Producer

The project includes a Python producer:

```text
producer/sample_producer.py
```

The producer sends simple JSON messages to a Confluent Kafka topic.

Example message:

```json
{
  "id": 1,
  "message": "sample message 1"
}
```

The producer uses environment variables for credentials:

```text
KAFKA_API_KEY
KAFKA_API_SECRET
```

This avoids hardcoding secrets in the source code.

---

### 2. Confluent Kafka Topic

The producer sends messages to the Kafka topic:

```text
my_first_topic
```

The topic acts as the streaming source for Databricks.

---

### 3. Bronze Layer

File:

```text
src/bronze/kafka_ingestion.py
```

Purpose:

The Bronze layer ingests raw Kafka records using Spark Structured Streaming.

The Bronze table keeps the original Kafka fields such as:

```text
key
value
topic
partition
offset
timestamp
timestampType
```

The `value` column is still in raw Kafka format at this stage.

Bronze is intentionally raw. It is useful for debugging, replaying, and tracing data lineage.

---

### 4. Silver Layer

File:

```text
src/silver/parse_and_clean.sql
```

Purpose:

The Silver layer parses the Kafka `value` field from JSON and extracts clean columns.

Example output:

```text
id
message
topic
partition
offset
timestamp
```

Data quality expectations are applied in this layer.

Example checks:

```text
id IS NOT NULL
message IS NOT NULL
```

Invalid records can be dropped before they move further in the pipeline.

---

### 5. Static Lookup Table

File:

```text
src/ingest_from_blob.py
```

Purpose:

This step loads a small CSV lookup file from Blob Storage or a Databricks Volume and creates a Delta table.

Example lookup data:

```csv
id,name,category
0,Alice,Category_A
1,Bob,Category_B
2,Charlie,Category_A
3,David,Category_C
4,Eva,Category_B
5,Frank,Category_A
6,Grace,Category_C
7,Hassan,Category_B
8,Ivy,Category_A
9,Jack,Category_C
```

This table is used to enrich the streaming events.

---

### 6. Silver Enriched Layer

File:

```text
src/Silver_Enriched/join_silver_id_lookup_table.sql
```

Purpose:

The Silver Enriched layer joins the cleaned streaming events with the static lookup table.

Conceptually:

```text
silver_table
    JOIN
id_lookup_table
    ON silver_table.id = id_lookup_table.id
```

Example enriched output:

```text
id
message
name
category
timestamp
```

This step simulates a common data engineering pattern:

```text
streaming fact data + static reference data
```

---

### 7. Gold Layer

File:

```text
src/gold/aggregations.sql
```

Purpose:

The Gold layer creates business-ready aggregated data.

Example aggregation:

```text
count of messages by id
count of messages by name
count of messages by category
```

The Gold table is used by dashboards and BI tools.

---

## Databricks Asset Bundle / Declarative Automation Bundle

The project is deployed using a Databricks bundle.

Main bundle configuration:

```text
databricks.yml
```

Resource configuration files:

```text
resources/sample_job.job.yml
resources/asset_bundle_project_etl.pipeline.yml
```

The bundle contains two deployment targets:

```text
dev
prod
```

This allows the same project to be deployed to different environments.

Example commands:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run <job-name> --target dev
```

For production:

```bash
databricks bundle validate --target prod
databricks bundle deploy --target prod
databricks bundle run <job-name> --target prod
```

---

## Databricks Job Orchestration

The workflow is orchestrated as a Databricks Job.

The Job contains three main tasks:

### Task 1: Create / Refresh Lookup Table

This task reads the lookup CSV from Blob Storage or a Volume and creates the `id_lookup_table`.

### Task 2: Run Lakeflow Pipeline

This task runs the main pipeline:

```text
Kafka → Bronze → Silver → Silver Enriched → Gold
```

### Task 3: Databricks Dashboard

This task links the final Gold layer to a Databricks Dashboard for visualization.

---

## Dashboard and Power BI

The Gold table is visualized in two ways:

### Databricks Dashboard

The Databricks Dashboard is included as part of the Databricks workflow to show the final aggregated result inside the Databricks environment.

### Power BI

Power BI is connected externally to the Gold table through Databricks SQL.

The Power BI layer is not part of the Databricks Job DAG because it is a downstream reporting and consumption tool.

Architecture:

```text
Gold Delta Table
        |
        v
Databricks SQL Warehouse
        |
        v
Power BI Dashboard
```

---

## How to Run the Kafka Producer

Go to the producer folder:

```bash
cd producer
```

Create and activate a Python environment:

```bash
python -m venv .venv
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

On macOS/Linux:

```bash
source .venv/bin/activate
```

Install dependency:

```bash
pip install confluent-kafka
```

Set environment variables.

On Windows PowerShell:

```powershell
$env:KAFKA_API_KEY="your_api_key"
$env:KAFKA_API_SECRET="your_api_secret"
```

On macOS/Linux:

```bash
export KAFKA_API_KEY="your_api_key"
export KAFKA_API_SECRET="your_api_secret"
```

Run the producer:

```bash
python sample_producer.py
```

The producer sends sample JSON messages to the Kafka topic.

---

## How to Deploy the Databricks Bundle

From the root of the project:

```bash
databricks bundle validate --target dev
```

Deploy to development:

```bash
databricks bundle deploy --target dev
```

Run the job:

```bash
databricks bundle run <job-name> --target dev
```

Deploy to production:

```bash
databricks bundle deploy --target prod
```

Run production:

```bash
databricks bundle run <job-name> --target prod
```

---

## Secrets and Security

Do not commit secrets to GitHub.

The following values must not be pushed:

```text
Confluent API key
Confluent API secret
Databricks token
Azure credentials
.env files
local secret text files
```

Kafka credentials should be stored in Databricks Secrets and read in the pipeline.

Example secret names:

```text
scope: kafka_credentials
key: API_KEY
key: API_SECRET
```

The local Kafka producer reads credentials from environment variables.

---

## What This Project Demonstrates

This project demonstrates practical data engineering skills:

* Kafka producer development
* Confluent Kafka topic usage
* Spark Structured Streaming
* Lakeflow Declarative Pipelines
* Bronze/Silver/Gold Medallion architecture
* Data quality expectations
* Streaming table creation
* Materialized view / aggregation logic
* Static lookup enrichment
* Databricks Job orchestration
* Databricks Asset Bundle deployment
* Dev/prod environment configuration
* Databricks Dashboard
* Power BI connection to Databricks Gold table
* GitHub-based project documentation

---

## Interview Explanation

A simple way to explain this project in an interview:

> I built an end-to-end real-time data engineering pipeline using Confluent Kafka and Databricks. A Python producer sends JSON events to a Kafka topic. Databricks ingests the stream using Structured Streaming into a Bronze table. Then Lakeflow Declarative Pipelines parse and clean the data into Silver, enrich it with a static lookup table, and aggregate it into a Gold table. I orchestrated the lookup ingestion, pipeline execution, and dashboard in a Databricks Job. I also deployed the project using Databricks Asset Bundles with dev and prod targets, and connected the Gold table to Power BI for external reporting.

---

## Future Improvements

Possible next improvements:

* Add schema registry for Kafka messages
* Add more realistic event data
* Add error/quarantine table for invalid records
* Add watermarking and window-based aggregations
* Add CI/CD with GitHub Actions
* Add automated tests for transformation logic
* Add Docker support for local producer execution
* Add dbt for batch analytics transformations
* Add monitoring and alerting for pipeline failures
* Add a more advanced Power BI dashboard

---

## Project Status

This project is complete as a first end-to-end streaming data engineering portfolio project.

It includes:

```text
Kafka Producer
Confluent Kafka
Databricks Streaming Ingestion
Bronze/Silver/Silver Enriched/Gold Layers
Databricks Job Orchestration
DAB Deployment
Databricks Dashboard
Power BI Connection
```
