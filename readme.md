# Global Energy Mix & GHG Emissions (2000–2023)

> An end-to-end batch data pipeline built for the Data Engineering Zoomcamp 2026, analyzing global energy consumption patterns and greenhouse gas emissions across countries and decades.

---

## Problem Description

Understanding how countries produce and consume energy, and how these patterns are changing is one of the most pressing challenges of the 21st century. For chemical engineers and environmental analysts, tracking the shift from fossil fuels to renewables is essential for:

- Evaluating industrial decarbonization progress
- Measuring the carbon intensity of electricity generation
- Designing cleaner energy infrastructure and processes


This project builds an automated data pipeline based on the [Our World in Data – Energy Dataset](https://www.kaggle.com/datasets/bhavikjikadara/global-energy-electricity-and-emissions-data?utm_source=chatgpt.com>), which consolidates historical energy indicators for 200+ countries from 1900 to 2023. The pipeline answers two core questions:

1. **How have greenhouse gas emissions evolved over time, by country?**
2. **How does each country's renewable energy share compare to its fossil fuel dependency?**

---

## Architecture

```
Our World in Data (GitHub)
        │
        ▼
   Kestra (Orchestration)
   ├── Download CSV
   ├── Convert to Parquet
   └── Upload to GCS (Data Lake)
        │
        ▼
   Google Cloud Storage
   └── raw/{date}/energy_data.parquet
        │
        ▼
   Apache Spark (Processing)
   ├── Read Parquet from GCS
   ├── Filter & select columns
   └── Write to BigQuery (staging)
        │
        ▼
   BigQuery (Data Warehouse)
   ├── energy_staging      
   ├── stg_energy          
   ├── int_energy_by_country 
   ├── mart_energy_overview  
   └── mart_renewables_vs_fossil 
        │
        ▼
   Looker Studio (Dashboard)
   ├── Tile 1: GHG emissions over time by country (mart_energy_overview)
   └── Tile 2: Renewables vs Fossil fuel consumption ( mart_renewables_vs_fossil)
```

---

## Technologies

| Layer | Tool | Purpose |
|---|---|---|
| Cloud | GCP (Google Cloud Platform) | All infrastructure |
| IaC | Terraform | Provision GCS, BigQuery, Service Account |
| Orchestration | Kestra | Batch pipeline, scheduled daily |
| Data Lake | Google Cloud Storage | Raw Parquet files |
| Processing | Apache Spark (PySpark) | Data cleaning & transformation |
| Data Warehouse | BigQuery | Analytical tables |
| Transformation | dbt Core | Staging → Mart models |
| Dashboard | Looker Studio | Interactive visualizations |

---

## Cloud & Infrastructure (IaC)

All infrastructure is provisioned on **Google Cloud Platform** using **Terraform**, including:

- **GCS Bucket** — Data lake for raw Parquet files, partitioned by ingestion date (`raw/yyyy-mm-dd/`)
- **BigQuery Dataset** — Data warehouse (`energy_data`) in region `us-central1`
- **Service Account** — `energy-pipeline-sa` with `Storage Admin` and `BigQuery Admin` roles

---

## Data Ingestion — Batch Pipeline

Orchestrated with **Kestra**. The pipeline has three tasks:

1. `download_csv` — Downloads the latest CSV from the official Our World in Data GitHub repository
2. `convert_to_parquet` — Converts the CSV to Parquet format using pandas + pyarrow (more efficient for analytical workloads)
3. `upload_to_gcs` — Uploads the Parquet file to GCS under `raw/{date}/energy_data.parquet`




---

## Data Warehouse

Data is stored in **BigQuery** (`us-central1`) in the `energy_data` dataset. The `energy_staging` table produced by Spark is:

- **Partitioned** by `ingestion_date` (DATE type) — allows BigQuery to skip partitions when filtering by date, reducing query costs significantly
- **Column-pruned** — reduced from 130 original columns to 30 analytically relevant columns

The dbt mart tables (`mart_energy_overview`, `mart_renewables_vs_fossil`) are materialized as **tables** (not views) so that Looker Studio reads pre-computed results rather than recomputing on every dashboard load.

---

## Transformations (dbt + Spark)

**Spark** handles the heavy lifting:
- Reads raw Parquet from GCS
- Filters null countries and years
- Selects 30 relevant columns from 130
- Adds `ingestion_date` column
- Writes to BigQuery with partitioning

**dbt Core** (with BigQuery adapter) handles analytical transformations across 3 layers:

```
staging/
└── stg_energy.sql              ← cleans source, filters nulls

intermediate/
└── int_energy_by_country.sql   ← aggregates by country + year

mart/
├── mart_energy_overview.sql    ← Tile 1: GHG time series
└── mart_renewables_vs_fossil.sql ← Tile 2: energy mix comparison
```

---

## Dashboard

Built with **Looker Studio**, connected directly to BigQuery. The dashboard includes:

- **Filters**: Country selector and Year selector
- **KPI Scorecards**: Total GHG Emissions, Avg. Renewables Share, Avg. Carbon Intensity, Avg. Fossil Share
- **Tile 1**: Line chart — GHG emissions over time by country (`mart_energy_overview`)
- **Tile 2**: Stacked bar chart — Renewables vs Fossil fuel consumption by country (`mart_renewables_vs_fossil`)
-  Horizontal bar chart — Carbon intensity ranking by country
- Scatter plot — Fossil fuel consumption vs GHG emissions (correlation view)

[Image Looker.pdf](https://github.com/user-attachments/files/26788626/Image.Looker.pdf)

---

## Reproducibility

### Prerequisites

- GCP account with billing enabled
- Terraform installed (`brew install terraform`)
- Google Cloud SDK installed (`gcloud init`)
- Python 3.10+
- Docker Desktop
- Java 11+

## Dataset

**Source**: [Our World in Data — Energy Data](https://www.kaggle.com/datasets/bhavikjikadara/global-energy-electricity-and-emissions-data?utm_source=chatgpt.com>)

- ~23,000 rows, 130 columns
- Coverage: 200+ countries, 1900–2023
- Key metrics: energy consumption by source, GHG emissions, carbon intensity, renewables share, fossil fuel share, electricity generation

---

## Project Structure

```
energy-pipeline/
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars
├── kestra/
│   ├── docker-compose.yml
│   ├── flow.yml
│   └── gcp-key.json          
├── spark/
│   ├── jars/
│   │   ├── gcs-connector.jar
│   │   └── spark-bigquery-connector.jar
│   └── spark_transform.py
├── dbt/
│   └── energy_transform/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/
│           │   ├── sources.yml
│           │   └── stg_energy.sql
│           ├── intermediate/
│           │   └── int_energy_by_country.sql
│           └── mart/
│               ├── mart_energy_overview.sql
│               └── mart_renewables_vs_fossil.sql
├── data/
│   └── energy-data.csv
├── .gitignore
└── README.md
```

