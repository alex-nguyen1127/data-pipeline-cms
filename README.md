# CMS Hospital Quality Data Pipeline

An end-to-end data engineering pipeline that ingests public CMS (Centers for Medicare & Medicaid Services) hospital quality data, transforms and validates it, loads it into a PostgreSQL data warehouse, and runs on an automated weekly schedule via Apache Airflow.

## Datasets Used

| Dataset | Source | Format | Rows |
|---|---|---|---|
| Hospital General Information | data.cms.gov | CSV | ~5,000 |
| Timely and Effective Care | data.cms.gov | CSV | ~70,000 |

All datasets are publicly available from [data.cms.gov](https://data.cms.gov).

## Architecture

```
data.cms.gov  (public API)
        │
        ▼
  Python Ingestion
        │
        ▼
  AWS S3 (raw zone)
        │
        ▼
  Python Transform (Pandas)
        │
        ▼
  AWS S3 (processed zone)
        │
        ▼
  PostgreSQL Data Warehouse
```

All steps are orchestrated by **Apache Airflow** and version-controlled on GitHub.


### Phase 1: Extraction
Scripts run:
- ingestion/download_cms.py
- ingestion/upload_to_s3.py

Accomplished:  
- Ingested raw data from live government API using dynamic URL resolution
- Uploaded raw data into S3
- Structured logging, error handling

### Phase 2:
Scripts run:
- clean_hospital.py

Accomplished:
- Read raw data from S3 and write Pandas DataFrames to S3
- Clean both datasets by normalizing, striping white space, replacing nulls, etc.
- Uploaded cleaned data into S3

### Phase 3:
Scripts run:
- warehouse/create_schema.py
- warehouse/load_warehouse.py

Accomplished:
- Created a PostgreSQL Schema
- Implemented an S3-to-Postgres ETL pipeline
- Post Load verification step for row count assertions

### Phase 4:
Ran:
- docker-compose up -d (spins up everything the pipeline needs to run.)
- docker-compose run --rm airflow-init

Accomplished:
- Defined all services (Postgres, Airflow webserver, airflow scheduler)
- Apache Airflow runs inside Docker automatically executing pipeline on schedule (Sunday at 6am)
- Docker Compose managing entire infrastructure
    - Trigger DAG in http://localhost:8080/home