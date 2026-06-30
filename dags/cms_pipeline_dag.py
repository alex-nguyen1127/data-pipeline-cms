"""
cms_pipeline_dag.py

Apache Airflow DAG for the CMS Hospital Quality Data Pipeline.

This DAG orchestrates the full end-to-end pipeline on a weekly schedule:

  download_cms_data
        │
        ▼
  upload_to_s3_raw
        │
        ▼
  transform_and_validate
        │
        ▼
  load_warehouse
        │
        ▼
  verify_warehouse

Each task is a PythonOperator that calls the same functions used in the
standalone scripts — no logic is duplicated.

To view in Airflow UI:
  http://localhost:8080  (admin / admin)
  DAG name: cms_hospital_quality_pipeline
"""

import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Make our project modules importable inside Airflow's container
sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)

# ── Default task arguments ─────────────────────────────────────────────────────
# These apply to every task unless overridden at the task level.

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,          # don't wait for previous run to succeed
    "retries":          2,              # retry failed tasks twice
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,          # set to True and add email config for alerts
    "email_on_retry":   False,
}

# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="cms_hospital_quality_pipeline",
    description="Weekly ETL: download CMS hospital data → S3 → transform → PostgreSQL",
    default_args=default_args,
    # Run every Sunday at 6am UTC — CMS releases updates quarterly,
    # so weekly is frequent enough to catch new data without overloading the API
    schedule="0 6 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,           # don't backfill missed runs
    max_active_runs=1,       # only one pipeline run at a time
    tags=["cms", "healthcare", "etl"],
) as dag:

    # ── Task 1: Download CMS datasets ──────────────────────────────────────────
    def task_download_cms(**context):
        """
        Downloads CMS datasets from data.cms.gov to local disk.
        Skips files that already exist (idempotent).
        """
        from ingestion.download_cms import download_cms_datasets

        log.info("Task: download_cms_data")
        results = download_cms_datasets()

        failed = [name for name, path in results.items() if path is None]
        if failed:
            raise RuntimeError(f"Download failed for datasets: {failed}")

        log.info(f"Downloaded {len(results)} datasets successfully.")
        # Push file paths to XCom so downstream tasks can reference them
        return {name: str(path) for name, path in results.items()}

    download_task = PythonOperator(
        task_id="download_cms_data",
        python_callable=task_download_cms,
    )

    # ── Task 2: Upload raw files to S3 ────────────────────────────────────────
    def task_upload_s3(**context):
        """
        Uploads locally downloaded raw files to the S3 raw zone.
        """
        from ingestion.upload_to_s3 import upload_raw_files

        log.info("Task: upload_to_s3_raw")
        results = upload_raw_files()

        failed = [name for name, ok in results.items() if not ok]
        if failed:
            raise RuntimeError(f"S3 upload failed for: {failed}")

        log.info(f"Uploaded {len(results)} files to S3 raw zone.")

    upload_task = PythonOperator(
        task_id="upload_to_s3_raw",
        python_callable=task_upload_s3,
    )

    # ── Task 3: Transform and validate ────────────────────────────────────────
    def task_transform(**context):
        """
        Reads raw CSVs from S3, cleans and validates them,
        and writes processed files back to S3.
        """
        from transform.clean_hospital import run_transform

        log.info("Task: transform_and_validate")
        run_transform()
        log.info("Transform complete.")

    transform_task = PythonOperator(
        task_id="transform_and_validate",
        python_callable=task_transform,
    )

    # ── Task 4: Load warehouse ─────────────────────────────────────────────────
    def task_load(**context):
        """
        Reads processed CSVs from S3 and loads them into PostgreSQL.
        dim_hospital is upserted; fact_quality_measures is truncated and reloaded.
        """
        from warehouse.load_warehouse import run_load

        log.info("Task: load_warehouse")
        run_load()
        log.info("Warehouse load complete.")

    load_task = PythonOperator(
        task_id="load_warehouse",
        python_callable=task_load,
    )

    # ── Task 5: Verify warehouse ───────────────────────────────────────────────
    def task_verify(**context):
        """
        Runs post-load checks to confirm the warehouse has data
        and the dim/fact join works correctly.
        Raises on failure so Airflow marks the run as failed.
        """
        import psycopg2

        log.info("Task: verify_warehouse")

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME", "cms_warehouse"),
            user=os.getenv("DB_USER", "cms_user"),
            password=os.getenv("DB_PASSWORD", "cms_password"),
        )

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dim_hospital;")
                hospital_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM fact_quality_measures;")
                fact_count = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(DISTINCT f.facility_id)
                    FROM fact_quality_measures f
                    JOIN dim_hospital h ON f.facility_id = h.facility_id;
                """)
                joined_count = cur.fetchone()[0]

            log.info(f"dim_hospital rows:          {hospital_count:,}")
            log.info(f"fact_quality_measures rows: {fact_count:,}")
            log.info(f"Hospitals with measures:    {joined_count:,}")

            # Hard assertions — if these fail, the DAG run fails
            assert hospital_count > 5000, \
                f"Expected >5000 hospitals, got {hospital_count}"
            assert fact_count > 100000, \
                f"Expected >100000 fact rows, got {fact_count}"
            assert joined_count > 4000, \
                f"Expected >4000 joined hospitals, got {joined_count}"

            log.info("All verification checks passed.")

        finally:
            conn.close()

    verify_task = PythonOperator(
        task_id="verify_warehouse",
        python_callable=task_verify,
    )

    # ── Task dependencies (pipeline order) ────────────────────────────────────
    #
    #  download_cms_data
    #        │
    #        ▼
    #  upload_to_s3_raw
    #        │
    #        ▼
    #  transform_and_validate
    #        │
    #        ▼
    #  load_warehouse
    #        │
    #        ▼
    #  verify_warehouse

    download_task >> upload_task >> transform_task >> load_task >> verify_task