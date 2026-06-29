import os, io, logging, boto3, psycopg2, psycopg2.extras
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

S3_BUCKET          = os.getenv("S3_BUCKET_NAME")
S3_PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "processed/")
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "cms_warehouse"),
    "user":     os.getenv("DB_USER", "cms_user"),
    "password": os.getenv("DB_PASSWORD", "cms_password"),
}

def read_csv_from_s3(key: str) -> pd.DataFrame:
    """ Reads a processed CSV from S3 into a DataFrame """
    log.info(f"Reading s3://{S3_BUCKET}/{key}")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = response["Body"].read()
        df = pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False)
        log.info(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")
        return df
    except ClientError as e:
        raise RuntimeError(f"Failed to read {key} from S3: {e}")
    
def load_dim_hospital(conn, df: pd.DataFrame) -> int:
    """ Loads the dim_hospital dimension table """
    log.info("Loading dim_hospital")

    column_map = {
        "facility_id":          "facility_id",
        "facility_name":        "facility_name",
        "address":              "address",
        "city":                 "city",
        "state":                "state",
        "zip_code":             "zip_code",
        "county_or_parish":     "county_or_parish",
        "phone_number":         "phone_number",
        "hospital_type":        "hospital_type",
        "hospital_ownership":   "hospital_ownership",
        "emergency_services":   "emergency_services",
        "hospital_overall_rating": "overall_rating",
    }

    available = {k: v for k, v in column_map.items() if k in df.columns}
    subset = df[list(available.keys())].copy()
    subset.columns = list(available.values())

    subset = subset.replace("", None)
    subset = subset.where(subset.notna(), None)

    # Creates bool for DB schema
    if "emergency_services" in subset.columns:
        subset["emergency_services"] = subset["emergency_services"].map(
            {"Yes": True, "No": False}
        )
    
    # Converts invalid values to "NaN" then "NaN" to "None"
    if "overall_rating" in subset.columns:
        subset["overall_rating"] = pd.to_numeric(
            subset["overall_rating"], errors="coerce"
        )
        subset["overall_rating"] = subset["overall_rating"].where(
            subset["overall_rating"].notna(), None
        )
    
    # Converts DF to Records where each row becomes a dict
    records = subset.to_dict("records")

    cols = list(subset.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != "facility_id"])

    # UPSERT to update existing rows
    sql = f"""
        INSERT INTO dim_hospital ({col_names})
        VALUES ({placeholders})
        ON CONFLICT (facility_id) DO UPDATE SET {updates}; 
    """

    # Execute SQL statements
    with conn.cursor() as cur:
        values = [
            tuple(
                None if (v is None or (isinstance(v, float) and pd.isna(v)))
                else v
                for v in record.values()
            )
            for record in records
        ]
        psycopg2.extras.execute_batch(cur, sql, values, page_size=500)

    conn.commit()
    log.info(f"Upserted {len(records):,} rows into dim_hospital")
    return len(records)

def load_fact_quality_measures(conn, df: pd.DataFrame) -> int:
    """ Loads hospital infromation from DF into table in PostgreSQL """
    log.info("Loading fact_quality_measures")

    column_map = {
        "facility_id":      "facility_id",
        "condition":        "condition",
        "measure_id":       "measure_id",
        "measure_name":     "measure_name",
        "score":            "score",
        "sample":           "sample",
        "footnote":         "footnote",
        "start_date":       "start_date",
        "end_date":         "end_date",
        "facility_id_valid": "facility_id_valid",
    }

    # Keeps on mapped columns and match DF column names to DB schema
    available = {k: v for k, v in column_map.items() if k in df.columns}
    subset = df[list(available.keys())].copy()
    subset.columns = list(available.values())

    with conn.cursor() as cur:
        cur.execute("SELECT facility_id FROM dim_hospital;")
        valid_ids = {row[0] for row in cur.fetchall()}

    before = len(subset)
    subset = subset[subset["facility_id"].isin(valid_ids)]
    dropped = before - len(subset)
    if dropped > 0:
        log.warning(f"Drppped {dropped:,} rows wwith facility_id not in dim_hospital")

    subset = subset.replace("", None)
    subset = subset.where(subset.notna(), None)

    for col in ["score", "sample"]:
        if col in subset.columns:
            subset[col] = pd.to_numeric(subset[col], errors="coerce")
            subset[col] = subset[col].where(subset[col].notna(), None)
    
    records = subset.to_dict("records")
    cols = list(subset.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)

    sql = f"INSERT INTO fact_quality_measures ({col_names}) VALUES ({placeholders});"
    
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE fact_quality_measures RESTART IDENTITY;")
        log.info(" Truncated fact_quality_measures")

        values = [
            tuple(
                None if (v is None or (isinstance(v, float) and pd.isna(v)))
                else v
                for v in record.values()
            )
            for record in records
        ]
        psycopg2.extras.execute_batch(cur, sql, values, page_size=500)
    
    conn.commit()
    log.info(f"Inserted {len(records):,} rows into fact_quality_measures")
    return len(records)

def verify_load(conn) -> None:
    """ Prints row counts and checks validity after loading """
    log.info("Verifying load...")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dim_hospital;")
        hospital_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM fact_quality_measures;")
        fact_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM fact_quality_measures WHERE score IS NOT NULL;")
        scored_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT f.facility_id)
            FROM fact_quality_measures f
            JOIN dim_hospital h ON f.facility_id = h.facility_id;
        """)
        joined_count = cur.fetchone()[0]

    log.info(f"  dim_hospital rows:           {hospital_count:,}")
    log.info(f"  fact_quality_measures rows:  {fact_count:,}")
    log.info(f"  Fact rows with score:        {scored_count:,}")
    log.info(f"  Hospitals with measures:     {joined_count:,}")
    assert hospital_count > 0, "dim_hospital is empty!"
    assert fact_count > 0,     "fact_quality_measures is empty!"
    assert joined_count > 0,   "No successful dim/fact join — FK issue?"
    log.info("  All checks passed.")

def run_load() -> None:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET_NAME not set in .env")
    log.info("Starting load")

    # Loads cleaned files from S3
    hospital_df = read_csv_from_s3(f"{S3_PROCESSED_PREFIX}hospital_general_info.csv")
    care_df     = read_csv_from_s3(f"{S3_PROCESSED_PREFIX}timely_effective_care.csv") 

    # Connect to Postgres
    log.info("Connecting to PostgreSQL")
    conn = psycopg2.connect(**DB_CONFIG)
    log.info(f"Connected to {DB_CONFIG['dbname']} on {DB_CONFIG['host']}")

    try:
        load_dim_hospital(conn, hospital_df)
        load_fact_quality_measures(conn, care_df)
        verify_load(conn)

    finally:
        conn.close()

    log.info("Warehouse loaded")

if __name__ == "__main__":
    run_load()