import os, io, logging, boto3
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

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw/")
S3_PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "processed/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

CMS_NULL_VALUES = {"Not Available", "Not Applicable", "N/A", "-", ""}

def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client("s3", region_name=AWS_REGION)

def read_csv_from_s3(s3_client, key:str) -> pd.DataFrame:
    """ Turns csv from S3 to a Pandas DataFrame """
    log.info(f"Reading s3://{S3_BUCKET}/{key}")
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        body = response["Body"].read()
        df = pd.read_csv(
            io.BytesIO(body),
            dtype=str,
            keep_default_na=False,
        )
        log.info(f"Loaded {len(df):,} rows × {len(df.columns):,} columns")
        return df
    except ClientError as e:
        raise RuntimeError(f"Failed to read s3://{S3_BUCKET}/{key}: {e}")
    

def write_csv_to_s3(s3_client, df: pd.DataFrame, key:str) -> None:
    """ Writes Pandas DataFrame to S3 as a csv file """
    log.info(f"Writing {len(df):,} rows → s3://{S3_BUCKET}/{key}")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ServerSideEncryption="AES256",
        Metadata={"pipeline": "cms-hospital-quality", "zone": "processed"},
    )
    log.info(f"Write successful")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ Converts column names to snake_case """
    df.columns=(
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-/]+", "_", regex=True)   # spaces/dashes → underscore
        .str.replace(r"[^\w]", "", regex=True)        # remove non-word chars
        .str.replace(r"_+", "_", regex=True)          # collapse double underscores
    )
    return df

def strip_ws_strings(df: pd.DataFrame) -> pd.DataFrame:
    """ Strips leading/trailing whitespace from all string columns """
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    return df

def replace_cms_nulls(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """ Replace null strings with 'None/NaN' """
    df[columns] = df[columns].replace(list(CMS_NULL_VALUES), None) 
    return df

def log_null_report(df: pd.DataFrame, label:str) -> pd.DataFrame:
    """ Logs null counts for every column """
    log.info(f"Null report - {label}:")
    for col in df.columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            pct = null_count / len(df) * 100
            log.info(f"{col}: {null_count:,} nulls ({pct:.1f}%)")


def clean_hospital_general_info(df: pd.DataFrame) -> pd.DataFrame:
    """ Cleans the Hospital General Information dataset """
    log.info("Cleaning: hospital_general_info")
    original_rows = len(df)

    df = normalize_columns(df)
    df = strip_ws_strings(df)

    str_cols = df.select_dtypes(include="object").columns.tolist()
    df = replace_cms_nulls(df, str_cols)

    if "zip_code" in df.columns:
        df["zip_code"] = (
            df["zip_code"]
            .astype(str)
            .str.strip()
            .str.zfill(5)
            .replace("00nan", None)
        )

    if "hospital_overall_rating" in df.columns:
        df["hospital_overall_rating"] = pd.to_numeric(
            df["hospital_overall_rating"], errors="coerce"
        ).astype("Int64")


    assert len(df) == original_rows, "Row count changed during transformationm - unexpected"
    assert "facility_id" in df.columns, "facility_id column missing after transform"
    assert df["facility_id"].isna().sum() == 0, "facility_id has unexpected nulls"

    log_null_report(df, "hospital_general_info")
    log.info(f"  Clean complete: {len(df):,} rows")
    return df

def clean_timely_effective_care(df: pd.DataFrame) -> pd.DataFrame:
    """ Cleans the Timely and Effective Care dataset """
    log.info("Cleaning: timely_effective.csv")
    original_rows = len(df)

    df = normalize_columns(df)
    df = strip_ws_strings(df)

    sentinel_cols = [c for c in df.columns if c in (
        "score", "sample", "footnote", "condition", "measure_name"
    )]
    df = replace_cms_nulls(df, sentinel_cols)

    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    
    if "sample" in df.columns:
        df["sample"] = pd.to_numeric(
            df["sample"], errors="coerce"
        ).astype("Int64")
    
    if "facility_id" in df.columns:
        df["facility_id_valid"] = df["facility_id"].str.match(r"^\d{6}$", na=False)
        invalid_count = (~df["facility_id_valid"]).sum()
        if invalid_count > 0:
            log.warning(f"{invalid_count:,} rows have nonstandard facility_id values")

    assert len(df) == original_rows, "Row count changed during transform — unexpected!"
    assert "facility_id" in df.columns, "facility_id column missing after transform"

    log_null_report(df, "timely_effective_care")
    log.info(f"  Clean complete: {len(df):,} rows")
    return df

def run_transform() -> None:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET_NAME not set in .env")
    
    s3 = get_s3_client()

    transforms = [
        {
            "raw_key": f"{S3_RAW_PREFIX}hospital_general_info.csv",
            "processed_key": f"{S3_PROCESSED_PREFIX}hospital_general_info.csv",
            "clean_fn": clean_hospital_general_info
        },
        {
            "raw_key": f"{S3_RAW_PREFIX}timely_effective_care.csv",
            "processed_key": f"{S3_PROCESSED_PREFIX}timely_effective_care.csv",
            "clean_fn": clean_timely_effective_care
        },
    ]

    log.info(f"  Source:      s3://{S3_BUCKET}/{S3_RAW_PREFIX}")
    log.info(f"  Destination: s3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}")

    for t in transforms:
        log.info(f"{'-' * 60}")
        raw_df = read_csv_from_s3(s3, t["raw_key"])
        clean_df = t["clean_fn"](raw_df)
        write_csv_to_s3(s3, clean_df, t["processed_key"])

    log.info(f"{'-' * 60}")
    log.info("Transformed and processed files are in S3")

if __name__ == "__main__":
    run_transform()