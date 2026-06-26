import os, logging
from pathlib import Path
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

""" Config AWS from env """
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
LOCAL_DATA_DIR = Path(os.getenv("LOCAL_DATA_DIR", "data/raw"))

UPLOAD_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client("s3", region_name=AWS_REGION)

def upload_file_to_s3(s3_client, local_path: Path, s3_key: str, metadata: dict) -> bool:
    """ Uploads a single local file to S3 """
    log.info(f"Uploading {local_path} to s3://{S3_BUCKET}/{s3_key}")

    try: 
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=S3_BUCKET,
            Key=s3_key,
            ExtraArgs={
                "Metadata": metadata,
                "ServerSideEncryption": "AES256",
            },
        )
        log.info(f"Successfully uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")
        return True
    
    except ClientError as e:
        log.error(f"Failed to upload {local_path} to S3: {e}")
        return False
    except FileNotFoundError:
        log.error(f"Local file not found: {local_path}")
        return False
    

def upload_raw_files() -> dict:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET_NAME environment variable is not set.")
    
    log.info(f"Target bucket: {S3_BUCKET}, prefix: {S3_RAW_PREFIX} in region: {AWS_REGION}  ")

    try:
        s3 = get_s3_client()
        s3.head_bucket(Bucket=S3_BUCKET)
        log.info(f"Successfully connected to S3 bucket: {S3_BUCKET}")

    except NoCredentialsError:
        raise RuntimeError("AWS credentials not found. Please configure your AWS credentials.") 
    except ClientError as e:
        raise RuntimeError(f"Cannot access bucket '{S3_BUCKET}': {e}")
    
    files = list(LOCAL_DATA_DIR.glob("*.csv"))
    if not files:
        log.warning(f"No CSV files found in {LOCAL_DATA_DIR}. Run download_cms.py script first")
        return {}
    
    log.info(f"Files to upload: {len(files)}")
    results={}

    for local_path in files:
        s3_key = f"{S3_RAW_PREFIX}{local_path.name}"

        metadata = {
            "source": "data.cms.gov",
            "pipelione": "cms-hospital-quality",
            "zone": "raw",
            "uploaded-at": UPLOAD_TIMESTAMP,
            "original-filename": local_path.name
        }

        success = upload_file_to_s3(s3, local_path, s3_key, metadata)
        results[local_path.name] = success

    succeeded = sum(results.values())
    log.info(f"Upoad compete: {succeeded}/{len(files)} files uploaded to S3")

    return results
    
def list_raw_files(s3_client) -> None:
    """ Lists the files currently stored in S3 raw path """
    log.info(f"Listing files in s3://{S3_BUCKET}/{S3_RAW_PREFIX}")
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_RAW_PREFIX)

    if "Contents" not in response:
        log.info(" No files found")
        return
    
    for obj in response["Contents"]:
        size_kb = obj["Size"] / 1024
        log.info(f" {obj['Key']} ({size_kb:.1f} KB)] modified: {obj['LastModified']}")


if __name__ == "__main__":
    results = upload_raw_files()

    failed = [name for name, ok in results.items() if not ok]

    if failed:
        log.error(f"Failed uploads: {failed}")
        raise SystemExit
    

    s3= get_s3_client()
    list_raw_files(s3)
    log.info("Extraction complete. Raw files are in S3")