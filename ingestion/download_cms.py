"""
Uses the CMS metastore API to obtain and download CSV files
API reference: https://data.cms.gov/provider-data/docs

Datasets used:
 - Hospital General Information
 - Timely and Effective Care
"""

import os, json, logging, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    Level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


CMS_metastore_base = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)

CMS_datasets = [
    {
        "name": "hospital_general_info",
        "dataset_id": "xubh-q36u",
        "filename": "hospital_general_info.csv",
        "description": "General hospital info: name, address, type, star rating",
    },
    {
        "name": "timely_effective_care",
        "dataset_id": "r4nx-7f4e",
        "filename": "timely_effective_care.csv",
        "description": "Timely and effective care measures for hospitals",
    }
]

local_data_dir = Path(os.getenv("LOCAL_DATA_DIR", "data/raw"))

def resolve_download_url(dataset_id: str) -> str | None:
    url = f"{CMS_metastore_base}/{dataset_id}?show-reference-ids=false"
    log.info(f"  Resolving download URL via metastore API...")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        metadata = response.json()

        distributions = metadata.get("distributions", [])
        if not distributions:
            log.error(f" No distribution found in metadata for {dataset_id  }")
            return None
        
        download_url = distributions[0].get("data", {}).get("downloadURL")
        if not download_url:
            log.error(f" No download URL found in distribution for {dataset_id}")
            return None
        
        log.info(f" Resolved: {download_url}")
        return download_url
    
    except requests.exceptions.HTTPError as e:
        log.error(f" HTTP error resolving metadat: {e}")
        return None
    except requests.exceptions.RequestException as e:
        log.error(f" Request error resolving metadata: {e}")
        return None
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        log.error(f" Unexpected API format: {e}")
        return None
    


