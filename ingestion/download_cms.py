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
    log.info(f"  Resolving download URL via metastore API...")
    url = f"{CMS_metastore_base}/{dataset_id}?show-reference-ids=false"

    try:
        """ 
        Get metadata from the url API and raise error if the request failed. 
        If successful, convert JSON response to dictionary
        """
        response = requests.get(url, timeout=30) 
        response.raise_for_status() 
        metadata = response.json()


        distributions = metadata.get("distributions", [])
        if not distributions:
            log.error(f" No distribution found in metadata for {dataset_id  }")
            return None
        
        """ 
        Extract the download URL from the data object in the first distribution.
        """
        download_url = distributions[0].get("data", {}).get("downloadURL")
        if not download_url:
            log.error(f" No download URL found in distribution for {dataset_id}")
            return None
        
        """
        Success case, return url.
        """
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
    """
    Handles HTTP errors
    DNS failures, Connection refused, Timeouts and SSL Errors
    Bad JSON and Unstructured structures
    """

def download_file(url: str, destination: Path) -> bool:
    log.info(f" Downloading fille...")

    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        destination.parent.mkdir(parents=True, exist_ok=True)

        """ Downloads file in chunks """
        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        """Calculate file size """
        file_size_mb = destination.stat().st_size / (1024 * 1024)
        log.info(f" Saved: {destination} ({file_size_mb:.2f} MB)")
        return True
    
    except requests.exceptions.HTTPError as e:
        log.error(f" HTTP error downloading file: {e}")
        return False
    except requests.exceptions.ConnectionError as e:
        log.error(f" Connection error: {e}")
        return False
    except requests.exceptions.Timeout as e:
        log.error(f" Timeout error: {e}")
        return False
    """
    Handles HTTP errors
    Connection errors
    Request errors
    """

def download_cms_datasets() -> dict:
    results = {}
    log.info(f"Starting CMS dataset download -> {local_data_dir}")
    log.info(f"Datasets to download: {len(CMS_datasets)}")

    for dataset in CMS_datasets:
        log.info(f"Processing: {dataset['name']} ({dataset['description']})")
        destination = local_data_dir / dataset["filename"]

        """ Skips if aready downloaded"""
        if destination.exists():
            size_mb = destination.stat().st_size / (1024 * 1024)
            log.info(f" Already exists, skipping ({size_mb:.2f} MB)")
            results[dataset["name"]] = destination
            continue

        """ Find the current download url via the metastore API """
        url = resolve_download_url(dataset["description"])
        if url is None:
            results[dataset["name"]] = None
            continue
        
        """ Download CSV """
        success = download_file(url, destination)
        results[dataset["name"]] = destination if success else None

    succeeded = sum(1 for v in results.values() if v is not None)
    log.info(f"Download compete: {succeeded}/{len(CMS_datasets)} succeeded")
    return results
                    
if __name__ == "__main__":
    results = download_cms_datasets()
    
    failed = [name for name, path in results.items() if path is None]
    if failed:
        log.error(f"Failed to download datasets: {failed}")
        raise SystemExit(1)
    
    log.info("All datasets downloaded successfully.")

