import requests
import os
import logging
import polars as pl
from boto3 import session
from botocore.client import Config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

url = os.getenv("URL")
DO_URL = os.getenv("DO_URL")
ACCESS_ID = os.getenv("DO_ACCESS_ID")
SECRET_KEY = os.getenv("DO_SECRET_KEY")
session = session.Session(region_name="lon1")


def upload_to_dospace(url: str, df: pl.DataFrame, bucket_name: str, object_name: str) -> None:
    """Upload raw JSON data directly to DigitalOcean Space

    Args:
        url (str): The URL to extract the raw JSON data
        data (dict): The raw JSON data to upload
        bucket_name (str): Name of the DigitalOcean Space bucket
        object_name (str): Object name in the bucket
    """
    try:
        requests.get(url)
        logging.info("API endpoint is up")
    except requests.exceptions.RequestException as e:
        logging.error("API endpoint is down")
        raise SystemExit(e)

    try:
        data = requests.get(url).json()
        logging.info("Data extracted successfully")
    except requests.exceptions.RequestException:
        logging.error("Data extraction failed")
        raise

    try:
        # Upload JSON data to DigitalOcean Space
        upload = session.client(
            "s3",
            region_name="lon1",
            endpoint_url=DO_URL,
            aws_access_key_id=ACCESS_ID,
            aws_secret_access_key=SECRET_KEY,
        )
        upload.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=data,
            ContentType="application/json",
        )
        logging.info(f"Data uploaded to DigitalOcean Space: {bucket_name}/{object_name}")
    except Exception as e:
        logging.error("Data upload to DigitalOcean Space failed")
        raise SystemExit(e)


if __name__ == "__main__":
    df = upload_to_dospace(url, "tmp", "my-object")
