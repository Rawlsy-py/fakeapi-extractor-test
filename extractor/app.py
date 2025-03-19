import requests
import os
import logging
import polars as pl

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

url = os.getenv("URL")


def extractor(url: str) -> pl.DataFrame:
    """extract data from API endpoint

    Args:
        url (str): Url of the API endpoint

    Returns:
        pl.DataFrame: polars dataframe, nested elements will remain as structs
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
        df = pl.DataFrame(data)
        logging.info("Data converted to polars dataframe")
    except Exception as e:
        logging.error("Data conversion to polars dataframe failed")
        raise SystemExit(e)
    return df


def loader(df: pl.DataFrame, path: str) -> None:
    """load data to parquet file

    Args:
        df (pl.DataFrame): polars dataframe
        path (str): path to save the parquet file
    """
    try:
        df.write_parquet(path)
        logging.info(f"Data saved to {path}")
    except Exception as e:
        logging.error("Data saving failed")
        raise SystemExit(e)


if __name__ == "__main__":
    df = extractor(url)
    loader(df, "data.parquet")
    
