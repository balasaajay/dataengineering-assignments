import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from datetime import datetime, timedelta
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
  """ Download trip data from GCS to BigQuery"""
  os.makedirs(f"data/{color}", exist_ok = True)
  gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
  gcs_block = GcsBucket.load("gcs-bucket-name")
  gcs_block.get_directory(from_path=gcs_path, local_path=f".")
  return Path(f"{gcs_path}")  

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
  """ Data cleaning """
  df = pd.read_parquet(path)
  # print(f"PRE: Missing Passenger count: {df['passenger_count'].isna().sum()}")
  # df['passenger_count'].fillna(0, inplace=True)
  # print(f"POST: Missing Passenger count: {df['passenger_count'].isna().sum()}")
  print(f"length: {len(df)}")
  return df

@task(retries=3, log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
  """ Write dataframe to BQ"""
  gcp_credentials_block = GcpCredentials.load("gcp-sa-creds")
  df.to_gbq(
    destination_table = "trips_data_all.external_table_homework",
    project_id = "de-zoomcamp-ga",
    credentials = gcp_credentials_block.get_credentials_from_service_account(),
    chunksize = 500000,
    if_exists = 'append',
  )

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
  """ Main ETL flow to load data to BQ"""

  path = extract_from_gcs(color, year, month)
  df = transform(path)
  write_bq(df)

@flow()
def etl_parent_flow(
  months: list[int] = [1,2], year: int = 2021, color: str = "yellow"
) -> None:
  for month in months:
    etl_gcs_to_bq(year, month, color)

if __name__ == '__main__':
  color = "yellow"
  months = [2,3]
  year = 2019
  etl_gcs_to_bq(months, year, color)