import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from datetime import datetime, timedelta


# @task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), log_prints=True)
@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
  """ Read data from web to pandas df """

  # Experiment failed scenarios
  # if randint(0, 1) > 0:
  #   raise Exception
  print(f"Downloading: {dataset_url}")
  df = pd.read_csv(dataset_url)
  return df

@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
  """Fix datatye issues """

  # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
  # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
  df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
  df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
  print(df.head(2))
  print(f"columns: {df.dtypes} ")
  print(f"length: {len(df)}")
  return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
  """ write dataframe out as parquet file"""
  os.makedirs(f"data/{color}", exist_ok = True)
  path=Path(f"data/{color}/{dataset_file}.parquet")
  df.to_parquet(path, compression="gzip")
  print(f"Path: {path}")
  return path

@task()
def write_gcs(path: Path) -> None:
  """ Write parquet file to gcs"""
  gcs_block = GcsBucket.load("gcs-bucket-name")
  gcs_block.upload_from_path(from_path=path, to_path=path)


@flow()
def etl_web_to_gcs() -> None:
  """ The main etl function
  """ 
  color = "yellow"
  year = 2019
  month = 3
  dataset_file = f"{color}_tripdata_{year}-{month:02}"
  dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
  
  df = fetch(dataset_url)
  df = clean_data(df)
  path = write_local(df, color, dataset_file)
  write_gcs(path)
   

if __name__ == '__main__':
  etl_web_to_gcs()