import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = f'{os.environ.get("AIRFLOW_HOME", "/opt/airflow/")}/'

YELLOW_URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'
YELLOW_FILE_NAME_TEMPLATE = 'zones_tripdata_{{execution_date.strftime(\'%Y-%m\')}}'
YELLOW_URL_TEMPLATE = YELLOW_URL_PREFIX + f'{YELLOW_FILE_NAME_TEMPLATE}.csv'
YELLOW_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f'{YELLOW_FILE_NAME_TEMPLATE}'
YELLOW_GCS_PATH_TEMPLATE = "raw/zones_tripdata/{{execution_date.strftime(\'%Y\')}}/" +f"{YELLOW_FILE_NAME_TEMPLATE}.parquet"

FHV_URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
FHV_FILE_NAME_TEMPLATE = 'fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}'
FHV_URL_TEMPLATE = FHV_URL_PREFIX + f'{FHV_FILE_NAME_TEMPLATE}.csv'
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f'{FHV_FILE_NAME_TEMPLATE}'
FHV_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{execution_date.strftime(\'%Y\')}}/" +f"{FHV_FILE_NAME_TEMPLATE}.parquet"

ZONES_URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
ZONES_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + 'taxi_zone_lookup'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zones/taxi_zone_lookup.parquet"

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def download_parquetized_upload_data(
    dag,
    url_template,
    local_path_template,
    gcs_path_template
):
    with dag:

        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_path_template}.csv"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{local_path_template}.csv",
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": f"{local_path_template}.parquet",
            },
        )

        delete_files_task = BashOperator(
            task_id="detete_files_task",
            bash_command=f"rm {local_path_template}.parquet {local_path_template}.csv"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> delete_files_task


default_args = {
    "owner": "airflow",
    "start_date": "2019-01-01",
    "depends_on_past": False,
    "retries": 1,
}
dag_zones = DAG(
    dag_id="zones_data_ingestion_gcs_dag_v01",
    schedule_interval="@once",
    default_args=default_args,
    catchup=True,
    max_active_runs=4,
    tags=['dtc-de'],
)

download_parquetized_upload_data(dag_zones,
    ZONES_URL_TEMPLATE,
    ZONES_OUTPUT_FILE_TEMPLATE,
    ZONES_GCS_PATH_TEMPLATE
)
