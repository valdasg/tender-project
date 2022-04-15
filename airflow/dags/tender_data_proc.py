from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from google.cloud import storage



import pandas as pd
from datetime import datetime
import os
import requests
from zipfile import ZipFile
import glob
import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow.models import DAG

# Dataset variables
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
COUNTRY_LIST_URL = 'https://www.eea.europa.eu/data-and-maps/data/waterbase-lakes-4/country-codes-and-abbreviations-32-records/country-codes-and-abbreviations-32-records/at_download/file'

# Google cloud variables
BUCKET_NAME = 'dl-eu-pub-tender'

# Google Dataproc variables
CLUSTER_NAME = 'spark-temp-cluster'
REGION = 'europe-west6'
PROJECT_ID = 'eu-pub-tender'
PYSPARK_URI='gs://dl-eu-pub-tender/code/spark_to_bq.py' 

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    }
}

# Do not forget jar file location as it will cause 'can not find bigquery' error
# Also, use correct scala version
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar"]
    },
}

# Python callable functions
def create_data_links():
    df_country_abrvs = pd.read_csv(f'{AIRFLOW_HOME}/data/countries_abr.csv', index_col='ISO2')
    # Loop trough abreviations ad it to file params
    
    for index, row in df_country_abrvs.iterrows():
        # Define target url
        TARGET_URL = f'https://opentender.eu/data/files/data-{index.lower()}-csv.zip'
               
        
        #  Save to path
        
        r = requests.get(TARGET_URL)
        if r.ok:
            LOCAL_CSV_DIR = AIRFLOW_HOME + '/data/country_data/' + index.lower()
            if not os.path.exists(f'{LOCAL_CSV_DIR}'):
            # Create folder if it does not exist
                os.makedirs(LOCAL_CSV_DIR)  
            filename = f'{index}.zip'  
            file_path = os.path.join(LOCAL_CSV_DIR, filename)  
                           
            print("saving to", os.path.abspath(file_path))
            with open(file_path, 'wb') as f:
                f.write(r.content)
                    
            # Unzip
            print(f'Extracting...  {filename}')
            with ZipFile(f'{LOCAL_CSV_DIR}/{index}.zip', 'r') as zipObj:
                    zipObj.extractall(f'{LOCAL_CSV_DIR}')
            # Remove zip file
            os.remove(f'{LOCAL_CSV_DIR}/{index}.zip')
     

def upload_to_gcs(local_path: str, bucket: str, gcs_path: str):
    GCS_CLIENT = storage.Client()
    rel_paths = glob.glob(local_path +'/**', recursive=True)
    bucket = GCS_CLIENT.get_bucket(bucket)
    for local_file in rel_paths:

        remote_path = f'{gcs_path}/{"/".join(local_file.split(os.sep)[1:])}'
        
        if os.path.isfile(local_file):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_file) 
 
# Instantiate DAG

default_args = {
    'start_date': datetime(2020,1,1)
}

# Instantiate DAG

default_args = {
    'start_date': datetime(2020,1,1)
}

with DAG (
    dag_id = 'eu_proc_processing',
    schedule_interval = '@daily',
    default_args = default_args,
    catchup = False
) as dag:
    # Define task operators
    check_abr_availability = HttpSensor (
        task_id = 'check_abr_availability',
        http_conn_id = 'abbr_page',
        endpoint = '/at_download/file'
        )
    
    check_data_availability = HttpSensor (
        task_id = 'check_data_availability',
        http_conn_id = 'data_page',
        endpoint = '/'
        )
    
    download_abbrevs = BashOperator (
        task_id = 'download_abbrevs',
        bash_command = f"curl -sSLf {COUNTRY_LIST_URL} > {AIRFLOW_HOME}/data/countries_abr.csv",
        trigger_rule = 'all_success'        
    )
    
    download_data = PythonOperator (
        task_id = 'download_data',
        python_callable = create_data_links        
    )
    
    upload_files = PythonOperator(
        task_id="upload_files_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            'local_path': f'data/',
            'bucket': f'{BUCKET_NAME}',
            'gcs_path': f'raw_data'            
        },
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_transform_laod", 
        job= PYSPARK_JOB, 
        location=REGION, 
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    
    
[check_abr_availability, check_data_availability] >> download_abbrevs >> download_data >> upload_files >> create_cluster >> submit_job >> delete_cluster
