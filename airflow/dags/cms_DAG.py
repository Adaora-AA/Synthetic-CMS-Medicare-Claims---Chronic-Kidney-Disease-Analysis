from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from cms_upload import main as upload_to_gcs
#from scripts.cms_upload import main as upload_to_gcs
from cms_pipeline import main as create_bq_tables_from_gcs
#from scripts.cms_pipeline import main as create_bq_tables_from_gcs


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "cms_ingestion_stream",
    default_args=default_args,
    schedule= "0 0 L * *",  # Runs at 00:00 on the last day of every month
    start_date=datetime(2025, 2, 23),
    end_date=datetime(2025, 4, 26),
    catchup=False,
    max_active_runs=1,
)


# Task 1: Scrap links from cms website, download files and upload to GCS
cms_download_upload = PythonOperator(
    task_id="upload_to_gcs_task",
    python_callable=upload_to_gcs,  
    execution_timeout=timedelta(minutes=60), 
    retries=3,  
    retry_delay=timedelta(minutes=5),    
    dag=dag
)

# Task 2: Create BigQuery tables from GCS Parquet files
create_bq_tables_task = PythonOperator(
    task_id="create_bq_tables_from_gcs_task",
    python_callable=create_bq_tables_from_gcs,
    execution_timeout=timedelta(minutes=60), 
    retries=3,  
    retry_delay=timedelta(minutes=5),    
    dag=dag
)


cms_download_upload >> create_bq_tables_task  
