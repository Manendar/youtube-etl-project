from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'youtube_etl_pipeline',
    default_args=default_args,
    description='Complete YouTube ETL Pipeline with Extract, Transform, Load, and Process',
    schedule_interval=None,
    catchup=False,
    tags=['youtube', 'etl', 'pyspark']
) as dag:
    create_hdfs_dirs = SSHOperator(
        task_id='create_hdfs_directories',
        ssh_conn_id='ssh_pyspark',
        command='cd /app && PYTHONPATH=/app python3 scripts/makedir.py'
    )
    extract_to_raw = SSHOperator(
        task_id='extract_raw_data',
        ssh_conn_id='ssh_pyspark',
        command='cd /app && PYTHONPATH=/app python3 scripts/extract/youtube_extract.py',
        cmd_timeout=600
    )

    create_staging = SSHOperator(
        task_id='create_staging_zone',
        ssh_conn_id='ssh_pyspark',
        command='cd /app && PYTHONPATH=/app python3 scripts/load/load_to_staging.py'
    )

    transform_data = SSHOperator(
        task_id='transform_clean_data',
        ssh_conn_id='ssh_pyspark',
        command='cd /app && PYTHONPATH=/app python3 scripts/transform/clean_youtube_data.py',
        cmd_timeout=600
    )

    create_processed = SSHOperator(
        task_id='create_processed_zone',
        ssh_conn_id='ssh_pyspark',
        command='cd /app && PYTHONPATH=/app python3 scripts/load/load_to_processed.py'
    )

    process_with_spark = SSHOperator(
        task_id='process_spark_analytics',
        ssh_conn_id='ssh_pyspark',
        command='cd /app && PYTHONPATH=/app python3 spark_jobs/process_data.py',
        cmd_timeout=600
    )

    # Define pipeline flow
create_hdfs_dirs >> extract_to_raw >> create_staging >> transform_data >> create_processed >> process_with_spark
