from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
from etl.src.extract_fixtures import extract_fixtures
from etl.src.transform_fixtures import transform_fixtures
from etl.src.load_fixtures import load_to_db


default_args ={
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id= "fixtures_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["fixtures_etl"]
) as dag:
    
    extraction_task = PythonOperator(
        task_id="extract_fixtures",
        python_callable= extract_fixtures,

    )

    transformation_task = PythonOperator(
        task_id="transform_fixtures",
        python_callable=transform_fixtures
    )

    load_task = PythonOperator(
        task_id="load_fixtures",
        python_callable=load_to_db,
        op_kwargs={'parquet_file': './data/cleaned_fixtures.parquet'}

    )

    dbt_run = DockerOperator(
        task_id= "run_dbt",
        image= "football_pipeline-dbt:latest",
        api_version= "auto",
        auto_remove= True,
        mount_tmp_dir=False,
        network_mode= "etl_network",
        mounts= [
            Mount(
                source="/Users/ayomideabass/Documents/Projects/football_stats_project/dbt/football_pipeline",
                target="/opt/dbt", 
                type="bind",
                read_only=False
            ),
            Mount(
                source="/Users/ayomideabass/.dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind",
                read_only=True
            ),
            Mount(
                source="/Users/ayomideabass/Documents/Projects/football_stats_project/dbt/football_pipeline/logs",
                target="/opt/dbt/logs",
                type="bind",
                read_only=False,
            ),  
        ],
        environment = {"DBT_PROFILES_DIR": "/root/.dbt"},
        working_dir="/opt/dbt",
        command= ["dbt", "run", "--profiles-dir", "/root/.dbt"]
    )


    extraction_task >> transformation_task >> load_task >> dbt_run

