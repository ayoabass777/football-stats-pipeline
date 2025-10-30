from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
from etl.src.update_fixtures import update_fixtures_main
from etl.src.transform_fixtures import transform_fixtures
from etl.src.load_updates import load_played_updates





default_args ={
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fixture_update_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["fixture_update"]
) as dag:
    
    update_fixtures_task = PythonOperator(
        task_id="update_fixtures",
        python_callable=update_fixtures_main,
    )

    transform_fixtures_task = PythonOperator(
        task_id= "transform_fixtures",
        python_callable=transform_fixtures,
        op_kwargs={"is_update": True},
    )

    load_played_updates_task = PythonOperator(
        task_id="load_played_updates",
        python_callable=load_played_updates,
    )

    dbt_run = DockerOperator(
        task_id= "incremental_run_dbt",
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
        environment={"DBT_PROFILES_DIR": "/root/.dbt"},
        working_dir="/opt/dbt",
        command= ["dbt", "run", "--profiles-dir", "/root/.dbt"]
    )

    update_fixtures_task >> transform_fixtures_task >> load_played_updates_task >> dbt_run




