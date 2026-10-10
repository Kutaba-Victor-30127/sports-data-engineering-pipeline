from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = "/opt/airflow/project"

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="sports_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["data-engineering", "aws", "dbt"],
) as dag:

    upload_raw_to_s3 = BashOperator(
        task_id="upload_raw_to_s3",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.aws.s3_client"
        ),
    )

    validate_json = BashOperator(
        task_id="validate_json",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.ingestion.load_json_events"
        ),
    )

    load_s3_to_postgres = BashOperator(
        task_id="load_s3_to_postgres",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.ingestion.load_events"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "dbt build "
            "--project-dir sports_warehouse "
            "--profiles-dir airflow/dbt"
        ),
    )

    (
        upload_raw_to_s3
        >> validate_json
        >> load_s3_to_postgres
        >> dbt_build
    )