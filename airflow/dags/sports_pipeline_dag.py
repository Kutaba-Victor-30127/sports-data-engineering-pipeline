from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task


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

    @task
    def upload_raw_batch():
        from src.aws.s3_client import upload_events_batch

        return upload_events_batch()

    @task
    def validate_json_batch(batch):
        from src.ingestion.load_json_events import load_json_events

        load_json_events(
            s3_key=batch["json_key"]
        )

    @task
    def load_batch_to_postgres(batch):
        from src.ingestion.load_events import (
            load_events,
            load_to_staging,
        )

        events = load_events(
            s3_key=batch["csv_key"],
            batch_id=batch["batch_id"],
        )

        load_to_staging(events)

    batch = upload_raw_batch()

    json_validation = validate_json_batch(batch)

    postgres_load = load_batch_to_postgres(batch)

    json_validation >> postgres_load

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "dbt build "
            "--project-dir sports_warehouse "
            "--profiles-dir airflow/dbt"
        ),
    )

    postgres_load >> dbt_build