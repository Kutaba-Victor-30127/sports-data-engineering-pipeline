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
    def prepare_batch():
        from src.aws.s3_client import (
            create_batch_metadata,
        )

        return create_batch_metadata()

    
    @task
    def ingest_batch(batch):
        from src.ingestion.run_batch import (
            run_batch,
        )

        return run_batch(
            batch=batch,
            upload_needed=True,
        )

    batch = prepare_batch()

    ingestion = ingest_batch(
        batch
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_ROOT} && " 
            "dbt build "
            "--project-dir sports_warehouse "
            "--profiles-dir airflow/dbt  "
        ),
    )

    ingestion >> dbt_build