from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="financial_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["finance", "etl"]
) as dag:

    start = DummyOperator(task_id="start")

    run_glue_etl = DummyOperator(
        task_id="run_glue_financial_etl"
    )

    load_redshift = DummyOperator(
        task_id="load_data_to_redshift"
    )

    end = DummyOperator(task_id="end")

    # DAG flow
    start >> run_glue_etl >> load_redshift >> end
