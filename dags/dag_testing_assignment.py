import logging
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def extract_data(**context):
    """
    Extract data from source (simulated).
    Returns list of dictionaries.
    """
    try:
        data = [
            {"name": "apple", "price": 1000},
            {"name": "banana", "price": 500},
        ]

        logging.info("Extracted data: %s", data)

        # push ke XCom
        context["ti"].xcom_push(key="raw_data", value=data)

    except Exception as e:
        logging.error("Error in extract_data: %s", e)
        raise

def transform_data(data):
    """
    Transform extracted data.
    Example: uppercase product name.
    """
    if not data:
        return []

    transformed = []
    for item in data:
        transformed.append({
            "name": item["name"].upper(),
            "price": item["price"],
        })

    return transformed

def transform_task_callable(**context):
    """
    Airflow task wrapper for transform_data.
    Pull data from XCom, transform, push back.
    """
    try:
        ti = context["ti"]

        raw_data = ti.xcom_pull(
            task_ids="extract_task",
            key="raw_data"
        )

        if raw_data is None:
            raise ValueError("No data received from extract_task")

        transformed_data = transform_data(raw_data)

        ti.xcom_push(
            key="transformed_data",
            value=transformed_data
        )

    except Exception as e:
        logging.error("Error in transform_task: %s", e)
        raise

def load_data(data):
    """
    Simulate loading data to destination.
    """
    if not data:
        raise ValueError("No data to load")

    for item in data:
        logging.info("Loading item: %s", item)

def load_task_callable(**context):
    """
    Airflow task wrapper for load_data.
    """
    try:
        ti = context["ti"]

        transformed_data = ti.xcom_pull(
            task_ids="transform_task",
            key="transformed_data"
        )

        if transformed_data is None:
            raise ValueError("No data received from transform_task")

        load_data(transformed_data)

    except Exception as e:
        logging.error("Error in load_task: %s", e)
        raise

with DAG(
    dag_id="data_validation_dag",
    default_args=default_args,
    description="DAG for testing and validation assignment",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 22),
    catchup=False,
    tags=["testing", "validation", "dag-testing"],
) as dag:

    start = EmptyOperator(task_id="start")
    extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract_data,
)
    transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transform_task_callable,
)
    load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_task_callable,
)
    end = EmptyOperator(task_id="end")

    start >> extract_task >> transform_task >> load_task >> end

