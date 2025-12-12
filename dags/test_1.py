from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor


# ---------------------------------------------------------------------------
# DAG 1: Test_1 – Simple ETL-style TaskFlow
# ---------------------------------------------------------------------------

@dag(
    dag_id="Test_1",
    description="Test_1: Simple ETL-style TaskFlow DAG.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["test", "example"],
)
def test_1_dag():
    """
    Simple ETL-style DAG:
    - extract_data
    - transform_data
    - load_data
    """

    @task
    def extract_data():
        # In real life, this could query an API or database
        records = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]
        print(f"Extracted {len(records)} records")
        return records

    @task
    def transform_data(records: list[dict]):
        transformed = [
            {**r, "value": r["value"] * 2, "processed_at": datetime.utcnow().isoformat()}
            for r in records
        ]
        print(f"Transformed records: {transformed}")
        return transformed

    @task
    def load_data(transformed_records: list[dict]):
        # Replace this with write to S3/GCS/DB/etc.
        print(f"Loading {len(transformed_records)} records...")
        for r in transformed_records:
            print(f"Loaded record: {r}")
        return "load_complete"

    recs = extract_data()
    transformed = transform_data(recs)
    load_data(transformed)


test_1 = test_1_dag()
