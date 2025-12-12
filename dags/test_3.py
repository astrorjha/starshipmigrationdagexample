from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# ---------------------------------------------------------------------------
# DAG 3: Test_3 – Sensor + Dynamic Task Mapping (TaskFlow)
# ---------------------------------------------------------------------------

@dag(
    dag_id="Test_3",
    description="Test_3: Uses TimeDeltaSensor and dynamic task mapping.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["test", "sensor", "mapped"],
)
def test_3_dag():
    """
    DAG that:
    - waits a short time via TimeDeltaSensor (simulated waiting)
    - dynamically processes a list of items
    - summarizes mapped task results
    """

    # Lightweight, environment-agnostic sensor
    wait_for_window = TimeDeltaSensor(
        task_id="wait_for_window",
        delta=timedelta(seconds=30),  # Fake "wait until something is ready"
        mode="reschedule",
    )

    @task
    def build_item_list() -> list[int]:
        items = [1, 2, 3, 4, 5]
        print(f"Built item list: {items}")
        return items

    @task
    def process_item(item: int) -> dict:
        result = {
            "item": item,
            "square": item * item,
            "processed_at": datetime.utcnow().isoformat(),
        }
        print(f"Processed: {result}")
        return result

    @task
    def summarize_results(results: list[dict]):
        print("Summary of mapped task results:")
        for r in results:
            print(r)
        return {"count": len(results)}

    items = build_item_list()
    processed = process_item.expand(item=items)  # dynamic task mapping
    summary = summarize_results(processed)

    wait_for_window >> items
    # `items` is an XCom-arg task; we want processing to happen after both
    items >> processed >> summary


test_3 = test_3_dag()