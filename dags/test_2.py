from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# ---------------------------------------------------------------------------
# DAG 2: Test_2 – Branching, BashOperator, PythonOperator
# ---------------------------------------------------------------------------

@dag(
    dag_id="Test_2",
    description="Test_2: Demonstrates branching and mixed operator types.",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["test", "branching"],
)
def test_2_dag():
    """
    DAG with:
    - initial Python task to compute a simple metric
    - branch based on metric
    - different paths with BashOperator
    - a join task at the end
    """

    def compute_metric_callable(**context):
        # Dummy metric: current minute (0-59)
        metric = datetime.utcnow().minute
        print(f"Computed metric: {metric}")
        # Push to XCom explicitly (optional; Airflow also auto-pushes return value)
        context["ti"].xcom_push(key="metric", value=metric)
        return metric

    compute_metric = PythonOperator(
        task_id="compute_metric",
        python_callable=compute_metric_callable,
    )

    def branch_on_metric_callable(**context):
        metric = context["ti"].xcom_pull(
            task_ids="compute_metric", key="metric"
        )
        print(f"Branching on metric: {metric}")
        # Simple rule: even → even_path, odd → odd_path
        if metric % 2 == 0:
            return "even_path"
        else:
            return "odd_path"

    branch = BranchPythonOperator(
        task_id="branch_on_metric",
        python_callable=branch_on_metric_callable,
    )

    even_path = BashOperator(
        task_id="even_path",
        bash_command="echo 'Metric was even; running even_path tasks.'",
    )

    odd_path = BashOperator(
        task_id="odd_path",
        bash_command="echo 'Metric was odd; running odd_path tasks.'",
    )

    def join_callable(**context):
        print("Join task running after branch path completes.")

    join = PythonOperator(
        task_id="join",
        python_callable=join_callable,
        trigger_rule="none_failed_min_one_success",
    )

    compute_metric >> branch
    branch >> [even_path, odd_path] >> join


test_2 = test_2_dag()