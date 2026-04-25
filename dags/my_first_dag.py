from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow"
}

with DAG(
    "my_first_dag",
    default_args={
        "owner": "airflow",
        "retry_delay": timedelta(minutes=5),
    },
    description="My first DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 4, 23),
    catchup=False,
    tags=["example"],
) as dag:

    task_1 = BashOperator(
        task_id="task_1",
        bash_command="echo 'Hello Airflow!'"
    )

    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo 'Task 2 finalizes'"
    )

    task_1 >> task_2