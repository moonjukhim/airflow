"""
차시 3: DAG란 무엇인가
- DAG(Directed Acyclic Graph) 구조 이해
- with문 vs @dag 데코레이터 두 가지 정의 방법
- default_args와 DAG 수준 설정
"""

from datetime import timedelta

import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _process():
    result = sum(range(1, 101))
    print(f"1부터 100까지의 합: {result}")
    return result


# === 방법 1: with 문 (권장) ===
with DAG(
    dag_id="03a_my_first_dag",
    description="with문으로 DAG 정의",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["exercise", "ch03"],
):
    start = EmptyOperator(task_id="start")

    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    end = EmptyOperator(task_id="end")

    start >> process >> end


# === 방법 2: @dag 데코레이터 ===
from airflow.sdk import dag as dag_decorator, task


@dag_decorator(
    dag_id="03b_dag_decorator",
    description="@dag 데코레이터로 DAG 정의",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["exercise", "ch03"],
)
def dag_with_decorator():
    @task
    def extract():
        print("데이터 추출")
        return {"rows": 100}

    @task
    def load(data: dict):
        print(f"데이터 적재: {data['rows']}건")

    load(extract())


dag_with_decorator()
