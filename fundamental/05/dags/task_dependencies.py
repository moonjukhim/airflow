"""
차시 5: Task 의존성과 실행 흐름
- >> 연산자와 chain() 함수
- 병렬(fan-out) / 합류(fan-in) / 다이아몬드 패턴
- cross_downstream 활용
"""

import pendulum
from airflow.models.baseoperator import chain, cross_downstream
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task


def _log_step(step_name, **context):
    ds = context["ds"]
    print(f"[{ds}] {step_name} 실행 완료")


with DAG(
    dag_id="05_task_dependencies",
    description="의존성 패턴: >>, chain(), cross_downstream()",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch05"],
):
    start = EmptyOperator(task_id="start")

    # 패턴 1: Fan-out (1 → N)
    fetch_api = PythonOperator(
        task_id="fetch_api",
        python_callable=_log_step,
        op_kwargs={"step_name": "API 데이터 수집"},
    )
    fetch_db = PythonOperator(
        task_id="fetch_db",
        python_callable=_log_step,
        op_kwargs={"step_name": "DB 데이터 수집"},
    )
    fetch_file = PythonOperator(
        task_id="fetch_file",
        python_callable=_log_step,
        op_kwargs={"step_name": "파일 데이터 수집"},
    )

    # 패턴 2: Fan-in (N → 1)
    merge = EmptyOperator(task_id="merge_all")

    # 패턴 3: chain()으로 순차 연결
    validate = EmptyOperator(task_id="validate")
    report = EmptyOperator(task_id="report")
    end = EmptyOperator(task_id="end")

    # >> 연산자: 리스트로 병렬 지정
    start >> [fetch_api, fetch_db, fetch_file] >> merge

    # chain(): 순차 체인을 간결하게
    chain(merge, validate, report, end)

    # cross_downstream: 모든 upstream × 모든 downstream 조합
    # 예: cleanup_a, cleanup_b 모두 완료 후 → notify_a, notify_b 모두 실행
    cleanup_a = EmptyOperator(task_id="cleanup_a")
    cleanup_b = EmptyOperator(task_id="cleanup_b")
    notify_a = EmptyOperator(task_id="notify_a")
    notify_b = EmptyOperator(task_id="notify_b")

    end >> [cleanup_a, cleanup_b]
    cross_downstream([cleanup_a, cleanup_b], [notify_a, notify_b])
