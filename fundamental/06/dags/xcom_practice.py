"""
차시 6: XCom - Task 간 데이터 전달
- return 자동 push (key='return_value')
- 명시적 xcom_push / xcom_pull (커스텀 key)
- 딕셔너리 전달 및 여러 Task의 XCom 조합
- XCom 크기 제한 주의 (메타데이터 DB에 저장됨)
"""

import json
import random
from datetime import datetime

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


# === 방법 1: return 자동 push ===
def _extract_orders():
    orders = [
        {"id": i, "amount": random.randint(10000, 100000)}
        for i in range(1, 6)
    ]
    print(f"추출: {len(orders)}건")
    return orders


# === 방법 2: 명시적 push (여러 key) ===
def _generate_metrics(**context):
    ti = context["ti"]
    ti.xcom_push(key="total_sales", value=1500000)
    ti.xcom_push(key="avg_order", value=35000)
    ti.xcom_push(key="order_date", value=context["ds"])
    print("매출 지표 3개를 XCom에 push 완료")


# === 방법 3: 여러 Task의 XCom을 pull ===
def _build_report(**context):
    ti = context["ti"]

    # return_value key는 task_ids만 지정
    orders = ti.xcom_pull(task_ids="extract_orders")

    # 명시적 key는 key 파라미터 필요
    total = ti.xcom_pull(task_ids="generate_metrics", key="total_sales")
    avg = ti.xcom_pull(task_ids="generate_metrics", key="avg_order")
    order_date = ti.xcom_pull(task_ids="generate_metrics", key="order_date")

    print(f"=== {order_date} 일일 리포트 ===")
    print(f"주문 건수: {len(orders)}건")
    print(f"총 매출: {total:,}원")
    print(f"평균 주문액: {avg:,}원")
    print(f"주문 상세: {json.dumps(orders, ensure_ascii=False)}")


with DAG(
    dag_id="06_xcom_practice",
    description="XCom: return 자동 push, 명시적 push/pull, 다중 key",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch06"],
):
    extract = PythonOperator(
        task_id="extract_orders",
        python_callable=_extract_orders,
    )

    metrics = PythonOperator(
        task_id="generate_metrics",
        python_callable=_generate_metrics,
    )

    report = PythonOperator(
        task_id="build_report",
        python_callable=_build_report,
    )

    [extract, metrics] >> report
