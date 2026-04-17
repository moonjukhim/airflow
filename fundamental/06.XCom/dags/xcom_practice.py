"""
차시 6: XCom - Task 간 데이터 전달
- return 자동 push (key='return_value')
- 명시적 xcom_push / xcom_pull (커스텀 key)
- 딕셔너리 전달 및 여러 Task의 XCom 조합
- XComArgs: TaskFlow API 스타일의 암묵적 XCom 전달
- XCom 크기 제한 주의 (메타데이터 DB에 저장됨)
"""

import json
import random
from datetime import datetime

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task


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

    # === 방법 4: XComArgs (TaskFlow API) ===
    # @task 데코레이터가 반환값을 XCom에 자동 push하고,
    # 함수 호출 결과(XComArg)를 다른 task의 인자로 넘기면
    # Airflow가 의존성과 xcom_pull을 자동으로 처리한다.
    @task
    def fetch_prices() -> dict:
        prices = {"AAPL": 190.5, "GOOG": 142.3, "MSFT": 415.2}
        print(f"가격 조회: {prices}")
        return prices

    @task
    def fetch_volumes() -> dict:
        volumes = {"AAPL": 52_000_000, "GOOG": 18_000_000, "MSFT": 22_000_000}
        print(f"거래량 조회: {volumes}")
        return volumes

    @task
    def compute_market_cap(prices: dict, volumes: dict) -> dict:
        # XComArg가 실제 값(dict)으로 해제되어 전달된다.
        caps = {sym: prices[sym] * volumes[sym] for sym in prices}
        print(f"시가총액 계산: {caps}")
        return caps

    @task
    def summarize(caps: dict, top_symbol: str) -> None:
        # XComArg 인덱싱: caps["AAPL"] 처럼 dict key 접근도 지원된다.
        print(f"최대 종목: {top_symbol} / 전체: {caps}")

    prices_xarg = fetch_prices()
    volumes_xarg = fetch_volumes()
    caps_xarg = compute_market_cap(prices_xarg, volumes_xarg)

    # XComArg 인덱싱 예시 — caps_xarg["AAPL"] 자체도 XComArg
    summarize(caps_xarg, top_symbol=caps_xarg["AAPL"])

    # 전통 Operator와 혼합: classic operator의 .output 속성도 XComArg
    @task
    def count_orders(orders: list) -> int:
        return len(orders)

    count_orders(extract.output)
