"""
차시 10: Catchup & Backfill
- catchup 동작 원리
- backfill 명령어 실습
- 언제 켜고 꺼야 하나

catchup=True (기본값): start_date부터 현재까지 밀린 모든 DAG Run을 자동 실행
catchup=False: 가장 최근 스케줄만 실행

backfill CLI: 특정 기간의 DAG Run을 수동으로 실행
  airflow dags backfill -s 2025-01-01 -e 2025-01-07 <dag_id>
"""

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _process_daily_data(**context):
    """날짜별 데이터 처리를 시뮬레이션"""
    logical = context["logical_date"]
    ds = logical.format("YYYY-MM-DD")
    interval_start = context["data_interval_start"].format("YYYY-MM-DD HH:mm")
    interval_end = context["data_interval_end"].format("YYYY-MM-DD HH:mm")

    print(f"=== {ds} 데이터 처리 ===")
    print(f"  데이터 구간: {interval_start} ~ {interval_end}")
    print(f"  처리 내용: {ds} 일자의 이벤트 로그를 집계합니다.")


# === 1) catchup=True: 밀린 실행을 자동으로 채움 ===
with DAG(
    dag_id="10a_catchup_enabled",
    description="catchup=True - 과거 실행을 모두 채우는 DAG",
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=True,    # start_date부터 오늘까지 모든 DAG Run 생성
    end_date=pendulum.datetime(2025, 3, 5, tz="Asia/Seoul"),  # 실습용 제한
    tags=["exercise", "ch10"],
):
    PythonOperator(
        task_id="process_data",
        python_callable=_process_daily_data,
    )


# === 2) catchup=False: 최신 스케줄만 실행 ===
with DAG(
    dag_id="10b_catchup_disabled",
    description="catchup=False - 최신 스케줄만 실행하는 DAG",
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,   # 과거 실행 건너뜀, 최신 1건만 실행
    tags=["exercise", "ch10"],
):
    PythonOperator(
        task_id="process_data",
        python_callable=_process_daily_data,
    )


# === 사용 가이드 ===
# catchup=True 를 써야 할 때:
#   - 과거 데이터를 날짜별로 처리해야 할 때 (일별 집계, 로그 처리)
#   - 파이프라인이 멱등성(idempotent)을 보장할 때
#
# catchup=False 를 써야 할 때:
#   - 최신 데이터만 필요할 때 (실시간 알림, 모니터링)
#   - 과거 데이터를 다시 처리하면 문제가 생길 때 (중복 이메일 발송 등)
#
# Backfill 명령어 (CLI에서 실행):
#   airflow dags backfill -s 2025-01-01 -e 2025-01-07 10b_catchup_disabled
#   → catchup=False 인 DAG도 특정 기간만 수동으로 실행 가능
