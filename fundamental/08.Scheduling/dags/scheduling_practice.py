"""
차시 8: 스케줄링 기초
- cron 표현식
- schedule 파라미터
- data_interval / logical_date 개념

Airflow의 스케줄링은 "데이터 구간"을 기준으로 동작합니다.
DAG Run은 data_interval이 끝난 후에 실행됩니다.
예: schedule="@daily" → 2025-01-01 데이터는 2025-01-02 00:00에 실행
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _show_schedule_info(**context):
    """스케줄 관련 주요 변수를 확인"""
    print("=== 스케줄 정보 ===")
    print(f"logical_date      : {context['logical_date']}")
    print(f"data_interval_start: {context['data_interval_start']}")
    print(f"data_interval_end  : {context['data_interval_end']}")
    print()
    print("logical_date = data_interval_start = 처리 대상 데이터 구간의 시작")
    print("data_interval_end = 처리 대상 데이터 구간의 끝")
    print("실제 실행 시각은 data_interval_end 이후입니다.")


# === 1) 스케줄 없음 (수동 실행만) ===
with DAG(
    dag_id="08a_no_schedule",
    description="수동 실행만 가능한 DAG",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch08"],
):
    PythonOperator(
        task_id="show_info",
        python_callable=_show_schedule_info,
    )


# === 2) 매일 자정 실행 (cron 표현식) ===
with DAG(
    dag_id="08b_daily_cron",
    description="매일 자정(UTC) 실행 - cron 표현식",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 0 * * *",  # 분 시 일 월 요일
    catchup=False,
    tags=["exercise", "ch08"],
):
    BashOperator(
        task_id="daily_job",
        bash_command='echo "일일 배치: {{ data_interval_start | ds }} ~ {{ data_interval_end | ds }}"',
    )


# === 3) 프리셋 스케줄 ===
with DAG(
    dag_id="08c_hourly_preset",
    description="매시간 실행 - 프리셋 사용",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="@hourly",  # = "0 * * * *"
    catchup=False,
    tags=["exercise", "ch08"],
):
    BashOperator(
        task_id="hourly_job",
        bash_command='echo "시간별 처리: {{ data_interval_start }} ~ {{ data_interval_end }}"',
    )


# === 4) 주 1회 (월요일 오전 9시 KST) ===
with DAG(
    dag_id="08d_weekly_monday",
    description="매주 월요일 09:00 KST 실행",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 0 * * 1",  # UTC 00:00 월요일 = KST 09:00 월요일
    catchup=False,
    tags=["exercise", "ch08"],
):
    PythonOperator(
        task_id="weekly_report",
        python_callable=_show_schedule_info,
    )
