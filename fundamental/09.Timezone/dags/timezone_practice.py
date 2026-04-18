"""
차시 9: Timezone과 실행 시각의 함정
- UTC vs KST (Asia/Seoul = UTC+9)
- logical_date vs 실제 실행 시각
- 흔한 실수 패턴

핵심: Airflow 내부는 모두 UTC. 한국 시간으로 생각하면 9시간 차이로 혼란이 생김.
logical_date는 "처리할 데이터의 시점"이지, "DAG이 실행되는 시점"이 아닙니다.
"""

from datetime import datetime, timezone

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _show_timezone_trap(**context):
    """시간대 관련 흔한 혼란을 직접 확인"""
    logical = context["logical_date"]
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]
    now_utc = datetime.now(timezone.utc)

    print("=" * 60)
    print("1) logical_date (= data_interval_start)")
    print(f"   UTC : {logical}")
    print(f"   KST : {logical.in_timezone('Asia/Seoul')}")
    print()
    print("2) data_interval")
    print(f"   start: {interval_start}")
    print(f"   end  : {interval_end}")
    print()
    print("3) 실제 실행 시각 (지금)")
    print(f"   UTC : {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("=" * 60)
    print("[흔한 실수 패턴]")
    print()
    print("실수 1: logical_date를 '실행 시각'으로 착각")
    print("  → logical_date는 '처리할 데이터 구간의 시작'입니다.")
    print("  → @daily DAG: 1/2에 실행되면 logical_date는 1/1")
    print()
    print("실수 2: KST 기준으로 스케줄을 설정")
    print("  → schedule='0 9 * * *' 은 UTC 09:00 = KST 18:00")
    print("  → KST 09:00에 실행하려면 schedule='0 0 * * *' (UTC 00:00)")
    print()
    print("실수 3: datetime.now()로 날짜 필터링")
    print("  → 항상 context의 logical_date를 사용하세요.")
    print("  → 재실행/백필 시 datetime.now()는 잘못된 날짜를 반환합니다.")


def _safe_date_usage(**context):
    """올바른 날짜 사용법"""
    logical = context["logical_date"]

    ds = logical.format("YYYY-MM-DD")
    ds_kst = logical.in_timezone("Asia/Seoul").format("YYYY-MM-DD")

    print("=== 올바른 날짜 사용 ===")
    print(f"SQL WHERE: date = '{ds}'")
    print(f"파일 경로: /data/{ds}/events.csv")
    print(f"KST 기준 날짜가 필요하면: {ds_kst}")
    print()
    print("Jinja 템플릿에서:")
    print("  {{ ds }}                    → YYYY-MM-DD (UTC)")
    print("  {{ logical_date }}          → 전체 datetime 객체")
    print("  {{ data_interval_start }}   → 구간 시작")
    print("  {{ data_interval_end }}     → 구간 끝")


# DAG의 start_date에 timezone을 명시하면 스케줄도 해당 TZ 기준으로 동작
with DAG(
    dag_id="09_timezone_practice",
    description="UTC vs KST 시간대 함정 실습",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 15 * * *",  # UTC 15:00 = KST 00:00 (자정)
    catchup=False,
    tags=["exercise", "ch09"],
):
    show_trap = PythonOperator(
        task_id="show_timezone_trap",
        python_callable=_show_timezone_trap,
    )

    safe_usage = PythonOperator(
        task_id="safe_date_usage",
        python_callable=_safe_date_usage,
    )

    show_trap >> safe_usage
