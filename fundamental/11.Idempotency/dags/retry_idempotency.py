"""
차시 11: Retry와 Idempotency 설계
- 자동 재시도 설정 (retries, retry_delay)
- 멱등성(Idempotency)이란?
- 중복 실행에 안전한 파이프라인 설계

멱등성: 같은 작업을 여러 번 실행해도 결과가 동일한 성질.
Airflow에서 Task가 재시도되거나, DAG를 다시 실행해도 데이터가 꼬이지 않아야 합니다.
"""

import os
import random
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


# === 비멱등적 (나쁜 예) ===
def _append_data_bad():
    """비멱등적: 실행할 때마다 데이터가 중복 추가됨"""
    filepath = "/tmp/airflow_bad_example.txt"
    with open(filepath, "a") as f:  # "a" = append 모드
        f.write("new_record\n")

    with open(filepath) as f:
        lines = f.readlines()
    print(f"[비멱등] 현재 레코드 수: {len(lines)}건")
    print("재실행할 때마다 레코드가 계속 늘어납니다!")


# === 멱등적 (좋은 예) ===
def _write_data_good(**context):
    """멱등적: 날짜별 파일을 덮어쓰기 → 몇 번 실행해도 결과 동일"""
    ds = context["logical_date"].format("YYYY-MM-DD")
    filepath = f"/tmp/airflow_good_{ds}.txt"
    with open(filepath, "w") as f:  # "w" = write 모드 (덮어쓰기)
        f.write(f"date={ds}\n")
        f.write("record_count=100\n")
    print(f"[멱등적] {filepath} 에 데이터 저장 완료")
    print("몇 번 실행해도 같은 파일에 같은 내용이 저장됩니다.")


# === 재시도를 시뮬레이션하는 불안정한 Task ===
def _unreliable_api_call():
    """50% 확률로 실패하는 API 호출 시뮬레이션"""
    if random.random() < 0.5:
        raise Exception("API 호출 실패! 서버가 응답하지 않습니다.")
    print("API 호출 성공!")


def _cleanup_on_retry(**context):
    """재시도 전 정리 작업"""
    ti = context["ti"]
    attempt = ti.try_number
    print(f"현재 시도 횟수: {attempt}")
    if attempt > 1:
        print("이전 시도의 임시 파일을 정리합니다...")
        tmp_file = "/tmp/airflow_partial_result.txt"
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
            print("임시 파일 삭제 완료")


with DAG(
    dag_id="11_retry_idempotency",
    description="Retry 설정과 멱등성 설계 실습",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch11"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
):
    # Task 1: 비멱등적 예제 (나쁜 패턴)
    bad_example = PythonOperator(
        task_id="bad_not_idempotent",
        python_callable=_append_data_bad,
    )

    # Task 2: 멱등적 예제 (좋은 패턴)
    good_example = PythonOperator(
        task_id="good_idempotent",
        python_callable=_write_data_good,
    )

    # Task 3: 불안정한 API → 자동 재시도
    unreliable = PythonOperator(
        task_id="unreliable_api",
        python_callable=_unreliable_api_call,
        retries=5,                           # Task 수준 재시도 설정
        retry_delay=timedelta(seconds=5),    # 재시도 간격
    )

    # Task 4: 재시도 시 정리 작업
    with_cleanup = PythonOperator(
        task_id="with_retry_cleanup",
        python_callable=_cleanup_on_retry,
    )

    # Task 5: SQL 멱등성 패턴 시뮬레이션
    idempotent_sql = BashOperator(
        task_id="idempotent_sql_pattern",
        bash_command=(
            'echo "=== 멱등적 SQL 패턴 ===" && '
            'echo "" && '
            'echo "나쁜 예: INSERT INTO sales VALUES (...);" && '
            'echo "  → 재실행 시 중복 데이터 발생" && '
            'echo "" && '
            'echo "좋은 예 1: DELETE + INSERT (날짜 기준)" && '
            'echo "  DELETE FROM sales WHERE date = \'{{ ds }}\';" && '
            'echo "  INSERT INTO sales SELECT ... WHERE date = \'{{ ds }}\';" && '
            'echo "" && '
            'echo "좋은 예 2: INSERT ... ON CONFLICT UPDATE (UPSERT)" && '
            'echo "  INSERT INTO sales VALUES (...) ON CONFLICT (id) DO UPDATE SET ...;" && '
            'echo "" && '
            'echo "좋은 예 3: CREATE OR REPLACE (테이블/뷰)" && '
            'echo "  CREATE TABLE IF NOT EXISTS tmp_{{ ds_nodash }} AS SELECT ...;"'
        ),
    )

    [bad_example, good_example] >> idempotent_sql
    unreliable >> with_cleanup


#########################################################
# # Airflow Task에서 호출 예제
# import requests
# import uuid
# 
# def call_payment_api(**context):
#     ds = context["logical_date"].format("YYYY-MM-DD")
# 
#     # 💡 핵심: deterministic key
#     idempotency_key = f"payment_{ds}"
# 
#     payload = {
#         "order_id": f"order_{ds}",
#         "amount": 100
#     }
# 
#     response = requests.post(
#         "http://localhost:8000/payment",
#         json=payload,
#         headers={
#             "Idempotency-Key": idempotency_key
#         },
#         timeout=5
#     )
# 
#     print(response.json())