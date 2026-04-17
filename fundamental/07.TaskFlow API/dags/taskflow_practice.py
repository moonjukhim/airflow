"""
차시 7: TaskFlow API
- @task 데코레이터: XCom 자동 push/pull
- 기존 Operator와 혼합 사용
- ETL 패턴 (extract → load → transform) 예제
"""

import random

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task


@task(task_id="extract_from_source")
def extract():
    """소스 시스템에서 데이터 추출 시뮬레이션"""
    rows = random.randint(1000, 5000)
    result = {
        "rows": rows,
        "source_table": "order_tb",
    }
    print(f"추출 완료: {result}")
    return result


@task(task_id="load_to_stage")
def load(extract_meta: dict):
    """스테이징 영역에 적재"""
    print(f"소스: {extract_meta['source_table']}, {extract_meta['rows']}건")
    load_result = {
        "schema": "stage",
        "table": "stage_order_tb",
        "rows": extract_meta["rows"],
    }
    print(f"스테이지 적재 완료: {load_result}")
    return load_result


@task(task_id="transform_in_dw")
def transform(load_meta: dict):
    """DW에서 변환 (fact 테이블 생성)"""
    print(f"변환 입력: {load_meta['schema']}.{load_meta['table']}")
    final = {
        "schema": "dw",
        "table": "order_fact",
        "rows": load_meta["rows"],
    }
    print(f"DW 변환 완료: {final}")
    return final


@task(task_id="generate_summary")
def generate_summary(transform_meta: dict):
    """변환 결과 요약"""
    print(f"=== 파이프라인 요약 ===")
    print(f"최종 테이블: {transform_meta['schema']}.{transform_meta['table']}")
    print(f"처리 건수: {transform_meta['rows']:,}건")
    return f"{transform_meta['schema']}.{transform_meta['table']}: {transform_meta['rows']}건"


with DAG(
    dag_id="07_taskflow_practice",
    description="TaskFlow API: ELT 패턴 + 기존 Operator 혼합",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch07"],
):
    # TaskFlow 체이닝: return → 다음 함수 인자로 자동 전달
    raw = extract()
    staged = load(raw)
    transformed = transform(staged)
    summary = generate_summary(transformed)

    # 기존 Operator와 혼합: Jinja 템플릿으로 XCom 참조
    notify = BashOperator(
        task_id="notify",
        bash_command=(
            'echo "파이프라인 완료: '
            "{{ ti.xcom_pull(task_ids='generate_summary') }}\""
        ),
    )

    summary >> notify
