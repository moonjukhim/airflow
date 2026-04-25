"""
차시 4: Task와 Operator
- BashOperator: 쉘 명령, 환경변수, 외부 스크립트
- PythonOperator: 함수 실행, op_args/op_kwargs
- @task.bash 데코레이터
"""

import json
import os
from datetime import datetime

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task


def _generate_data(**context):
    """context에서 logical_date를 활용한 데이터 생성"""
    ds = context["ds"]
    data = {
        "date": ds,
        "users": [
            {"name": "Alice", "age": 30, "city": "Seoul"},
            {"name": "Bob", "age": 25, "city": "Busan"},
            {"name": "Charlie", "age": 35, "city": "Daegu"},
        ],
    }
    print(f"생성된 데이터 ({ds}): {json.dumps(data, ensure_ascii=False, indent=2)}")
    return data


def _calculate_stats(data_list, metric_name="default"):
    """op_args / op_kwargs 인자 전달 예제"""
    stats = {
        "metric": metric_name,
        "count": len(data_list),
        "mean": sum(data_list) / len(data_list),
        "min": min(data_list),
        "max": max(data_list),
    }
    print(f"통계 결과: {stats}")
    return stats


with DAG(
    dag_id="04_operators_practice",
    description="BashOperator·PythonOperator·@task.bash 실습",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch04"],
):
    # BashOperator: Jinja 템플릿으로 실행일자 활용
    bash_template = BashOperator(
        task_id="bash_with_template",
        bash_command='echo "실행일: {{ ds }}, 구간: {{ data_interval_start }} ~ {{ data_interval_end }}"',
    )

    # BashOperator: 환경변수 전달
    bash_env = BashOperator(
        task_id="bash_with_env",
        bash_command='echo "환경: $DEPLOY_ENV, 리전: $REGION"',
        env={"DEPLOY_ENV": "production", "REGION": "ap-northeast-2"},
    )

    # PythonOperator: context 활용
    python_ctx = PythonOperator(
        task_id="python_with_context",
        python_callable=_generate_data,
    )

    # PythonOperator: op_args + op_kwargs
    python_args = PythonOperator(
        task_id="python_with_args",
        python_callable=_calculate_stats,
        op_args=[[30, 25, 35, 40, 28]],
        op_kwargs={"metric_name": "user_age"},
    )

    # @task.bash 데코레이터: TaskFlow 스타일로 Bash 실행
    @task.bash
    def bash_decorator_example():
        return 'echo "TaskFlow 스타일 Bash: PID=$$, Date=$(date +%F)"'

    bash_template >> bash_env
    python_ctx >> python_args
    bash_decorator_example()
