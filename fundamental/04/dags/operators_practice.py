"""
차시 4: Task와 Operator
- Task의 역할: DAG 안에서 실행되는 작업의 단위
- BashOperator: 쉘 명령 실행
- PythonOperator: Python 함수 실행

Operator는 Task의 "무엇을 할 것인가"를 정의합니다.
Task는 Operator의 인스턴스(실제 실행 단위)입니다.
"""

import json
from datetime import datetime

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


# === PythonOperator에서 사용할 함수들 ===

def _generate_data():
    """데이터 생성 함수"""
    data = {
        "timestamp": datetime.now().isoformat(),
        "users": [
            {"name": "Alice", "age": 30, "city": "Seoul"},
            {"name": "Bob", "age": 25, "city": "Busan"},
            {"name": "Charlie", "age": 35, "city": "Daegu"},
        ],
    }
    print(f"생성된 데이터: {json.dumps(data, ensure_ascii=False, indent=2)}")
    return data


def _calculate_stats():
    """통계 계산 함수 - 인자를 받는 예제"""
    ages = [30, 25, 35]
    stats = {
        "count": len(ages),
        "mean": sum(ages) / len(ages),
        "min": min(ages),
        "max": max(ages),
    }
    print(f"통계 결과: {stats}")


def _greet(name, greeting="안녕하세요"):
    """op_args / op_kwargs 사용 예제"""
    print(f"{greeting}, {name}님! Airflow 실습에 오신 것을 환영합니다.")


with DAG(
    dag_id="04_operators_practice",
    description="BashOperator와 PythonOperator 실습",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch04"],
):
    # === BashOperator 예제 ===

    # 1) 단순 명령 실행
    bash_simple = BashOperator(
        task_id="bash_simple",
        bash_command='echo "현재 시각: $(date +%Y-%m-%d\ %H:%M:%S)"',
    )

    # 2) 여러 명령을 연결
    bash_multi = BashOperator(
        task_id="bash_multi_command",
        bash_command=(
            'echo "=== 시스템 정보 ===" && '
            'echo "호스트: $(hostname)" && '
            'echo "디스크: $(df -h / | tail -1)" && '
            'echo "메모리: $(free -h | grep Mem | awk \'{print $3\"/\"$2}\')"'
        ),
    )

    # 3) 환경변수 사용
    bash_env = BashOperator(
        task_id="bash_with_env",
        bash_command='echo "안녕하세요, $MY_NAME님! 오늘은 $TODAY 입니다."',
        env={"MY_NAME": "학생", "TODAY": "Airflow 실습일"},
    )

    # === PythonOperator 예제 ===

    # 4) 기본 함수 호출
    python_basic = PythonOperator(
        task_id="python_generate_data",
        python_callable=_generate_data,
    )

    # 5) 인자 없는 함수
    python_stats = PythonOperator(
        task_id="python_calculate_stats",
        python_callable=_calculate_stats,
    )

    # 6) op_args / op_kwargs로 인자 전달
    python_with_args = PythonOperator(
        task_id="python_with_args",
        python_callable=_greet,
        op_args=["수강생"],
        op_kwargs={"greeting": "환영합니다"},
    )

    # 실행 순서 정의
    bash_simple >> bash_multi >> bash_env
    python_basic >> python_stats >> python_with_args
