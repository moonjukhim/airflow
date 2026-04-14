"""
차시 3: DAG란 무엇인가
- DAG(Directed Acyclic Graph) 구조 이해
- 첫 DAG 파일 작성
- Web UI에서 실행 확인

DAG는 Task들의 방향성 비순환 그래프입니다.
순환(cycle)이 없어야 하며, 각 Task는 명확한 실행 순서를 가집니다.
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# === DAG 정의 방법 1: with 문 (권장) ===
with DAG(
    dag_id="03_my_first_dag",
    description="DAG 구조를 이해하기 위한 첫 번째 DAG",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,           # 수동 실행만
    catchup=False,           # 과거 실행 건너뛰기
    tags=["exercise", "ch03"],
    # DAG 수준 기본 설정
    default_args={
        "owner": "airflow-student",
        "retries": 1,
    },
):
    # Task 1: BashOperator로 간단한 명령 실행
    start = BashOperator(
        task_id="start",
        bash_command='echo "=== DAG 시작: $(date) ==="',
    )

    # Task 2: PythonOperator로 함수 실행
    def _process():
        print("데이터 처리 중...")
        result = sum(range(1, 101))
        print(f"1부터 100까지의 합: {result}")

    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    # Task 3: 종료 알림
    end = BashOperator(
        task_id="end",
        bash_command='echo "=== DAG 종료: $(date) ==="',
    )

    # DAG 구조: start → process → end
    # Web UI의 Graph 탭에서 시각적으로 확인 가능
    start >> process >> end
