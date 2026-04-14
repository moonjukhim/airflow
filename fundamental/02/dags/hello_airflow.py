"""
차시 2: Airflow 개요 & 환경 구성
- Airflow 구조 (Scheduler, Webserver, Worker, Metadata DB)
- DAG 개념
- Docker 환경 구성

이 DAG를 Web UI(localhost:8080)에서 확인하며 Airflow 구조를 이해합니다.
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _print_airflow_architecture():
    components = {
        "Scheduler": "DAG 파싱, Task 스케줄링 담당",
        "Webserver": "Web UI 제공 (localhost:8080)",
        "Worker": "실제 Task 실행",
        "Metadata DB": "DAG/Task 상태 저장 (PostgreSQL)",
        "DAG Processor": "DAG 파일 파싱 전담",
        "Triggerer": "Deferrable Operator 대기 처리",
    }
    print("=== Airflow 핵심 컴포넌트 ===")
    for name, role in components.items():
        print(f"  {name}: {role}")


with DAG(
    dag_id="02_hello_airflow",
    description="Airflow 환경 구성 확인용 DAG",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch02"],
):
    check_env = BashOperator(
        task_id="check_environment",
        bash_command=(
            'echo "=== Airflow 환경 정보 ===" && '
            "airflow version && "
            'echo "Python: $(python --version)" && '
            'echo "작업 디렉토리: $(pwd)" && '
            'echo "DAG 폴더: $AIRFLOW_HOME/dags"'
        ),
    )

    show_architecture = PythonOperator(
        task_id="show_architecture",
        python_callable=_print_airflow_architecture,
    )

    hello = BashOperator(
        task_id="hello_world",
        bash_command='echo "Airflow 환경 구성 완료! Web UI에서 이 DAG의 실행 결과를 확인하세요."',
    )

    check_env >> show_architecture >> hello
