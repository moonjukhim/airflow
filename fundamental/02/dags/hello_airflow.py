"""
차시 2: Airflow 개요 & 환경 구성
- Airflow 구조 (Scheduler, Webserver, Worker, Metadata DB)
- DAG parse time vs Task run time 차이 이해
- Docker 환경 구성
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# === DAG PARSE TIME: DAG 파일이 파싱될 때마다 실행됨 ===
print("##### [PARSE TIME] DAG 파일이 Scheduler에 의해 파싱됨")


def _show_architecture():
    # === TASK RUN TIME: Task가 실제 실행될 때만 실행됨 ===
    print("##### [RUN TIME] Task가 Worker에서 실행됨")
    components = {
        "Scheduler": "DAG 파싱, Task 스케줄링 담당",
        "API Server": "Web UI + REST API 제공 (localhost:8080)",
        "Worker": "실제 Task 실행 (Celery/Kubernetes)",
        "Metadata DB": "DAG/Task 상태 저장 (PostgreSQL)",
        "DAG Processor": "DAG 파일 파싱 전담",
        "Triggerer": "Deferrable Operator 대기 처리",
    }
    print("=== Airflow 핵심 컴포넌트 ===")
    for name, role in components.items():
        print(f"  {name}: {role}")


with DAG(
    dag_id="02_hello_airflow",
    description="Airflow 환경 구성 확인 + parse time vs run time",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch02"],
):
    print("##### [PARSE TIME] DAG context 블록 안 — 아직 parse time")

    check_env = BashOperator(
        task_id="check_environment",
        bash_command=(
            'echo "=== Airflow 환경 정보 ===" && '
            'echo "Airflow: $(python -c "import airflow; print(airflow.__version__)")" && '
            'echo "Python: $(python --version)" && '
            'echo "작업 디렉토리: $(pwd)" && '
            'echo "DAG 폴더: $AIRFLOW_HOME/dags"'
        ),
    )

    show_architecture = PythonOperator(
        task_id="show_architecture",
        python_callable=_show_architecture,
    )

    hello = BashOperator(
        task_id="hello_world",
        bash_command='echo "환경 구성 완료! Scheduler 로그에서 PARSE TIME 출력을 확인하세요."',
    )

    check_env >> show_architecture >> hello
