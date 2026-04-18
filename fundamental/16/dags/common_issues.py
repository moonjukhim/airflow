"""
차시 16: 운영 시 자주 겪는 문제와 대처법
- DAG가 Web UI에 안 뜰 때
- 스케줄이 안 맞을 때
- 실무 체크리스트

이 DAG는 의도적으로 다양한 문제 상황을 시연합니다.
각 Task의 로그를 확인하여 문제 원인과 해결법을 학습하세요.
"""

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _checklist_dag_not_showing():
    """문제 1: DAG가 Web UI에 안 뜰 때"""
    print("=" * 60)
    print("[문제] DAG가 Web UI에 표시되지 않습니다!")
    print("=" * 60)
    print()
    print("체크리스트:")
    print("  1. 파일 위치 확인")
    print("     → DAG 파일이 dags/ 폴더에 있는가?")
    print(f"     → 현재 AIRFLOW_HOME: {os.environ.get('AIRFLOW_HOME', '미설정')}")
    print()
    print("  2. Python 문법 오류 확인")
    print("     → airflow dags list-import-errors")
    print("     → 파일 상단에 import 에러가 있으면 DAG 전체가 무시됨")
    print()
    print("  3. DAG 파일에 'airflow' 또는 'DAG' 키워드가 있는가?")
    print("     → Airflow는 빠른 파싱을 위해 키워드로 먼저 필터링")
    print()
    print("  4. DAG ID 중복 확인")
    print("     → 같은 dag_id가 여러 파일에 있으면 하나만 인식됨")
    print()
    print("  5. Scheduler가 실행 중인가?")
    print("     → docker compose ps 로 확인")
    print()
    print("  6. .airflowignore 파일 확인")
    print("     → dags/ 폴더의 .airflowignore에 해당 파일이 포함되어 있을 수 있음")


def _checklist_schedule_mismatch():
    """문제 2: 스케줄이 기대와 다를 때"""
    print("=" * 60)
    print("[문제] DAG 실행 시각이 기대와 다릅니다!")
    print("=" * 60)
    print()
    print("원인 1: UTC vs KST 혼동")
    print("  → schedule='0 9 * * *' 는 UTC 09:00 = KST 18:00")
    print("  → KST 09:00 실행: schedule='0 0 * * *' (UTC 00:00)")
    print()
    print("원인 2: data_interval 오해")
    print("  → @daily DAG는 해당 날짜가 '끝난 후' 실행됨")
    print("  → 1월 1일 데이터는 1월 2일 00:00에 처리됨")
    print()
    print("원인 3: start_date가 미래인 경우")
    print("  → start_date 이후 첫 번째 스케줄 간격이 지나야 첫 실행")
    print()
    print("원인 4: DAG가 paused 상태")
    print("  → Web UI에서 토글을 켜야 스케줄 실행됨")
    print("  → 또는 dags_are_paused_at_creation=False 설정")


def _checklist_task_failure():
    """문제 3: Task가 자주 실패할 때"""
    print("=" * 60)
    print("[문제] Task가 반복적으로 실패합니다!")
    print("=" * 60)
    print()
    print("디버깅 순서:")
    print("  1. Web UI → DAG → Task Instance → Log 확인")
    print("     → 에러 메시지와 스택 트레이스 확인")
    print()
    print("  2. 외부 시스템 상태 확인")
    print("     → DB 연결 가능? API 응답 정상?")
    print("     → Admin > Connections에서 Test Connection")
    print()
    print("  3. 리소스 부족 확인")
    print("     → Worker 메모리/CPU 확인")
    print("     → docker stats 로 컨테이너 리소스 모니터링")
    print()
    print("  4. 권한 문제")
    print("     → 파일 읽기/쓰기 권한")
    print("     → DB 사용자 권한")
    print()
    print("  5. 재시도 설정 검토")
    print("     → retries, retry_delay 적절한가?")
    print("     → 일시적 오류 vs 영구적 오류 구분")


def _production_checklist():
    """실무 운영 체크리스트"""
    print("=" * 60)
    print("=== Airflow 실무 운영 체크리스트 ===")
    print("=" * 60)
    print()
    print("[DAG 작성 시]")
    print("  [ ] catchup 설정을 의도에 맞게 했는가?")
    print("  [ ] start_date가 고정된 과거 날짜인가? (동적 날짜 금지)")
    print("  [ ] Task가 멱등적(idempotent)인가?")
    print("  [ ] XCom에 대용량 데이터를 넣지 않았는가?")
    print("  [ ] 민감 정보가 코드에 하드코딩되어 있지 않은가?")
    print()
    print("[스케줄링]")
    print("  [ ] 시간대(timezone)를 명시했는가?")
    print("  [ ] data_interval 개념을 이해하고 있는가?")
    print("  [ ] 동시 실행 제한(max_active_runs)이 필요한가?")
    print()
    print("[모니터링]")
    print("  [ ] 실패 알림(email/Slack)이 설정되어 있는가?")
    print("  [ ] SLA 설정이 필요한가?")
    print("  [ ] DAG Run 소요 시간을 추적하고 있는가?")
    print()
    print("[운영]")
    print("  [ ] Connection은 Secrets Backend로 관리하는가?")
    print("  [ ] DAG 파일 배포 방식이 정해져 있는가?")
    print("  [ ] 로그 보관 정책이 있는가?")
    print("  [ ] 메타데이터 DB 백업이 설정되어 있는가?")


with DAG(
    dag_id="16_common_issues",
    description="운영 시 자주 겪는 문제와 실무 체크리스트",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch16"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
):
    dag_not_showing = PythonOperator(
        task_id="issue_dag_not_showing",
        python_callable=_checklist_dag_not_showing,
    )

    schedule_mismatch = PythonOperator(
        task_id="issue_schedule_mismatch",
        python_callable=_checklist_schedule_mismatch,
    )

    task_failure = PythonOperator(
        task_id="issue_task_failure",
        python_callable=_checklist_task_failure,
    )

    checklist = PythonOperator(
        task_id="production_checklist",
        python_callable=_production_checklist,
    )

    # 모든 문제 체크 후 종합 체크리스트
    [dag_not_showing, schedule_mismatch, task_failure] >> checklist

    # 실제로 import error를 확인하는 명령
    check_import_errors = BashOperator(
        task_id="check_import_errors",
        bash_command=(
            'echo "=== DAG Import Error 확인 ===" && '
            "airflow dags list-import-errors 2>/dev/null || "
            'echo "import error 없음 (정상)"'
        ),
    )

    check_import_errors >> checklist
