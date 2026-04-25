"""
차시 12: Hook과 Connection 개념
- Hook이 Operator와 어떻게 다른가
- Airflow Connection 등록 실습
- Hook을 사용한 외부 시스템 접근

Operator: "무엇을 할 것인가" (Task의 행동 정의)
Hook: "어떻게 연결할 것인가" (외부 시스템과의 연결 관리)
Connection: "어디에 어떤 인증으로 연결할 것인가" (접속 정보 저장)

연결 등록 방법:
  1) Web UI: Admin > Connections
  2) CLI: airflow connections add ...
  3) 환경변수: AIRFLOW_CONN_<CONN_ID>=<URI>
"""

import pendulum
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG


def _explore_connections():
    """등록된 Connection 정보를 조회하는 예제"""
    print("=== Connection 조회 방법 ===")
    print()

    # 기본 제공되는 connection 조회 시도
    try:
        conn = BaseHook.get_connection("conn_airflow")
        print(f"Connection ID : {conn.conn_id}")
        print(f"Connection Type: {conn.conn_type}")
        print(f"Host           : {conn.host}")
        print(f"Port           : {conn.port}")
        print(f"Schema(DB)     : {conn.schema}")
        print(f"Login          : {conn.login}")
        print(f"Extra          : {conn.extra}")
    except Exception as e:
        print(f"'airflow_db' connection이 없습니다: {e}")
        print("→ 환경변수 또는 Web UI에서 Connection을 먼저 등록하세요.")

    print()
    print("=== Connection URI 형식 ===")
    print("postgres://user:password@host:5432/dbname")
    print("mysql://user:password@host:3306/dbname")
    print("http://user:password@api.example.com:443")
    print("aws://access_key:secret_key@/?region_name=ap-northeast-2")

def _demonstrate_hook_pattern_real(**context):
    """PostgresHook을 사용한 실제 DB 접근"""

    print("=== Hook 실제 사용 예제 ===")

    # 1. Hook 생성 (Airflow Connection 사용)
    hook = PostgresHook(postgres_conn_id="conn_airflow")

    # 2. Connection 가져오기
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # 3. 쿼리 실행
        print("[STEP 1] 테이블 생성")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 4. 데이터 삽입 (멱등성 고려: DELETE + INSERT 예시)
        print("[STEP 2] 데이터 삽입")
        cursor.execute("DELETE FROM users WHERE name = 'Alice'")
        cursor.execute("INSERT INTO users (name) VALUES ('Alice')")

        # 5. 조회
        print("[STEP 3] 데이터 조회")
        cursor.execute("SELECT id, name, created_at FROM users")

        results = cursor.fetchall()

        # 6. 결과 처리
        print("[STEP 4] 결과 가공")
        for row in results:
            user_id, name, created_at = row
            print(f"User: id={user_id}, name={name}, created_at={created_at}")

        # 7. 커밋
        conn.commit()

    except Exception as e:
        print("에러 발생:", e)
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()

    print("=== Hook 실행 완료 ===")

def _demonstrate_hook_pattern():
    """Hook 사용 패턴 설명"""
    print("=== Hook vs Operator ===")
    print()
    print("[Operator 방식] - 선언적, 간편")
    print("  PostgresOperator(sql='SELECT ...', postgres_conn_id='my_db')")
    print("  → SQL 실행만 하면 될 때")
    print()
    print("[Hook 방식] - 프로그래밍 방식, 유연")
    print("  hook = PostgresHook(postgres_conn_id='my_db')")
    print("  conn = hook.get_conn()")
    print("  cursor = conn.cursor()")
    print("  cursor.execute('SELECT ...')")
    print("  results = cursor.fetchall()")
    print("  → 결과를 가공하거나 복잡한 로직이 필요할 때")
    print()
    print("[언제 Hook을 직접 쓰는가?]")
    print("  1. 기존 Operator가 지원하지 않는 작업")
    print("  2. 쿼리 결과를 Python으로 가공해야 할 때")
    print("  3. 여러 시스템을 하나의 Task에서 연결할 때")
    print("  4. 커스텀 Operator를 만들 때 (내부에서 Hook 사용)")


def _connection_registration_guide():
    """Connection 등록 방법 가이드"""
    print("=== Connection 등록 3가지 방법 ===")
    print()
    print("1) 환경변수 (Docker/CI 환경에 적합)")
    print("   export AIRFLOW_CONN_MY_POSTGRES='postgres://user:pw@host:5432/db'")
    print()
    print("2) CLI 명령어")
    print("   airflow connections add 'my_postgres' \\")
    print("     --conn-type postgres \\")
    print("     --conn-host localhost \\")
    print("     --conn-port 5432 \\")
    print("     --conn-login airflow \\")
    print("     --conn-password airflow \\")
    print("     --conn-schema my_database")
    print()
    print("3) Web UI: Admin > Connections > + 버튼")
    print("   → 가장 직관적이지만 코드로 관리 불가")
    print()
    print("운영 환경 권장: Secrets Backend (Vault, AWS Secrets Manager)")


with DAG(
    dag_id="12_hook_and_connection",
    description="Hook과 Connection 개념 실습",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch12"],
):
    explore = PythonOperator(
        task_id="explore_connections",
        python_callable=_explore_connections,
    )

    hook_pattern = PythonOperator(
        task_id="hook_vs_operator",
        python_callable=_demonstrate_hook_pattern_real,
    )

    guide = PythonOperator(
        task_id="registration_guide",
        python_callable=_connection_registration_guide,
    )

    explore >> hook_pattern >> guide
