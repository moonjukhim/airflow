"""
차시 13: DB 연동 실습
- PostgreSQL 연결
- SQLExecuteQueryOperator 실습
- Hook을 사용한 데이터 조회 및 가공

사전 준비: Docker Compose로 PostgreSQL 컨테이너 실행 필요
Connection 등록: AIRFLOW_CONN_EXERCISE_DB 환경변수 또는 Web UI에서 설정
"""

import json

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

POSTGRES_CONN_ID = "exercise_db"


def _create_and_seed():
    """Hook으로 테이블 생성 및 샘플 데이터 삽입"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            category VARCHAR(50),
            price INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    hook.run("DELETE FROM products;")

    hook.run("""
        INSERT INTO products (name, category, price) VALUES
            ('노트북', '전자기기', 1200000),
            ('마우스', '전자기기', 35000),
            ('키보드', '전자기기', 89000),
            ('책상', '가구', 250000),
            ('의자', '가구', 350000),
            ('모니터', '전자기기', 450000),
            ('헤드셋', '전자기기', 120000),
            ('책장', '가구', 180000);
    """)
    print("테이블 생성 및 샘플 데이터 삽입 완료 (8건)")


def _query_with_hook():
    """Hook으로 데이터 조회 후 Python에서 가공"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    records = hook.get_records("""
        SELECT category, COUNT(*) as cnt, AVG(price)::INTEGER as avg_price
        FROM products
        GROUP BY category
        ORDER BY avg_price DESC;
    """)

    print("=== 카테고리별 통계 ===")
    for category, count, avg_price in records:
        print(f"  {category}: {count}개 상품, 평균가 {avg_price:,}원")

    return {row[0]: {"count": row[1], "avg_price": row[2]} for row in records}


def _show_results(**context):
    """SQLExecuteQueryOperator 결과 확인"""
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="select_expensive_products")
    print("=== 고가 상품 목록 (20만원 이상) ===")
    if results:
        for row in results:
            print(f"  {row}")


with DAG(
    dag_id="13_db_integration",
    description="PostgreSQL 연동 실습",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch13"],
):
    # Step 1: Hook으로 테이블 생성 및 데이터 삽입
    setup = PythonOperator(
        task_id="create_and_seed",
        python_callable=_create_and_seed,
    )

    # Step 2: SQLExecuteQueryOperator로 조회
    select_expensive = SQLExecuteQueryOperator(
        task_id="select_expensive_products",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            SELECT name, category, price
            FROM products
            WHERE price >= 200000
            ORDER BY price DESC;
        """,
    )

    # Step 3: Hook으로 집계 쿼리 실행 + Python 가공
    aggregate = PythonOperator(
        task_id="aggregate_with_hook",
        python_callable=_query_with_hook,
    )

    # Step 4: 결과 확인
    show = PythonOperator(
        task_id="show_results",
        python_callable=_show_results,
    )

    setup >> [select_expensive, aggregate]
    select_expensive >> show
