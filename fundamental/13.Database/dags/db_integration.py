"""
차시 13: DB 연동 실습
- PostgresHook: 프로그래밍 방식 (get_records, run)
- SQLExecuteQueryOperator: 선언적 방식
- 외부 SQL 파일 사용 (template_searchpath)
- hook_params로 session timezone 설정
"""

import os

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task

POSTGRES_CONN_ID = "exercise_db"
LOCAL_TZ = "Asia/Seoul"
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "include", "sql")


@task(task_id="seed_data")
def seed_data():
    """Hook으로 직접 데이터 삽입 (멱등적: DELETE + INSERT)"""
    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID,
        options=f"-c timezone={LOCAL_TZ}",
    )

    hook.run("""
        CREATE TABLE IF NOT EXISTS public.products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            category VARCHAR(50),
            price INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    hook.run("DELETE FROM public.products;")

    hook.run("""
        INSERT INTO public.products (name, category, price) VALUES
            ('노트북', '전자기기', 1200000),
            ('마우스', '전자기기', 35000),
            ('키보드', '전자기기', 89000),
            ('책상', '가구', 250000),
            ('의자', '가구', 350000),
            ('모니터', '전자기기', 450000),
            ('헤드셋', '전자기기', 120000),
            ('책장', '가구', 180000);
    """)
    print("8건 삽입 완료")


@task(task_id="query_with_hook")
def query_with_hook():
    """Hook으로 집계 쿼리 + Python 가공"""
    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID,
        options=f"-c timezone={LOCAL_TZ}",
    )

    # DB session timezone 확인
    tz_result = hook.get_first("SHOW timezone;")
    print(f"DB session timezone: {tz_result}")

    records = hook.get_records("""
        SELECT category, COUNT(*) as cnt, AVG(price)::INTEGER as avg_price
        FROM public.products
        GROUP BY category
        ORDER BY avg_price DESC;
    """)

    print("=== 카테고리별 통계 (Hook) ===")
    for category, count, avg_price in records:
        print(f"  {category}: {count}개, 평균가 {avg_price:,}원")

    return {row[0]: {"count": row[1], "avg_price": row[2]} for row in records}


@task(task_id="show_operator_result")
def show_operator_result(**context):
    """SQLExecuteQueryOperator 결과 확인"""
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="select_expensive_sql_operator")
    print("=== 고가 상품 (SQLExecuteQueryOperator) ===")
    if results:
        for row in results:
            print(f"  {row}")


with DAG(
    dag_id="13_db_integration",
    description="PostgresHook + SQLExecuteQueryOperator + 외부 SQL",
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch13"],
    template_searchpath=[SQL_DIR],
):
    seeded = seed_data()

    # SQLExecuteQueryOperator: 인라인 SQL
    select_expensive = SQLExecuteQueryOperator(
        task_id="select_expensive_sql_operator",
        conn_id=POSTGRES_CONN_ID,
        hook_params={"options": f"-c timezone={LOCAL_TZ}"},
        sql="""
            SELECT name, category, price
            FROM public.products
            WHERE price >= 200000
            ORDER BY price DESC;
        """,
    )

    # SQLExecuteQueryOperator: 외부 SQL 파일 (template_searchpath 활용)
    select_by_file = SQLExecuteQueryOperator(
        task_id="select_by_sql_file",
        conn_id=POSTGRES_CONN_ID,
        hook_params={"options": f"-c timezone={LOCAL_TZ}"},
        sql="select_category_stats.sql",
    )

    queried = query_with_hook()
    shown = show_operator_result()

    seeded >> [select_expensive, select_by_file, queried]
    select_expensive >> shown
