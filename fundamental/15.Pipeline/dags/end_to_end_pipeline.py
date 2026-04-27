"""
차시 15: 실전 파이프라인 구축
- API 호출 → DB 저장 → 결과 확인까지 end-to-end 실습
- TaskFlow API로 깔끔하게 구성
- 실전에서 자주 쓰는 패턴 종합

시나리오: JSONPlaceholder API에서 사용자 + 게시물 데이터를 가져와
PostgreSQL에 저장하고, 사용자별 게시물 수를 집계하는 파이프라인
"""

import json
from datetime import timedelta

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task

POSTGRES_CONN_ID = "[MY_CONNECTION_ID]"
# POSTGRES_CONN_ID = "conn_airflow"


@task(task_id="setup_tables")
def setup_tables():
    """DB 테이블 초기화 (멱등적)"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run("""
        CREATE TABLE IF NOT EXISTS api_users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(200),
            email VARCHAR(200),
            city VARCHAR(100),
            fetched_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS api_posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES api_users(id),
            title TEXT,
            body TEXT,
            fetched_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS user_post_summary (
            user_id INTEGER PRIMARY KEY,
            user_name VARCHAR(200),
            post_count INTEGER,
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    print("테이블 준비 완료: api_users, api_posts, user_post_summary")


@task(task_id="fetch_users")
def fetch_users():
    """외부 API에서 사용자 데이터 가져오기"""
    response = requests.get(
        "https://jsonplaceholder.typicode.com/users",
        timeout=10,
    )
    response.raise_for_status()
    users = response.json()
    print(f"사용자 {len(users)}명 조회 완료")
    return users


@task(task_id="fetch_posts")
def fetch_posts():
    """외부 API에서 게시물 데이터 가져오기"""
    response = requests.get(
        "https://jsonplaceholder.typicode.com/posts",
        timeout=10,
    )
    response.raise_for_status()
    posts = response.json()
    print(f"게시물 {len(posts)}건 조회 완료")
    return posts


@task(task_id="load_users")
def load_users(users: list):
    """사용자 데이터를 DB에 적재 (UPSERT 패턴 - 멱등적)"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    for user in users:
        hook.run(
            """
            INSERT INTO api_users (id, name, email, city)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                city = EXCLUDED.city,
                fetched_at = NOW();
            """,
            parameters=(
                user["id"],
                user["name"],
                user["email"],
                user["address"]["city"],
            ),
        )
    print(f"사용자 {len(users)}명 DB 적재 완료")


@task(task_id="load_posts")
def load_posts(posts: list):
    """게시물 데이터를 DB에 적재 (UPSERT 패턴)"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    for post in posts:
        hook.run(
            """
            INSERT INTO api_posts (id, user_id, title, body)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                fetched_at = NOW();
            """,
            parameters=(post["id"], post["userId"], post["title"], post["body"]),
        )
    print(f"게시물 {len(posts)}건 DB 적재 완료")


@task(task_id="aggregate_summary")
def aggregate_summary():
    """사용자별 게시물 수 집계 → summary 테이블 갱신"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run("""
        INSERT INTO user_post_summary (user_id, user_name, post_count, updated_at)
        SELECT u.id, u.name, COUNT(p.id), NOW()
        FROM api_users u
        LEFT JOIN api_posts p ON u.id = p.user_id
        GROUP BY u.id, u.name
        ON CONFLICT (user_id) DO UPDATE SET
            user_name = EXCLUDED.user_name,
            post_count = EXCLUDED.post_count,
            updated_at = NOW();
    """)
    print("사용자별 게시물 집계 완료")


@task(task_id="generate_report")
def generate_report():
    """최종 결과 리포트 출력"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    records = hook.get_records("""
        SELECT user_name, post_count
        FROM user_post_summary
        ORDER BY post_count DESC;
    """)

    total_users = len(records)
    total_posts = sum(r[1] for r in records)

    print("=" * 50)
    print("=== 실전 파이프라인 최종 리포트 ===")
    print("=" * 50)
    print(f"총 사용자: {total_users}명")
    print(f"총 게시물: {total_posts}건")
    print()
    print("사용자별 게시물 수:")
    for name, count in records:
        bar = "#" * count
        print(f"  {name:25s} | {count:3d}건 | {bar}")
    print("=" * 50)


with DAG(
    dag_id="15_end_to_end_pipeline",
    description="API → DB → 집계 → 리포트 end-to-end 실습",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch15"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
):
    tables = setup_tables()
    users = fetch_users()
    posts = fetch_posts()

    users_loaded = load_users(users)
    posts_loaded = load_posts(posts)

    tables >> [users, posts]
    users_loaded >> posts_loaded >> aggregate_summary() >> generate_report()
