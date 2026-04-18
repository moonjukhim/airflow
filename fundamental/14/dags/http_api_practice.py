"""
차시 14: HTTP API 연동 실습
- SimpleHttpOperator / HttpSensor
- 외부 API 호출 후 결과 저장 파이프라인
- HTTP Connection 설정

이 예제는 공개 API(JSONPlaceholder)를 사용합니다.
Connection 등록: AIRFLOW_CONN_JSONPLACEHOLDER 환경변수 설정 필요
"""

import json

import pendulum
import requests
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


# === 방법 1: PythonOperator + requests (간단한 경우) ===
def _fetch_with_requests():
    """requests 라이브러리로 직접 API 호출"""
    response = requests.get(
        "https://jsonplaceholder.typicode.com/users",
        timeout=10,
    )
    response.raise_for_status()
    users = response.json()

    print(f"총 {len(users)}명의 사용자 조회")
    for user in users[:3]:
        print(f"  - {user['name']} ({user['email']})")

    return users


def _process_users(**context):
    """API 응답 데이터를 가공"""
    ti = context["ti"]
    users = ti.xcom_pull(task_ids="fetch_users_python")

    cities = {}
    for user in users:
        city = user["address"]["city"]
        cities[city] = cities.get(city, 0) + 1

    print("=== 도시별 사용자 분포 ===")
    for city, count in sorted(cities.items()):
        print(f"  {city}: {count}명")

    return cities


# === 방법 2: HttpOperator (Airflow Connection 활용) ===
def _parse_posts(response):
    """HttpOperator의 response_filter: 응답에서 필요한 데이터만 추출"""
    posts = json.loads(response.text)
    return [{"id": p["id"], "title": p["title"]} for p in posts[:5]]


def _show_posts(**context):
    """HttpOperator 결과 출력"""
    ti = context["ti"]
    posts = ti.xcom_pull(task_ids="fetch_posts_http_operator")

    print("=== 최신 게시물 5건 ===")
    if posts:
        for post in posts:
            print(f"  [{post['id']}] {post['title'][:50]}...")


def _fetch_todo_detail():
    """단일 리소스 조회 예제"""
    response = requests.get(
        "https://jsonplaceholder.typicode.com/todos/1",
        timeout=10,
    )
    todo = response.json()
    status = "완료" if todo["completed"] else "미완료"
    print(f"Todo #{todo['id']}: {todo['title']} [{status}]")
    return todo


with DAG(
    dag_id="14_http_api_practice",
    description="HTTP API 연동 실습 (JSONPlaceholder)",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["exercise", "ch14"],
):
    # 방법 1: PythonOperator + requests
    fetch_python = PythonOperator(
        task_id="fetch_users_python",
        python_callable=_fetch_with_requests,
    )

    process = PythonOperator(
        task_id="process_users",
        python_callable=_process_users,
    )

    # 방법 2: HttpOperator (Connection 기반)
    fetch_http = HttpOperator(
        task_id="fetch_posts_http_operator",
        http_conn_id="jsonplaceholder",
        endpoint="posts",
        method="GET",
        response_filter=_parse_posts,
        log_response=True,
    )

    show_posts = PythonOperator(
        task_id="show_posts",
        python_callable=_show_posts,
    )

    # 단일 리소스 조회
    fetch_todo = PythonOperator(
        task_id="fetch_todo",
        python_callable=_fetch_todo_detail,
    )

    fetch_python >> process
    fetch_http >> show_posts
    fetch_todo
