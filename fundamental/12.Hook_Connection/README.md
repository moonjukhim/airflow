### 12. Hook & Connection

1. Plugin 설치

  - 프로젝트 루트의 requirements.txt 파일에 다음의 내용을 추가

  ```text
  apache-airflow-providers-postgres
  psycopg2-binary
  ```

2. astro를 통한 airflow 재시작

  ```bash
  astro dev restart
  ```

3. astro를 통해 실행하면 다음의 정보들을 확인할 수 있음

```text
✔ Project stopped
✔ Project image has been updated
✔ Project started
➤ Airflow UI: http://airflow.localhost:6563
➤ Postgres Database: postgresql://localhost:5432/postgres
➤ The default Postgres DB credentials are: postgres:postgres
```

```text
[내 PC (Host)]
   ↓ 포트 매핑
[Docker Container (Airflow)]
```

4. Connection 정보

```text
Connection ID : conn_airflow
Connection type : Postgres
Desc : conn_airflow
host : postgres
login : postgres
pwd : postgres
port : 5432
Database : postgres
```
