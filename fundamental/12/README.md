
1. 패키지 목록 업데이트

  ```bash
  sudo apt update
  ```

2. PostgreSQL 설치

  ```bash
  sudo apt install postgresql postgresql-contrib -y
  ```

2. 서비스 상태 확인

  ```bash
  sudo systemctl status postgresql
  # active (running)
  ```

3. PostgreSQL 접속

  ```bash
  sudo -i -u postgres
  psql
  ```

4. 비밀번호 설정

  ```sql
  ALTER USER postgres PASSWORD 'your_password';
  ```

5. DB 생성

  ```sql
  CREATE DATABASE airflow_db;
  ```

6. 사용자 생성 & 권한 부여

  ```sql
  CREATE USER airflow_user WITH PASSWORD 'airflow123';
  GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
  ```



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

