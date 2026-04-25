
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

7. 외부 접속 허용

```bash
sudo nano /etc/postgresql/*/main/postgresql.conf
```

다음의 내용을 찾아서 수정

```text
listen_addresses = '*'
```

```bash
sudo nano /etc/postgresql/*/main/pg_hba.conf
```

다음의 내용을 추가 

```text
host all all 0.0.0.0/0 md5
```

설정 후 재시작

```bash
sudo systemctl restart postgresql
```

재시작 후, 포트 확인

```bash
sudo netstat -ntlp | grep 5432
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

