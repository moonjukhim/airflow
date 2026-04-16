# Git Cloning

```bash
git clone https://github.com/moonjukhim/airflow.git
```

# PowerShell에서 VirtualBox로 접속

```shell
ssh airflow@192.168.56.111
```

2. ssh에 접속한 후, DAG 파일을 dags 폴더에 복사

```bash
# pwd 명령어로 확인 시
# /home/airflow/Desktop/airflow 위치
cp airflow/fundamental/03/dags/my_first_dag.py  dags/
```