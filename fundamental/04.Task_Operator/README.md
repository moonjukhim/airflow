
1. Airflow가 실행되고 있는지 확인

```bash
astro dev status
astro dev start
```

1. Terminal 접속

```bash
ssh airflow@[MY_IP]
```

2. DAG 파일을 dags 폴더로 카피

```bash
cd airflow
cp airflow/fundamental/04/dags/operators_practice.py dags/
```