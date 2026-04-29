### airflow 의 필요한 설정 부분

##### 1. openssh-server 설치와 방화벽 설정

```bash
sudo apt update
sudo apt install -y openssh-server
sudo systemctl enable --now ssh
```

```bash
sudo ufw allow 22/tcp
sudo ufw enable
```

##### 2. Docker 설치

```bash
# 시스템 업데이트
sudo apt update && sudo apt upgrade -y

# Docker 설치에 필요한 패키지
sudo apt install -y ca-certificates curl gnupg lsb-release

# Docker GPG 키 추가
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Docker 저장소 등록
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Docker 엔진 설치
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# sudo 없이 docker 사용
sudo usermod -aG docker $USER
newgrp docker
```

##### 3. astro 설치

```bash
curl -sSL install.astronomer.io | sudo bash -s
```

```bash
mkdir ~/airflow && cd ~/airflow
astro dev init
```

```bash
astro dev start
```

##### 4. 추가적인 설정

```yaml
services:
  api-server:
    ports:
      - "8081:8080"
    environment:
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true" # config 볼 수 있도록
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL: 30
  scheduler:
    environment:
      AIRFLOW__CORE__DAG_DIR_LIST_INTERVAL: 5
      AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL: 5
      AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC: 5
      AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL: 5
      AIRFLOW__SCHEDULER__PARSING_PROCESSES: 10
```

##### 5. 다시 astro 실행

```bash
astro dev restart
```

##### 6. Web UI

- ifconfig로 확인 IP로 접속하여 UI 확인
- http://192.168.56.108:8081
