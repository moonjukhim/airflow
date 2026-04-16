# Airflow

### SSH 설정

```bash
sudo apt install openssh-server -y
```

```bash
sudo systemctl status ssh
```

3. 방화벽 조정

```bash
sudo ufw allow 22
```

4. 방화벽 확인

```bash
sudo ufw status
```


### Docker 설치

1. 기존 Docker 제거

    ```bash
    sudo apt-get remove docker docker-engine docker.io containerd runc
    ```

2. 패키지 업데이트

    ```bash
    sudo apt-get update
    ```

3. 필수 패키지 설치

    ```bash
    sudo apt-get install apt-transport-https ca-certificates curl software-properties-common
    ```

4. Docker 공식 GPG 키 추카

    ```bash
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    ```

5. Docker 저장소 추가

echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

6. 다시 패키지 업데이트

sudo apt-get update

7. Docker 및 관련 패키지 설치

sudo apt-get install docker-ce docker-ce-cli containerd.io

8. Docker 설치 확인

sudo docker --version

9. Docker 서비스 시작

sudo systemctl start docker


---

sudo usermod -aG docker $USER
newgrp docker

---

curl -sSL install.astronomer.io | sudo bash -s



