### airflow 의 필요한 설정 부분

```bash
sudo apt update
sudo apt install -y openssh-server
sudo systemctl enable --now ssh
```

```bash
sudo ufw allow 22/tcp
sudo ufw enable
```

1. 

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




- config 볼 수 있도록
- dag 폴더 체크하는 간격

