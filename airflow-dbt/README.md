# for running locally

```
touch .env
echo "FERNET_KEY=$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')" >> .env
echo "SECRET_KEY=$(openssl rand -hex 32)" >> .env
docker build -t airflow-dbt:latest .
docker-compose up -d
```

# for deploying to VM
