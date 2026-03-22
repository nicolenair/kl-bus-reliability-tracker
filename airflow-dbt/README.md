# for running locally

```
touch .env
echo "FERNET_KEY=$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')" >> .env
echo "SECRET_KEY=$(openssl rand -hex 32)" >> .env
docker build -t airflow-dbt:latest .
docker-compose up -d
```

# for deploying to VM

```
# Authenticate Docker to GCP
gcloud auth configure-docker us-central1-docker.pkg.dev

# Build and push
docker build -t us-central1-docker.pkg.dev/kl-bus-reliability-tracker/airflow-dbt/airflow-dbt:latest .
docker push us-central1-docker.pkg.dev/kl-bus-reliability-tracker/airflow-dbt/airflow-dbt:latest
```

in VM
```
# Pull your project files (or scp them over)
git clone https://github.com/nicolenair/kl-bus-reliability-tracker && cd airflow-dbt
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose pull
docker-compose up -d
```