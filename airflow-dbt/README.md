# for running locally

```
docker build -t us-central1-docker.pkg.dev/kl-bus-reliability-tracker/airflow-dbt/airflow-dbt:latest .
docker-compose up -d
```

# for deploying to VM

## local
```
# Authenticate Docker to GCP
gcloud auth configure-docker us-central1-docker.pkg.dev

# Build and push
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker build -t us-central1-docker.pkg.dev/kl-bus-reliability-tracker/airflow-dbt/airflow-dbt:latest .
docker push us-central1-docker.pkg.dev/kl-bus-reliability-tracker/airflow-dbt/airflow-dbt:latest
```

## in VM

Complete .env file based on .env.template
```
gcloud auth configure-docker us-central1-docker.pkg.deva
# Pull your project files (or scp them over)
git clone https://github.com/nicolenair/kl-bus-reliability-tracker && cd airflow-dbt
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose pull
docker-compose up -d
```