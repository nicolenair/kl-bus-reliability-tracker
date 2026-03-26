# for running locally

```
docker build -t us-central1-docker.pkg.dev/kl-bus-reliability-tracker/airflow-dbt/airflow-dbt:latest .
docker-compose up -d
docker run -it  -v ${PWD}/dbt_project:/dbt_project dbt-custom
```

# for deploying to VM

## in VM

Complete .env file based on .env.template
```
gcloud auth configure-docker us-central1-docker.pkg.dev
sudo usermod -aG docker $USER
newgrp docker
```

```
# Pull your project files (or scp them over)
git clone https://github.com/nicolenair/kl-bus-reliability-tracker && cd kl-bus-reliability-tracker/airflow-dbt/ && mkdir dbt_project
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker build -t us-central1-docker.pkg.dev/kl-bus-reliability-tracker/airflow-dbt/airflow-dbt:latest .
docker build -t dbt-custom:latest .
docker-compose down
docker-compose up -d
```

## ssh

```
gcloud compute ssh --project=kl-bus-reliability-tracker --zone=us-central1-a airflow-dbt-vm -- -L 8080:localhost:8080
```

