FROM apache/airflow:3.1.8-python3.12

USER root

# System dependencies
RUN apt-get update && apt-get install -y \
    git curl build-essential libpq-dev \
    && apt-get clean

USER airflow

# Install dbt + any extra packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and dbt project
COPY dags/ $AIRFLOW_HOME/dags/
COPY dbt_project/ /opt/dbt_project/

# Set dbt project path as env var
ENV DBT_PROJECT_DIR=/opt/dbt_project