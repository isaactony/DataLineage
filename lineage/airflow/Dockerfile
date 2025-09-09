FROM apache/airflow:2.7.0-python3.9

# Switch to root user to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install additional Python packages
RUN pip install --no-cache-dir \
    openlineage-airflow==1.0.0 \
    openlineage-sqlalchemy==1.0.0 \
    dbt-postgres==1.6.0 \
    psycopg2-binary==2.9.7 \
    pandas==2.0.3

# Copy DAGs and configuration
COPY dags/ /opt/airflow/dags/
COPY requirements.txt /opt/airflow/

# Install additional requirements
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Set environment variables
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://marquez:marquez@postgres:5432/marquez
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins
ENV AIRFLOW__CORE__AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080
ENV AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
ENV AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
ENV AIRFLOW__OPENLINEAGE__ENABLED=True
ENV AIRFLOW__OPENLINEAGE__NAMESPACE=data-lineage-audit
ENV AIRFLOW__OPENLINEAGE__TRANSPORT=http
ENV AIRFLOW__OPENLINEAGE__URL=http://marquez:5000
