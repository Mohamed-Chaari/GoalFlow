FROM python:3.11-slim

USER root

# Install dependencies needed for docker CLI
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    lsb-release \
    apt-transport-https \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Docker CLI
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

# Install Airflow and postgres provider
ENV AIRFLOW_HOME=/opt/airflow
RUN pip install --no-cache-dir apache-airflow==2.8.0 psycopg2-binary apache-airflow-providers-postgres

WORKDIR /opt/airflow
COPY dags/ /opt/airflow/dags/

# Start script
CMD ["bash", "-c", "airflow db init && airflow users create -u ${AIRFLOW_ADMIN_USER:-admin} -p ${AIRFLOW_ADMIN_PASSWORD:-admin} -r Admin -e admin@example.com -f Admin -l User && airflow scheduler & airflow webserver"]
