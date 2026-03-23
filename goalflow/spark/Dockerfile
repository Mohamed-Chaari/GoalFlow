FROM python:3.11-slim

USER root

# Install OpenJDK 21
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir pyspark==3.5.0 && \
    pip install --no-cache-dir -r requirements.txt

# Pre-download required JARs for PySpark to avoid downloading at runtime
# This is a bit tricky, but we can use spark-shell to pre-download them
RUN spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 -e "println(\"JARs downloaded\")" || true

COPY . .

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
