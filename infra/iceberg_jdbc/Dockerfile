FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

LABEL maintainer="Marian Bodnar"
LABEL version="2.0"
LABEL description="Spark 4.0 with Iceberg + PostgreSQL - Simple local setup"

USER root

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_PID_DIR=/opt/spark/run \
    SPARK_CONF_DIR=/opt/spark/conf \
    SPARK_LOCAL_DIRS=/opt/spark/tmp

# Install PostgreSQL and utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql \
        postgresql-contrib \
        curl \
        netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download Iceberg runtime for Spark 4.0
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
      -o /opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar

# Download JDBC drivers
RUN curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.9/postgresql-42.7.9.jar \
      -o /opt/spark/jars/postgresql-42.7.9.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.26.0.0.0/ojdbc11-23.26.0.0.0.jar \
      -o /opt/spark/jars/ojdbc11-23.26.0.0.0.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/13.2.1.jre11/mssql-jdbc-13.2.1.jre11.jar \
      -o /opt/spark/jars/mssql-jdbc-13.2.1.jre11.jar

# Create directories
RUN mkdir -p \
      /lakehouse/warehouse \
      /lakehouse/checkpoints \
      /metastore/pgdata \
      /opt/spark/conf \
      /opt/spark/tmp \
      /opt/spark/run \
      /opt/spark/logs \
      /home/spark \
      /data/apache && \
    usermod -d /home/spark spark

# Initialize PostgreSQL with custom data directory
RUN chown -R postgres:postgres /metastore && \
    pg_dropcluster --stop 14 main && \
    pg_createcluster 14 main --datadir=/metastore/pgdata && \
    pg_ctlcluster 14 main start && \
    until pg_isready -U postgres; do sleep 1; done && \
    su - postgres -c "psql -c \"CREATE USER hive WITH PASSWORD 'hive';\"" && \
    su - postgres -c "psql -c \"CREATE DATABASE hive_metastore OWNER hive;\"" && \
    su - postgres -c "psql -d hive_metastore -c \"GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO hive;\"" && \
    su - postgres -c "psql -d hive_metastore -c \"GRANT ALL ON SCHEMA public TO hive;\"" && \
    su - postgres -c "psql -d hive_metastore -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO hive;\"" && \
    su - postgres -c "psql -d hive_metastore -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO hive;\"" && \
    pg_ctlcluster 14 main stop

# Set permissions
RUN chown -R spark:spark /lakehouse /opt/spark /home/spark /data/apache

# Copy startup script
COPY start.sh /start.sh
RUN chmod +x /start.sh

VOLUME ["/lakehouse", "/metastore/pgdata", "/data/apache"]

EXPOSE 4040 15002

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD pg_isready -U postgres && nc -z localhost 15002 || exit 1

ENTRYPOINT ["/start.sh"]
