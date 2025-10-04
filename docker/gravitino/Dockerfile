FROM apache/gravitino-iceberg-rest:latest

# Download PostgreSQL JDBC driver
USER root
RUN cd /root/gravitino-iceberg-rest-server/libs && \
    curl -L -O https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    chmod 644 postgresql-42.7.3.jar

# Switch back to default user if needed
USER root
