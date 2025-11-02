FROM apache/spark:3.5.1

USER root

COPY requirements.txt /tmp/spark-requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/spark-requirements.txt && rm /tmp/spark-requirements.txt

# Download S3 support jars matching Hadoop 3.3.4 bundled with Spark 3.5.1 and Iceberg runtime for Spark 3.5 / Scala 2.12
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

USER 185

