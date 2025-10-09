FROM apache/spark:latest

USER root

# Download hadoop-aws 3.4.1 and AWS SDK bundle v2 (required for Hadoop 3.4.x)
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.19/bundle-2.28.19.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.767/aws-java-sdk-bundle-1.12.767.jar

USER 185

