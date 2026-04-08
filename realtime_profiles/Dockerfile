FROM flink:1.20-java17


RUN #wget -P /opt/flink/opt https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/0.9.0-incubating/fluss-flink-tiering-0.9.0-incubating.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/0.9.0-incubating/fluss-flink-1.20-0.9.0-incubating.jar
RUN #wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/0.9.0-incubating/fluss-lake-paimon-0.9.0-incubating.jar
RUN #wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/1.0.1/paimon-flink-1.20-1.0.1.jar
RUN #wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/1.0.1/paimon-s3-1.0.1.jar
RUN #wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-s3/0.9.0-incubating/fluss-fs-s3-0.9.0-incubating.jar

RUN #wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar