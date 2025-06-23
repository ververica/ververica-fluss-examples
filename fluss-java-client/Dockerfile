FROM flink:1.20-java17

RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/1.0.1/paimon-flink-1.20-1.0.1.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/alibaba/fluss/fluss-flink-1.20/0.7.0/fluss-flink-1.20-0.7.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/alibaba/fluss/fluss-lake-paimon/0.7.0/fluss-lake-paimon-0.7.0.jar
RUN wget -P /opt/flink/opt https://repo1.maven.org/maven2/com/alibaba/fluss/fluss-flink-tiering/0.7.0/fluss-flink-tiering-0.7.0.jar
