FROM flink:1.20-java11

RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/1.0.1/paimon-flink-1.20-1.0.1.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/com/alibaba/fluss/fluss-connector-flink/0.6.0/fluss-connector-flink-0.6.0.jar

RUN wget -P /opt/flink/lib https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar

