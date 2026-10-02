# First code is for docker-compose and second code is for kubernetes deployment. Both are working fine. You can use any of them.
#FROM eclipse-temurin:17-jdk
#
#RUN useradd \
#    --create-home \
#    --uid 10002 \
#    appuser
#
#WORKDIR /app
#
#COPY target/log-processing-service-1.0-SNAPSHOT.jar app.jar
#
#RUN chown -R 10002:10002 /app
#
#USER 10002
#
#EXPOSE 8080
#
#ENTRYPOINT ["java", "-jar", "app.jar"]

FROM flink:2.3

COPY target/log-processing-service-1.0-SNAPSHOT.jar /opt/flink/usrlib/log-processing-service.jar

ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-2.3.0.jar
#Need to enable prometheus metrics plugin for flink to expose metrics to prometheus.
#ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-2.3.0.jar;flink-metrics-prometheus-2.3.0.jar