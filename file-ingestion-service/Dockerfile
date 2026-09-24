FROM eclipse-temurin:17-jdk

RUN useradd \
    --create-home \
    --uid 10001 \
    appuser

WORKDIR /app

COPY target/file-ingestion-service-0.0.1-SNAPSHOT.jar app.jar

RUN chown -R 10001:10001 /app

USER 10001

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]