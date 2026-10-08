FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:resolve
COPY src src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --create-home analytics
USER analytics
COPY --from=build /src/target/quote-analytics.jar /app/quote-analytics.jar
ENV STATE_DIR=/home/analytics/state
ENTRYPOINT ["java", "-jar", "/app/quote-analytics.jar"]
