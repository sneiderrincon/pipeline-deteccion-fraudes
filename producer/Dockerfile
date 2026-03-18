FROM maven:3.9.7-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -q -DskipTests

FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
COPY --from=build /app/target/producer.jar producer.jar
ENTRYPOINT ["java", "-jar", "producer.jar"]
