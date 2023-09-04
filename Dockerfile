FROM maven:3-eclipse-temurin-17-alpine as builder

WORKDIR /build
COPY . .
RUN mvn clean install -DskipTests

FROM openjdk:17-slim

RUN mkdir -p /opt/app && mkdir -p /opt/app/libs

# Copy all dependencies from the libs folder
COPY --from=builder /build/target/libs/*.jar /opt/app/libs/
# Copy the executable jar
COPY --from=builder /build/target/pcloud-s3proxy*.jar /opt/app/pcloud-s3proxy.jar


EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/opt/app/pcloud-s3proxy.jar", "-e", "0.0.0.0:8080"]