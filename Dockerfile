FROM openjdk:17-slim

RUN mkdir /opt/app && mkdir /opt/app/libs

# Copy all dependencies from the libs folder
COPY target/libs/*.jar /opt/app/libs/
# Copy the executable jar
COPY target/pcloud-s3proxy*.jar /opt/app/pcloud-s3proxy.jar


EXPOSE 8080
CMD ["java", "-jar", "/opt/app/pcloud-s3proxy.jar", "-e", "0.0.0.0:8080"]