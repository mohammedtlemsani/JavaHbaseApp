FROM openjdk:8-jdk-alpine
COPY target/hbaseApp-1.0-SNAPSHOT-shaded.jar /app.jar
CMD ["java","-jar","/app.jar"]