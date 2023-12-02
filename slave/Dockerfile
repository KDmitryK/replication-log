FROM eclipse-temurin:21-jdk-alpine
VOLUME /tmp
COPY build/libs/slave-0.0.1-SNAPSHOT.jar slave.jar
ENTRYPOINT ["java","-jar","/slave.jar"]
