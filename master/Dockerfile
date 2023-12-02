FROM eclipse-temurin:21-jdk-alpine
VOLUME /tmp
COPY build/libs/master-0.0.1-SNAPSHOT.jar master.jar
ENTRYPOINT ["java","-jar","/master.jar"]
