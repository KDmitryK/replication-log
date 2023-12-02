./gradlew build

docker build -t ds-master ./master
docker build -t ds-slave ./slave

docker-compose up
