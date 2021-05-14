FROM openjdk:11
COPY ./build/classes/java/main/com/github/tatisled/sparkbatchingapp/ /tmp
WORKDIR /tmp
ENTRYPOINT ["java","SparkApp"]