FROM openjdk:11

COPY gradle /tmp/build/gradle
COPY src /tmp/build/src
COPY build.gradle /tmp/build/build.gradle
COPY application.yml /tmp/build/application.yml
COPY gradlew /tmp/build/gradlew
COPY settings.gradle /tmp/build/settings.gradle

WORKDIR /tmp/build/

#RUN apk add --no-cache curl
#
RUN ./gradlew buildArtifacts

RUN cp artifacts/queue-over-http.jar /app.jar
RUN cp artifacts/application.yml.example /application.yml

RUN rm -rf /tmp/build/

WORKDIR /

ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-Dspring.profiles.active=default", "-jar", "/app.jar"]