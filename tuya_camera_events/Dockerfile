ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
    openjdk17-jre \
    maven \
    git

WORKDIR /app

RUN git clone https://github.com/tuya/tuya-pulsar-sdk-java.git /tmp/tuya-sdk

COPY src /app/src
COPY pom.xml /app/pom.xml
COPY run.sh /run.sh

RUN cd /tmp/tuya-sdk && \
    mvn clean install -DskipTests && \
    cd /app && \
    mvn clean package -DskipTests

RUN chmod a+x /run.sh

CMD [ "/run.sh" ]