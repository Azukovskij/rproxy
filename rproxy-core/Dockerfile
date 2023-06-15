ARG BASE_IMG=adoptopenjdk/openjdk11:x86_64-debianslim-jre-11.0.19_7
FROM ${BASE_IMG}

ARG HTTP_PORT=7878
ARG MAX_CONNECTIONS=32

ENV WORKSPACE /usr/lib/genesis
ENV HTTP_PORT ${HTTP_PORT}
ENV MAX_CONNECTIONS ${MAX_CONNECTIONS}

WORKDIR ${WORKSPACE}
COPY target/*-jar-with-dependencies.jar fat.jar

EXPOSE ${HTTP_PORT}

CMD java -jar ${WORKSPACE}/fat.jar  server --port ${HTTP_PORT} --maxconnections ${MAX_CONNECTIONS}
