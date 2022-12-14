FROM alpine:3.17.0
RUN apk update && \
    apk upgrade && \
    apk add bash && \
    apk add ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/cache/apk/*
COPY ../target/SparkIntegrationExample-1.0-SNAPSHOT.jar /jobs/SparkIntegrationExample.jar