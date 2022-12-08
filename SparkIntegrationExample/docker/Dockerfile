FROM alpine:3.17.0 AS builder
RUN apk update && \
    apk upgrade && \
    apk add ca-certificates && \
    update-ca-certificates && \
    apk add openjdk11 && \
    apk add maven && \
    rm -rf /var/cache/apk/*
WORKDIR /workspace/app
ADD pom.xml .
ADD src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -f pom.xml package -DskipTests

FROM alpine:3.17.0
RUN apk update && \
        apk upgrade && \
        apk add ca-certificates && \
        update-ca-certificates
COPY --from=builder /workspace/app/target/lib /jobs/lib
COPY --from=builder /workspace/app/target/SparkIntegrationExample*.jar /jobs/SparkIntegrationExample.jar