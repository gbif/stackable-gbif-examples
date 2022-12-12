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
        update-ca-certificates && \
        apk add openjdk11-jre && \
        rm -rf /var/cache/apk/*
RUN addgroup -S supergroup &&adduser -S stackable -G supergroup
USER stackable
COPY --from=builder --chown=stackable:supergroup /workspace/app/target/lib /app/lib
COPY --from=builder --chown=stackable:supergroup /workspace/app/target/HadoopIntegrationExample*.jar /app
WORKDIR /app
ENTRYPOINT ["/bin/sh", "-c", "java -jar /app/HadoopIntegrationExample*.jar"]