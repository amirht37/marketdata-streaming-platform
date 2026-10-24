
# Build the official Redis Kafka Connector archive

FROM eclipse-temurin:17-jdk-jammy AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /root/redis-kafka-connect

RUN git clone \
    --branch v1.1.0 \
    --depth 1 \
    https://github.com/redis-field-engineering/redis-kafka-connect.git .

RUN chmod +x ./gradlew \
    && ./gradlew createConfluentArchive

# Install the built connector into your existing Connect image
FROM confluentinc/cp-kafka-connect:7.6.1

RUN curl -fL \
    https://github.com/prometheus/jmx_exporter/releases/download/1.4.0/jmx_prometheus_javaagent-1.4.0.jar \
    -o /opt/jmx_prometheus_javaagent.jar

COPY --from=build \
    /root/redis-kafka-connect/core/redis-kafka-connect/build/confluent/redis-redis-kafka-connect-*.zip \
    /tmp/redis-kafka-connect.zip

RUN confluent-hub install --no-prompt /tmp/redis-kafka-connect.zip \
    && rm /tmp/redis-kafka-connect.zip
