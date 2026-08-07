# ---------- stage 1: build the connector ----------
# Keeps Maven and a JDK off the host: the only prerequisite is Docker.
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build
# Copy the POM alone first so the dependency layer is cached across source edits.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline 2>/dev/null || true
COPY src ./src
RUN mvn -B clean package -DskipTests

# ---------- stage 2: Flink runtime ----------
FROM flink:2.3.0-java17

# envsubst renders the SQL template so the API key never lands in a file on disk.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gettext-base curl unzip \
    && rm -rf /var/lib/apt/lists/*

# The Milvus vector-search connector built above.
COPY --from=builder /build/target/flink-connector-milvus-*.jar /opt/flink/lib/milvus-connector.jar

# OpenAI model provider. NOT part of the Flink distribution -- it is released separately
# as org.apache.flink:flink-model-openai and must be on the classpath. Without it,
# CREATE MODEL still succeeds (model DDL is lazy) and the first ML_PREDICT fails with
#   "Could not find any factories that implement ModelProviderFactory"
# The jar is self-contained: it bundles the OpenAI Java client and OkHttp.
ARG OPENAI_MODEL_VERSION=2.3.0
RUN curl -fL -o /opt/flink/lib/flink-model-openai.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-model-openai/${OPENAI_MODEL_VERSION}/flink-model-openai-${OPENAI_MODEL_VERSION}.jar"

# Kafka connector. 5.0.0-2.2 is the newest release; there is no -2.3 build yet, so this
# is one Flink minor behind the runtime. Swap it if a -2.3 artifact appears.
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
RUN curl -fL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && ls -la /opt/flink/lib/

# JDBC connector, for the Postgres incident store. Three parts, and all three are required:
# the core connector, the Postgres dialect, and the driver itself (Flink ships no JDBC drivers).
#
# 4.0.0-2.0 is the only JDBC release built for Flink 2.x, so this runs three minors behind the
# 2.3 runtime -- a wider gap than the Kafka connector's one. './run.sh verify' checks the jars
# are present; the real proof is a JDBC sink that plans and writes, which './run.sh smoke-jdbc'
# exercises without touching the models.
ARG JDBC_CONNECTOR_VERSION=4.0.0-2.0
ARG POSTGRES_DRIVER_VERSION=42.7.7
RUN curl -fL -o /opt/flink/lib/flink-connector-jdbc-core.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc-core/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar" \
 && curl -fL -o /opt/flink/lib/flink-connector-jdbc-postgres.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar" \
 && curl -fL -o /opt/flink/lib/postgresql.jar \
      "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar" \
 && ls -la /opt/flink/lib/

COPY deploy/submit.sh /opt/submit.sh
RUN chmod +x /opt/submit.sh
USER flink
