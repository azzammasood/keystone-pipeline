# Flink with the connectors the rollup job needs (Phase 4, ADR-024). Versions: the newest Flink
# that both flink-connector-kafka and the Iceberg Flink runtime support (ADR-014). Every jar is
# pinned by SHA-256 (cross-checked against Maven Central's .sha1 on 2026-10-20).
FROM flink:2.2.1-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/flink/lib; \
    get() { curl -fsSL -A "keystone-pipeline/0.1 (+https://github.com/azzammasood/keystone-pipeline)" -o "$(basename "$1")" "$MAVEN/$1"; echo "$2  $(basename "$1")" | sha256sum -c -; }; \
    get org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar 5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616; \
    get org/apache/flink/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar 9f37aacbbcf7c7b5dd34dd673b16f83c4f18787337cea8e6378c71a7e7405c9a; \
    get org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar c79bbee8792b1115c321e4be99fab3fb36db0bf8fa2bba24b481e6da14416de3; \
    get org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957; \
    get org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.jar 607c0299f4df33013bd0a0f37c8cf255c635bbaddd247b8fec68c210cc17d19f; \
    get org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.jar 18d071c7f72f8a946677b398a57af9c7afa2dd41a81023c310f99373fec200b3

# Checkpoints and savepoints (a named volume shared by the jobmanager and taskmanager); the
# volume takes this ownership when it is first created.
USER root
RUN mkdir -p /flink-state && chown flink:flink /flink-state
USER flink
