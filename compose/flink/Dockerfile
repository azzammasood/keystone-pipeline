# Flink with the connectors the rollup job needs (Phase 4, ADR-024). Versions: the newest Flink
# that both flink-connector-kafka and the Iceberg Flink runtime support (ADR-014). Every jar is
# pinned by SHA-256 (cross-checked against Maven Central's .sha1 on 2026-10-20).
FROM flink:2.2.1-scala_2.12-java17@sha256:c5124717afe3829cc2d7c9b538bfe0b9d66cae3107a174d8f0e0392abe6375b8

ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/flink/lib; \
    get() { curl -fsSL -A "keystone-pipeline/0.1 (+https://github.com/azzammasood/keystone-pipeline)" -o "$(basename "$1")" "$MAVEN/$1"; echo "$2  $(basename "$1")" | sha256sum -c -; }; \
    get org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar 5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616; \
    get org/apache/flink/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar 9f37aacbbcf7c7b5dd34dd673b16f83c4f18787337cea8e6378c71a7e7405c9a; \
    get org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar c79bbee8792b1115c321e4be99fab3fb36db0bf8fa2bba24b481e6da14416de3; \
    get org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957; \
    get org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.jar 607c0299f4df33013bd0a0f37c8cf255c635bbaddd247b8fec68c210cc17d19f; \
    get org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.jar 18d071c7f72f8a946677b398a57af9c7afa2dd41a81023c310f99373fec200b3;     get io/openlineage/openlineage-flink/1.54.0/openlineage-flink-1.54.0.jar 4cfb25dc2072050167bb5109b9128b061f2f97c2ae16e0a6493db01dd0b83613
# openlineage-flink (Phase 6): native Flink 2 lineage through a job-status listener (FLIP-314);
# its .sha1 cross-checked against Maven Central on 2026-10-21.

# Parquet format for the Phase 9 scale-ladder bench (`keystone bench run`: files in, files out).
# Its own layer so the jars above stay cached; .sha1 cross-checked on Maven Central 2026-10-21.
RUN set -eu; cd /opt/flink/lib; \
    curl -fsSL -A "keystone-pipeline/0.1 (+https://github.com/azzammasood/keystone-pipeline)" -o flink-sql-parquet-2.2.1.jar "$MAVEN/org/apache/flink/flink-sql-parquet/2.2.1/flink-sql-parquet-2.2.1.jar"; \
    echo "fb8a342e7f2a4a3d7b24ab0bfb3e776a4ec426e9e1de4efa6e433fe69b9400eb  flink-sql-parquet-2.2.1.jar" | sha256sum -c -

# Checkpoints and savepoints (a named volume shared by the jobmanager and taskmanager); the
# volume takes this ownership when it is first created.
USER root
RUN mkdir -p /flink-state && chown flink:flink /flink-state
USER flink
