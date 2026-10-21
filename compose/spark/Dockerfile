# Spark SQL as a reader of the Iceberg tables (Phase 5 gate, ADR-029). Spark 4.1 is the newest
# line with an Iceberg 1.12.0 runtime (4.2 has none yet). Jars pinned by SHA-256 (the runtime's
# .sha1 cross-checked against Maven Central on 2026-10-20); the aws bundle is the one Flink uses.
FROM spark:4.1.3-scala2.13-java17-ubuntu@sha256:746cf1a805dff72fc5648aadcad5bdb14cbf2f7f32ceeeec8a11f2c9c23104a4

ARG MAVEN=https://repo1.maven.org/maven2
USER root
RUN set -eu; cd /opt/spark/jars; \
    get() { curl -fsSL -A "keystone-pipeline/0.1 (+https://github.com/azzammasood/keystone-pipeline)" -o "$(basename "$1")" "$MAVEN/$1"; echo "$2  $(basename "$1")" | sha256sum -c -; }; \
    get org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar 3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82; \
    get org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957;     get io/openlineage/openlineage-spark_2.13/1.54.0/openlineage-spark_2.13-1.54.0.jar d9745f286ad66f6a147a85c17fcbbdf947885648e05d1a789c4e2e2874489708
# openlineage-spark (Phase 6): the listener in spark-defaults.conf writes OpenLineage events to
# /lineage; its .sha1 cross-checked against Maven Central on 2026-10-21.
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark
