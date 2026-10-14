# Hive Metastore and HiveServer2 (same image, chosen with SERVICE_NAME), backed by Postgres.
FROM apache/hive:4.0.1

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG POSTGRES_JDBC_SHA256=188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e

USER root
ADD --checksum=sha256:${POSTGRES_JDBC_SHA256} \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/hive/lib/postgresql-jdbc.jar
RUN chmod 644 /opt/hive/lib/postgresql-jdbc.jar
COPY hive-site.xml /opt/hive/conf/hive-site.xml
USER hive
