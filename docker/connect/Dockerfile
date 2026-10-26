FROM confluentinc/cp-kafka-connect:7.9.2

USER root
RUN mkdir -p /opt/connect-plugins/debezium-mysql /spool/input /spool/finished /spool/error \
    && curl -fsSL https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-mysql/2.7.4.Final/debezium-connector-mysql-2.7.4.Final-plugin.tar.gz \
       | tar -xz -C /opt/connect-plugins/debezium-mysql --strip-components=1 \
    && confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:2.0.65 \
    && chown -R appuser:appuser /spool /opt/connect-plugins
USER appuser
