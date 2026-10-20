# Stock Kafka with the fork's jars and the plugin. The interceptor is chosen at run time, so the same image is both
# the plain broker and the lake broker.
FROM apache/kafka:3.7.2

USER root
RUN rm -f /opt/kafka/libs/kafka-clients-*.jar /opt/kafka/libs/kafka_2.13-*.jar
COPY build/broker-libs/ build/plugin-libs/ /opt/kafka/libs/
COPY broker/entrypoint.sh broker/server.properties /opt/xform/
COPY broker/config/ /opt/xform/config/
RUN mkdir -p /var/lib/kafka/data && chown -R appuser:appuser /opt/xform /var/lib/kafka
USER appuser

ENV CLASSPATH=/opt/xform/config
ENTRYPOINT ["bash", "/opt/xform/entrypoint.sh"]
