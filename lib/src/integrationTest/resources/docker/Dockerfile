# Stock Kafka with the fork's clients/core jars swapped in, plus the plugin and its config.
ARG KAFKA_BASE_IMAGE
FROM ${KAFKA_BASE_IMAGE}

USER root
RUN rm -f /opt/kafka/libs/kafka-clients-*.jar /opt/kafka/libs/kafka_2.13-*.jar
COPY broker-libs/ plugin-libs/ /opt/kafka/libs/
COPY entrypoint.sh server.properties /opt/xform/
COPY config/ /opt/xform/config/
RUN chown -R appuser:appuser /opt/xform
USER appuser

# Interceptor properties are loaded as ResourceBundles, so their directory must be on the classpath.
ENV CLASSPATH=/opt/xform/config
ENV KAFKA_PRODUCE_REQUEST_INTERCEPTOR=org.apache.kafka.common.requests.transform.TransformingProduceRequestInterceptor
ENV KAFKA_PRODUCE_RESPONSE_INTERCEPTOR=org.apache.kafka.common.requests.transform.TransformingProduceResponseInterceptor

ENTRYPOINT ["bash", "/opt/xform/entrypoint.sh"]
