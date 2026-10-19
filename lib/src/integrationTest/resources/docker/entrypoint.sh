#!/bin/bash
set -e

# Testcontainers writes /tmp/advertised.listeners once the host port mapping is known.
# For a manual run, set ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,BROKER://localhost:9093 instead.
[ -z "$ADVERTISED_LISTENERS" ] || echo "$ADVERTISED_LISTENERS" > /tmp/advertised.listeners
while [ ! -s /tmp/advertised.listeners ]; do sleep 0.1; done

cp /opt/xform/server.properties /tmp/server.properties
echo "advertised.listeners=$(cat /tmp/advertised.listeners)" >> /tmp/server.properties

/opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c /tmp/server.properties
exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties
