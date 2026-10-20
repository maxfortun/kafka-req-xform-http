#!/bin/bash
set -e
cp /opt/xform/server.properties /tmp/server.properties
echo "advertised.listeners=${ADVERTISED_LISTENERS:-PLAINTEXT://broker:9092}" >> /tmp/server.properties
/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t "${CLUSTER_ID:-perf-cluster-0000000000}" -c /tmp/server.properties
exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties
