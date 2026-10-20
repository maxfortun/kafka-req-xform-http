#!/bin/bash
# Collects the fork's broker jars (from ~/.m2) and the plugin with its dependencies into perf/build for the image.
set -euo pipefail
cd "$(dirname "$0")"
export KAFKA_VERSION=${KAFKA_VERSION:-3.7.2-MAX12}
m2=${M2_REPO:-$HOME/.m2/repository}/org/apache/kafka

rm -rf ../lib/build/libs ../lib/dependencies
(cd .. && ./gradlew --no-daemon -q :lib:jar :lib:copyDependencies -x test)
rm -rf build && mkdir -p build/broker-libs build/plugin-libs
cp "$m2/kafka_2.13/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.jar" "$m2/kafka-clients/$KAFKA_VERSION/kafka-clients-$KAFKA_VERSION.jar" build/broker-libs/
cp ../lib/build/libs/*.jar build/plugin-libs/
# Skip artifacts the broker already ships (kafka-clients, slf4j, compression codecs), so it keeps its own versions.
provided=$(docker run --rm --entrypoint ls apache/kafka:3.7.2 /opt/kafka/libs | sed -E 's/-[0-9][^-]*\.jar$//' | sort -u)
for jar in ../lib/dependencies/*.jar; do
	artifact=$(basename "$jar" | sed -E 's/-[0-9][^-]*\.jar$//')
	grep -qx "$artifact" <<<"$provided" || cp "$jar" build/plugin-libs/
done
ls build/broker-libs build/plugin-libs
