package org.apache.kafka.common.requests.transform;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;

import org.json.JSONObject;

import org.junit.Test;

// content-lake.properties: off unless the record sets content-lake-broker-enable=true; retry.txnIdHeader=it-txn-id, maxAttempts=3.
public class ContentLakeInterceptorIT extends XformKafkaIT {
    private static final String PREFIX = "content-lake-broker-";
    private static final Map<String, Object> NO_RETRIES = Map.of(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
        ProducerConfig.RETRIES_CONFIG, 0,
        ProducerConfig.ACKS_CONFIG, "1");

    // Lake-enabled record; it-lake-id is unique across tests since the stub's call counts are shared.
    private static ProducerRecord<String, String> lakeRecord(String topic, Integer partition, String key, String... headerKeyValues) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, "v-" + key);
        record.headers().add(PREFIX + "enable", bytes("true"));
        record.headers().add(StubLake.ID, bytes(id(topic, key)));
        for (int i = 0; i < headerKeyValues.length; i += 2) {
            record.headers().add(headerKeyValues[i], bytes(headerKeyValues[i + 1]));
        }
        return record;
    }

    private static String id(String topic, String key) {
        return topic + "/" + key;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String header(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return null == header ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String headerIgnoreCase(ConsumerRecord<String, String> record, String key) {
        for (Header header : record.headers()) {
            if (key.equalsIgnoreCase(header.key())) {
                return new String(header.value(), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private static double connectionCloses(KafkaProducer<?, ?> producer) {
        for (Map.Entry<MetricName, ? extends Metric> metric : producer.metrics().entrySet()) {
            if ("connection-close-total".equals(metric.getKey().name()) && "producer-metrics".equals(metric.getKey().group())) {
                return (Double) metric.getValue().metricValue();
            }
        }
        throw new AssertionError("producer has no connection-close-total metric");
    }

    @Test
    public void lakeResponseReplacesRecord() throws Exception {
        String topic = createTopic();
        long timestamp = 1_700_000_000_123L;
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            ProducerRecord<String, String> record = lakeRecord(topic, null, "k1");
            producer.send(new ProducerRecord<>(topic, null, timestamp, record.key(), record.value(), record.headers())).get(30, SECONDS);
        }

        List<ConsumerRecord<String, String>> records = consumeAll(topic, "read_uncommitted");

        assertEquals(1, records.size());
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals("lake:v-k1", record.value());
        assertEquals("k1", record.key());
        assertEquals(timestamp, record.timestamp());
        assertEquals("uid-" + id(topic, "k1"), headerIgnoreCase(record, StubLake.REC_UID));
        assertTrue("lineage is kept through headers.transientPattern",
            Pattern.matches("/it:" + Pattern.quote(topic) + ":\\d+", lineage(record)));
    }

    // Lineage runs first in the chain, so the lake sees it along with the broker's context headers.
    @Test
    public void lakeRequestCarriesLineageAndBrokerContext() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            producer.send(lakeRecord(topic, null, "k1", "uid", "u1")).get(30, SECONDS);
        }

        Map<String, String> headers = lake.lastHeaders(id(topic, "k1"));
        assertTrue(headers.get("lineage"), Pattern.matches("/it:" + Pattern.quote(topic) + ":\\d+", headers.get("lineage")));
        assertEquals(topic, headers.get(PREFIX + "topic-name"));
        assertEquals("0", headers.get(PREFIX + "partition-index"));
        assertEquals("k1", headers.get("kafka.key"));
        assertEquals("u1", headers.get("uid"));
        assertNull("broker control headers are not forwarded", headers.get(PREFIX + "enable"));
    }

    @Test
    public void recordsWithoutLakeEnabledSkipTheLake() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "k1", "v1");
            record.headers().add(StubLake.ID, bytes(id(topic, "k1")));
            producer.send(record).get(30, SECONDS);
        }

        assertEquals("v1", consumeAll(topic, "read_uncommitted").get(0).value());
        assertEquals(0, lake.calls(id(topic, "k1")));
    }

    // The interceptor answers REQUEST_TIMED_OUT, which the producer retries on the same connection.
    @Test
    public void producerRetriesUntilLakeSucceeds() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            producer.send(lakeRecord(topic, null, "k1", StubLake.FAIL, "2")).get(30, SECONDS);
            assertEquals(0.0, connectionCloses(producer), 0.0);
        }

        List<ConsumerRecord<String, String>> records = consumeAll(topic, "read_uncommitted");
        assertEquals("written once, after the third call", List.of("lake:v-k1"),
            records.stream().map(ConsumerRecord::value).collect(Collectors.toList()));
        assertEquals(3, lake.calls(id(topic, "k1")));
    }

    @Test
    public void failingPartitionDoesNotAffectOthersOrCloseTheConnection() throws Exception {
        String topic = createTopic(2);
        try (KafkaProducer<String, String> producer = producer(Map.of(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
                ProducerConfig.RETRIES_CONFIG, 0,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, 500))) {
            Future<RecordMetadata> ok = producer.send(lakeRecord(topic, 0, "ok"));
            Future<RecordMetadata> failing = producer.send(lakeRecord(topic, 1, "bad", StubLake.FAIL, "always"));

            assertEquals(0L, ok.get(30, SECONDS).offset());
            try {
                failing.get(30, SECONDS);
                fail("the failing partition should be rejected");
            } catch (ExecutionException e) {
                assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof TimeoutException);
            }
            assertEquals("a per-partition error, not a disconnect", 0.0, connectionCloses(producer), 0.0);

            producer.send(lakeRecord(topic, 1, "after")).get(30, SECONDS);
        }

        assertEquals(List.of("lake:v-ok"), consumeAll(topic, 0, "read_uncommitted").stream().map(ConsumerRecord::value).collect(Collectors.toList()));
        assertEquals(List.of("lake:v-after"), consumeAll(topic, 1, "read_uncommitted").stream().map(ConsumerRecord::value).collect(Collectors.toList()));
    }

    @Test
    public void routesToDlqAfterMaxAttemptsForSameTransaction() throws Exception {
        String topic = createTopic();
        String dlq = createTopic("__" + topic + "-dlq", 1);
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            producer.send(lakeRecord(topic, null, "k1", StubLake.FAIL, "always", "it-txn-id", id(topic, "k1"))).get(60, SECONDS);
        }

        assertEquals(3, lake.calls(id(topic, "k1")));
        List<ConsumerRecord<String, String>> routed = consumeAll(dlq, "read_uncommitted");
        assertEquals(1, routed.size());
        assertEquals("v-k1", routed.get(0).value());
        assertEquals("3", header(routed.get(0), PREFIX + "retry-attempts"));
        assertEquals(topic, header(routed.get(0), PREFIX + "original-topic"));
        assertEquals("true", header(routed.get(0), PREFIX + "error"));

        // An idempotent batch can't have sequence gaps, so the record also keeps its slot in the original topic.
        List<ConsumerRecord<String, String>> original = consumeAll(topic, "read_uncommitted");
        assertEquals(1, original.size());
        assertEquals("true", header(original.get(0), PREFIX + "error"));
    }

    @Test
    public void onExceptionDlqRoutesOnFirstFailure() throws Exception {
        String topic = createTopic();
        String dlq = createTopic("__" + topic + "-dlq", 1);
        try (KafkaProducer<String, String> producer = producer(NO_RETRIES)) {
            producer.send(lakeRecord(topic, null, "k1", StubLake.FAIL, "always", PREFIX + "onException", "dlq")).get(30, SECONDS);
        }

        assertEquals(1, lake.calls(id(topic, "k1")));
        assertEquals(List.of("v-k1"), consumeAll(dlq, "read_uncommitted").stream().map(ConsumerRecord::value).collect(Collectors.toList()));
        assertTrue("a non-idempotent batch drops the routed record", consumeAll(topic, "read_uncommitted").isEmpty());
    }

    @Test
    public void onExceptionHeadersStoresRecordWithErrorHeaders() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(NO_RETRIES)) {
            producer.send(lakeRecord(topic, null, "k1", StubLake.FAIL, "always", PREFIX + "onException", "headers")).get(30, SECONDS);
        }

        ConsumerRecord<String, String> record = consumeAll(topic, "read_uncommitted").get(0);
        assertEquals("v-k1", record.value());
        assertEquals("true", header(record, PREFIX + "error"));
        assertEquals("org.apache.kafka.common.requests.transform.HttpResponseException", header(record, PREFIX + "error-class"));
    }

    @Test
    public void onExceptionOriginalStoresRecordUnchanged() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(NO_RETRIES)) {
            producer.send(lakeRecord(topic, null, "k1", StubLake.FAIL, "always", PREFIX + "onException", "original", "uid", "u1")).get(30, SECONDS);
        }

        ConsumerRecord<String, String> record = consumeAll(topic, "read_uncommitted").get(0);
        assertEquals("v-k1", record.value());
        assertEquals("u1", header(record, "uid"));
        assertNull(header(record, PREFIX + "error"));
    }

    // The production use: the offsets post links the lake's record uid to the record's log offset.
    @Test
    public void postedOffsetCarriesLakeAssignedUid() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            producer.send(lakeRecord(topic, null, "k0")).get(30, SECONDS);
            producer.send(lakeRecord(topic, null, "k1")).get(30, SECONDS);
        }

        List<ConsumerRecord<String, String>> consumed = consumeAll(topic, "read_uncommitted");
        Map<String, JSONObject> posted = lake.awaitKeys(topic, List.of("k0", "k1"), Duration.ofSeconds(30));
        for (ConsumerRecord<String, String> record : consumed) {
            JSONObject offset = posted.get(record.key());
            assertEquals(record.offset(), offset.getLong("offset"));
            String uid = null;
            for (String name : offset.getJSONObject("headers").keySet()) {
                if (name.equalsIgnoreCase(StubLake.REC_UID)) {
                    uid = offset.getJSONObject("headers").getString(name);
                }
            }
            assertEquals("uid-" + id(topic, record.key()), uid);
        }
    }

    @Test
    public void transactionalRecordsGoThroughLakeIncludingRetries() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "it-tx-" + topic))) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(lakeRecord(topic, null, "k0"));
            producer.send(lakeRecord(topic, null, "k1", StubLake.FAIL, "1"));
            producer.commitTransaction();
        }

        List<String> values = consumeAll(topic, "read_committed").stream().map(ConsumerRecord::value).collect(Collectors.toList());
        assertEquals(2, values.size());
        assertTrue(values.toString(), values.containsAll(List.of("lake:v-k0", "lake:v-k1")));
    }
}
