package org.apache.kafka.common.requests.transform;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Runs against a real broker with the produce request interceptor chain set to lineage only
 * (resources/docker/config), with lineage prefix "/it:" and in-time enabled.
 */
public class LineageInterceptorIT {
    private static XformKafkaContainer kafka;
    private static Admin admin;

    @Rule
    public TestName testName = new TestName();

    @Rule
    public TestWatcher brokerLogsOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            System.err.println("Broker logs for " + description + ":\n" + kafka.getLogs());
        }
    };

    @BeforeClass
    public static void startBroker() {
        kafka = new XformKafkaContainer();
        kafka.start();
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    @AfterClass
    public static void stopBroker() {
        if (null != admin) {
            admin.close();
        }
        if (null != kafka) {
            kafka.stop();
        }
    }

    @Test
    public void addsLineageHeader() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            producer.send(new ProducerRecord<>(topic, "k1", "v1")).get(30, SECONDS);
        }

        List<ConsumerRecord<String, String>> records = consumeAll(topic, "read_uncommitted");

        assertEquals(1, records.size());
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals("k1", record.key());
        assertEquals("v1", record.value());
        assertLineage("/it:" + Pattern.quote(topic) + ":\\d+", record);
    }

    @Test
    public void appendsToExistingLineage() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "k1", "v1");
            record.headers().add("lineage", "/it:upstream:1".getBytes(StandardCharsets.UTF_8));
            producer.send(record).get(30, SECONDS);
        }

        List<ConsumerRecord<String, String>> records = consumeAll(topic, "read_uncommitted");

        assertEquals(1, records.size());
        assertLineage("/it:upstream:1/it:" + Pattern.quote(topic) + ":\\d+", records.get(0));
        assertEquals("lineage header is replaced, not duplicated", 1, count(records.get(0).headers().headers("lineage")));
    }

    // The interceptor rebuilds each batch; the broker rejects it if producer id or sequences are lost.
    @Test
    public void idempotentBatchIsAcceptedInOrder() throws Exception {
        String topic = createTopic();
        int count = 100;
        try (KafkaProducer<String, String> producer = producer(Map.of(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 100))) {
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                sends.add(producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i)));
            }
            producer.flush();
            for (Future<RecordMetadata> send : sends) {
                send.get(30, SECONDS);
            }
        }

        List<ConsumerRecord<String, String>> records = consumeAll(topic, "read_uncommitted");

        assertEquals(count, records.size());
        for (int i = 0; i < count; i++) {
            ConsumerRecord<String, String> record = records.get(i);
            assertEquals(i, record.offset());
            assertEquals("v" + i, record.value());
            assertLineage("/it:" + Pattern.quote(topic) + ":\\d+", record);
        }
    }

    // If the rebuilt batch lost its transactional flag, read_committed consumers would see aborted records.
    @Test
    public void transactionalBatchKeepsAbortSemantics() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "it-tx-" + topic))) {
            producer.initTransactions();

            producer.beginTransaction();
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(topic, "aborted-" + i));
            }
            producer.flush();
            producer.abortTransaction();

            producer.beginTransaction();
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(topic, "committed-" + i));
            }
            producer.commitTransaction();
        }

        List<ConsumerRecord<String, String>> committed = consumeAll(topic, "read_committed");
        assertEquals(List.of("committed-0", "committed-1", "committed-2"), values(committed));
        for (ConsumerRecord<String, String> record : committed) {
            assertLineage("/it:" + Pattern.quote(topic) + ":\\d+", record);
        }

        assertEquals(6, consumeAll(topic, "read_uncommitted").size());
    }

    private String createTopic() throws Exception {
        String topic = "it-" + testName.getMethodName();
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, SECONDS);
        return topic;
    }

    private static KafkaProducer<String, String> producer(Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        props.putAll(overrides);
        return new KafkaProducer<>(props);
    }

    // Reads partition 0 up to its end offset, which for read_committed is the last stable offset.
    private static List<ConsumerRecord<String, String>> consumeAll(String topic, String isolationLevel) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        TopicPartition partition = new TopicPartition(topic, 0);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.currentTimeMillis() + 30_000;
            while (consumer.position(partition) < end) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("Timed out reading " + partition + " up to offset " + end + ", got " + records.size() + " records");
                }
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        return records;
    }

    private static void assertLineage(String expectedRegex, ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("lineage");
        assertNotNull("lineage header missing at offset " + record.offset(), header);
        String lineage = new String(header.value(), StandardCharsets.UTF_8);
        assertTrue("lineage '" + lineage + "' does not match " + expectedRegex, Pattern.matches(expectedRegex, lineage));
    }

    private static List<String> values(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    }

    private static int count(Iterable<Header> headers) {
        int count = 0;
        for (Header ignored : headers) {
            count++;
        }
        return count;
    }
}
