package org.apache.kafka.common.requests.transform;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.testcontainers.Testcontainers;

/**
 * Broker with the interceptor chains from resources/docker/config: pre-write lineage,content-lake (content-lake disabled),
 * post-write content-lake-offsets posting to an OffsetsLake in this JVM.
 */
public abstract class XformKafkaIT {
    protected static XformKafkaContainer kafka;
    protected static OffsetsLake lake;
    protected static Admin admin;

    @Rule
    public TestName testName = new TestName();

    @Rule
    public TestWatcher brokerLogsOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            System.err.println("Broker logs for " + description + ":\n" + kafka.getLogs());
        }
    };

    // One broker for all test classes in the JVM; Testcontainers removes it when the JVM exits.
    @BeforeClass
    public static synchronized void startBroker() throws IOException {
        if (null != kafka) {
            return;
        }
        lake = new OffsetsLake();
        Testcontainers.exposeHostPorts(lake.port());
        XformKafkaContainer container = new XformKafkaContainer()
            .withOffsetsUri("http://host.testcontainers.internal:" + lake.port() + "/offsets");
        container.start();
        kafka = container;
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    protected String createTopic() throws Exception {
        String topic = "it-" + getClass().getSimpleName() + "-" + testName.getMethodName();
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, SECONDS);
        return topic;
    }

    protected static KafkaProducer<String, String> producer(Map<String, Object> overrides) {
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
    protected static List<ConsumerRecord<String, String>> consumeAll(String topic, String isolationLevel) {
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

    protected static String lineage(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("lineage");
        assertNotNull("lineage header missing at offset " + record.offset(), header);
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
