package org.apache.kafka.common.requests.transform;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.json.JSONObject;

import org.junit.Test;

// The post-write interceptor posts each appended record's offset, key and headers to the lake.
public class OffsetsInterceptorIT extends XformKafkaIT {
    private static final Duration POST_TIMEOUT = Duration.ofSeconds(30);

    // Single-record requests then a multi-record batch, so each posted offset depends on its request's base offset.
    @Test
    public void postsLogOffsetOfEachRecord() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of(ProducerConfig.LINGER_MS_CONFIG, 100))) {
            for (int i = 0; i < 2; i++) {
                producer.send(record(topic, "k" + i)).get(30, SECONDS);
            }
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int i = 2; i < 7; i++) {
                sends.add(producer.send(record(topic, "k" + i)));
            }
            producer.flush();
            for (Future<RecordMetadata> send : sends) {
                send.get(30, SECONDS);
            }
        }

        List<ConsumerRecord<String, String>> consumed = consumeAll(topic, "read_uncommitted");
        assertEquals(7, consumed.size());

        Map<String, JSONObject> posted = lake.awaitKeys(topic, keys(consumed), POST_TIMEOUT);
        for (ConsumerRecord<String, String> record : consumed) {
            JSONObject offset = posted.get(record.key());
            assertEquals(record.key(), record.offset(), offset.getLong("offset"));
            assertEquals(record.partition(), offset.getInt("partition"));
            assertEquals("uid-" + record.key(), offset.getJSONObject("headers").getString("uid"));
            assertEquals(lineage(record), offset.getJSONObject("headers").getString("lineage"));
        }
        assertEquals("each record is posted once", 7, lake.received(topic).size());
    }

    // Abort and commit markers take offsets too, so committed records do not sit at batch-relative positions.
    @Test
    public void postsLogOffsetOfCommittedTransactionalRecords() throws Exception {
        String topic = createTopic();
        try (KafkaProducer<String, String> producer = producer(Map.of(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "it-tx-" + topic))) {
            producer.initTransactions();

            producer.beginTransaction();
            for (int i = 0; i < 3; i++) {
                producer.send(record(topic, "aborted-" + i));
            }
            producer.flush();
            producer.abortTransaction();

            producer.beginTransaction();
            for (int i = 0; i < 3; i++) {
                producer.send(record(topic, "committed-" + i));
            }
            producer.commitTransaction();
        }

        List<ConsumerRecord<String, String>> committed = consumeAll(topic, "read_committed");
        assertEquals(3, committed.size());

        Map<String, JSONObject> posted = lake.awaitKeys(topic, keys(committed), POST_TIMEOUT);
        for (ConsumerRecord<String, String> record : committed) {
            assertEquals(record.key(), record.offset(), posted.get(record.key()).getLong("offset"));
        }
    }

    private static ProducerRecord<String, String> record(String topic, String key) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, "v-" + key);
        record.headers().add("uid", ("uid-" + key).getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static List<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).collect(Collectors.toList());
    }
}
