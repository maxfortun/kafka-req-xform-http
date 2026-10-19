package org.apache.kafka.common.requests.transform;

import static org.apache.kafka.common.requests.transform.ProduceRequests.batches;
import static org.apache.kafka.common.requests.transform.ProduceRequests.concat;
import static org.apache.kafka.common.requests.transform.ProduceRequests.header;
import static org.apache.kafka.common.requests.transform.ProduceRequests.idempotent;
import static org.apache.kafka.common.requests.transform.ProduceRequests.key;
import static org.apache.kafka.common.requests.transform.ProduceRequests.plain;
import static org.apache.kafka.common.requests.transform.ProduceRequests.record;
import static org.apache.kafka.common.requests.transform.ProduceRequests.recordList;
import static org.apache.kafka.common.requests.transform.ProduceRequests.records;
import static org.apache.kafka.common.requests.transform.ProduceRequests.request;
import static org.apache.kafka.common.requests.transform.ProduceRequests.transactional;
import static org.apache.kafka.common.requests.transform.ProduceRequests.value;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.InterceptResult;
import org.apache.kafka.common.requests.transform.AbstractProduceRequestDataInterceptor.PartitionErrorException;
import org.junit.Rule;
import org.junit.Test;

public class AbstractProduceRequestDataInterceptorTest {
    private static final short VERSION = 9;

    @Rule
    public TransformerConfig config = new TransformerConfig("interceptor");

    interface Script {
        Record apply(ScriptedInterceptor self, String topic, int partition, RecordBatch batch, Record record, RecordHeaders headers) throws Exception;
    }

    static class ScriptedInterceptor extends AbstractProduceRequestDataInterceptor {
        private final Script script;
        boolean stash = true;
        boolean stashValue = false;

        ScriptedInterceptor(String name, Script script) {
            super(name);
            this.script = script;
        }

        @Override
        protected Record intercept(ProduceRequestData.TopicProduceData topic, ProduceRequestData.PartitionProduceData partition,
                RecordBatch batch, Record record, RecordHeaders headers, short version) throws Exception {
            return script.apply(this, topic.name(), partition.index(), batch, record, headers);
        }

        @Override
        protected boolean shouldStash() {
            return stash;
        }

        @Override
        protected boolean shouldStashValue() {
            return stashValue;
        }

        Record withHeader(RecordBatch batch, Record record, RecordHeaders headers, String key, String value) throws Exception {
            headers.add(key, ProduceRequests.bytes(value));
            return newRecord(batch, record, headers.toArray(), record.value());
        }

        String dlqHeader() {
            return headerPrefix + "dlq-topic";
        }
    }

    private ScriptedInterceptor tagging() {
        return new ScriptedInterceptor(config.name(), (self, topic, partition, batch, record, headers) ->
            self.withHeader(batch, record, headers, "x-tag", topic + "-" + partition));
    }

    // Unique per test, since the stash is keyed by request equality.
    private String topic(String suffix) {
        return config.name() + "-" + suffix;
    }

    private static List<String> keys(MemoryRecords records) {
        return recordList(records).stream().map(ProduceRequests::key).collect(Collectors.toList());
    }

    @Test
    public void transformsEveryRecordOfEveryPartition() {
        String a = topic("a");
        String b = topic("b");
        ProduceRequestData in = request()
            .add(a, 0, plain(record("k0", "v0"), record("k1", "v1")))
            .add(a, 1, plain(record("k2", "v2")))
            .add(b, 0, plain(record("k3", "v3")))
            .build();

        InterceptResult result = tagging().intercept(in, VERSION);

        assertFalse(result.hasErrors());
        for (Record record : recordList(records(result.data(), a, 0))) {
            assertEquals(a + "-0", header(record, "x-tag"));
        }
        assertEquals(a + "-1", header(recordList(records(result.data(), a, 1)).get(0), "x-tag"));
        Record last = recordList(records(result.data(), b, 0)).get(0);
        assertEquals(b + "-0", header(last, "x-tag"));
        assertEquals("k3", key(last));
        assertEquals("v3", value(last));
    }

    @Test
    public void inputRequestIsNotModified() {
        String t = topic("t");
        ProduceRequestData in = request().add(t, 0, plain(record("k0", "v0"))).build();

        tagging().intercept(in, VERSION);

        assertNull(header(recordList(records(in, t, 0)).get(0), "x-tag"));
    }

    @Test
    public void keepsIdempotentProducerState() {
        String t = topic("t");
        MemoryRecords original = idempotent(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"));

        InterceptResult result = tagging().intercept(request().add(t, 0, original).build(), VERSION);

        RecordBatch in = batches(original).get(0);
        List<RecordBatch> out = batches(records(result.data(), t, 0));
        assertEquals(1, out.size());
        assertEquals(in.producerId(), out.get(0).producerId());
        assertEquals(in.producerEpoch(), out.get(0).producerEpoch());
        assertEquals(in.baseSequence(), out.get(0).baseSequence());
        assertEquals(in.lastSequence(), out.get(0).lastSequence());
        assertFalse(out.get(0).isTransactional());
    }

    @Test
    public void keepsTransactionalFlagAndPassesControlBatchesThrough() {
        String t = topic("t");
        MemoryRecords data = transactional(record("k0", "v0"), record("k1", "v1"));
        MemoryRecords marker = MemoryRecords.withEndTransactionMarker(2L, 2000L, 0, ProduceRequests.PRODUCER_ID,
            ProduceRequests.PRODUCER_EPOCH, new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        InterceptResult result = tagging().intercept(request().add(t, 0, concat(data, marker)).build(), VERSION);

        List<RecordBatch> out = batches(records(result.data(), t, 0));
        assertEquals(2, out.size());
        assertTrue(out.get(0).isTransactional());
        assertEquals(t + "-0", header(out.get(0).iterator().next(), "x-tag"));
        assertTrue(out.get(1).isControlBatch());
        assertEquals(batches(marker).get(0).checksum(), out.get(1).checksum());
    }

    @Test
    public void partitionErrorFailsOnlyThatPartition() {
        String t = topic("t");
        ScriptedInterceptor interceptor = new ScriptedInterceptor(config.name(), (self, topic, partition, batch, record, headers) -> {
            if (partition == 1) {
                throw new PartitionErrorException(Errors.NOT_ENOUGH_REPLICAS, "lake down");
            }
            return self.withHeader(batch, record, headers, "x-tag", "ok");
        });

        InterceptResult result = interceptor.intercept(request()
            .add(t, 0, plain(record("k0", "v0")))
            .add(t, 1, plain(record("k1", "v1")))
            .build(), VERSION);

        assertEquals(Map.of(new TopicPartition(t, 1), Errors.NOT_ENOUGH_REPLICAS), result.partitionErrors());
        assertEquals(0, records(result.data(), t, 1).sizeInBytes());
        assertEquals("ok", header(recordList(records(result.data(), t, 0)).get(0), "x-tag"));
    }

    @Test
    public void unexpectedExceptionIsReportedAsRetriableTimeout() {
        String t = topic("t");
        ScriptedInterceptor interceptor = new ScriptedInterceptor(config.name(), (self, topic, partition, batch, record, headers) -> {
            throw new IllegalStateException("boom");
        });

        InterceptResult result = interceptor.intercept(request().add(t, 0, plain(record("k0", "v0"))).build(), VERSION);

        assertEquals(Map.of(new TopicPartition(t, 0), Errors.REQUEST_TIMED_OUT), result.partitionErrors());
        assertEquals(0, records(result.data(), t, 0).sizeInBytes());
    }

    @Test
    public void failureOnLaterRecordDiscardsTheWholePartition() {
        String t = topic("t");
        ScriptedInterceptor interceptor = new ScriptedInterceptor(config.name(), (self, topic, partition, batch, record, headers) -> {
            if ("k1".equals(key(record))) {
                throw new PartitionErrorException(Errors.INVALID_RECORD, "bad");
            }
            return record;
        });

        InterceptResult result = interceptor.intercept(request().add(t, 0, plain(record("k0", "v0"), record("k1", "v1"))).build(), VERSION);

        assertEquals(Errors.INVALID_RECORD, result.partitionErrors().get(new TopicPartition(t, 0)));
        assertEquals("no partial writes", 0, records(result.data(), t, 0).sizeInBytes());
    }

    @Test
    public void topicsNotMatchingPatternAreUntouched() {
        config.set("topics.namePattern", "^.*-keep$");
        String keep = topic("keep");
        String skip = topic("skip");
        MemoryRecords skipped = plain(record("k1", "v1"));

        InterceptResult result = tagging().intercept(request()
            .add(keep, 0, plain(record("k0", "v0")))
            .add(skip, 0, skipped)
            .build(), VERSION);

        assertEquals(keep + "-0", header(recordList(records(result.data(), keep, 0)).get(0), "x-tag"));
        assertNull(header(recordList(records(result.data(), skip, 0)).get(0), "x-tag"));
        InterceptorStash.StashedRequest stash = InterceptorStash.pop(result.data());
        assertNotNull(stash);
        assertFalse(stash.partitions.containsKey(new TopicPartition(skip, 0)));
    }

    private ScriptedInterceptor dlqForKeys(String dlqTopic, String... dlqKeys) {
        List<String> routed = List.of(dlqKeys);
        return new ScriptedInterceptor(config.name(), (self, topic, partition, batch, record, headers) ->
            routed.contains(key(record)) ? self.withHeader(batch, record, headers, self.dlqHeader(), dlqTopic) : record);
    }

    @Test
    public void dlqHeaderMovesRecordToDlqTopic() {
        String t = topic("t");
        String dlq = topic("dlq");
        ScriptedInterceptor interceptor = dlqForKeys(dlq, "k1");

        InterceptResult result = interceptor.intercept(request().add(t, 2, plain(record("k0", "v0"), record("k1", "v1"))).build(), VERSION);

        assertEquals(List.of("k0"), keys(records(result.data(), t, 2)));
        List<Record> routed = recordList(records(result.data(), dlq, 2));
        assertEquals(1, routed.size());
        assertEquals("k1", key(routed.get(0)));
        assertEquals("v1", value(routed.get(0)));
        assertNull("routing header is removed", header(routed.get(0), interceptor.dlqHeader()));
        assertFalse(result.hasErrors());
    }

    // Idempotent batches can't have sequence gaps, so the routed record also keeps its slot in the original topic.
    @Test
    public void dlqRecordKeepsItsSlotInIdempotentBatch() {
        String t = topic("t");
        String dlq = topic("dlq");
        MemoryRecords original = idempotent(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"));

        InterceptResult result = dlqForKeys(dlq, "k1").intercept(request().add(t, 0, original).build(), VERSION);

        MemoryRecords out = records(result.data(), t, 0);
        assertEquals(List.of("k0", "k1", "k2"), keys(out));
        assertEquals(batches(original).get(0).lastSequence(), batches(out).get(0).lastSequence());
        assertEquals(List.of("k1"), keys(records(result.data(), dlq, 0)));
    }

    // Each producer batch numbers its records from 0, so DLQ records must get their own offsets.
    @Test
    public void dlqCollectsRecordsFromSeveralBatches() {
        String t = topic("t");
        String dlq = topic("dlq");
        MemoryRecords twoBatches = concat(plain(record("a0", "v"), record("a1", "v")), plain(record("b0", "v"), record("b1", "v")));

        InterceptResult result = dlqForKeys(dlq, "a0", "b0").intercept(request().add(t, 0, twoBatches).build(), VERSION);

        List<Record> routed = recordList(records(result.data(), dlq, 0));
        assertEquals(List.of("a0", "b0"), routed.stream().map(ProduceRequests::key).collect(Collectors.toList()));
        assertEquals(0L, routed.get(0).offset());
        assertEquals(1L, routed.get(1).offset());
        assertEquals(List.of("a1", "b1"), keys(records(result.data(), t, 0)));
    }

    @Test
    public void dlqRecordsKeepTheirPartitionIndex() {
        String t = topic("t");
        String dlq = topic("dlq");

        InterceptResult result = dlqForKeys(dlq, "p0", "p3").intercept(request()
            .add(t, 0, plain(record("p0", "v")))
            .add(t, 3, plain(record("p3", "v")))
            .build(), VERSION);

        assertEquals(List.of("p0"), keys(records(result.data(), dlq, 0)));
        assertEquals(List.of("p3"), keys(records(result.data(), dlq, 3)));
    }

    @Test
    public void stashesKeysHeadersAndTimestampsPerPartitionInOrder() {
        String t = topic("t");
        InterceptResult result = tagging().intercept(request()
            .add(t, 0, plain(record("k0", "v0", "uid", "u0"), record("k1", "v1", "uid", "u1")))
            .add(t, 1, plain(record("k2", "v2")))
            .build(), VERSION);

        InterceptorStash.StashedRequest stash = InterceptorStash.pop(result.data());
        assertNotNull(stash);
        List<InterceptorStash.StashedRecord> p0 = stash.partitions.get(new TopicPartition(t, 0)).records;
        assertEquals(List.of("k0", "k1"), p0.stream().map(r -> r.key).collect(Collectors.toList()));
        assertEquals("u1", p0.get(1).headers.get("uid"));
        assertEquals(t + "-0", p0.get(1).headers.get("x-tag"));
        assertEquals(1000L, p0.get(0).timestamp);
        assertNull("values are not stashed by default", p0.get(0).value);
        assertEquals(1, stash.partitions.get(new TopicPartition(t, 1)).records.size());
        assertFalse("pop removes the stash", InterceptorStash.hasStash(result.data()));
    }

    @Test
    public void stashesValuesWhenEnabled() {
        String t = topic("t");
        ScriptedInterceptor interceptor = tagging();
        interceptor.stashValue = true;

        InterceptResult result = interceptor.intercept(request().add(t, 0, plain(record("k0", "v0"))).build(), VERSION);

        InterceptorStash.StashedRequest stash = InterceptorStash.pop(result.data());
        assertArrayEquals(ProduceRequests.bytes("v0"), stash.partitions.get(new TopicPartition(t, 0)).records.get(0).value);
    }

    @Test
    public void failedPartitionsAreNotStashed() {
        String t = topic("t");
        ScriptedInterceptor interceptor = new ScriptedInterceptor(config.name(), (self, topic, partition, batch, record, headers) -> {
            if (partition == 1) {
                throw new PartitionErrorException(Errors.REQUEST_TIMED_OUT, "down");
            }
            return record;
        });

        InterceptResult result = interceptor.intercept(request()
            .add(t, 0, plain(record("k0", "v0")))
            .add(t, 1, plain(record("k1", "v1")))
            .build(), VERSION);

        InterceptorStash.StashedRequest stash = InterceptorStash.pop(result.data());
        assertTrue(stash.partitions.containsKey(new TopicPartition(t, 0)));
        assertFalse(stash.partitions.containsKey(new TopicPartition(t, 1)));
    }

    @Test
    public void nothingIsStashedWhenStashingIsOff() {
        String t = topic("t");
        ScriptedInterceptor interceptor = tagging();
        interceptor.stash = false;

        InterceptResult result = interceptor.intercept(request().add(t, 0, plain(record("k0", "v0"))).build(), VERSION);

        assertFalse(InterceptorStash.hasStash(result.data()));
    }

    @Test
    public void identityInterceptorKeepsRecordsEquivalent() {
        String t = topic("t");
        ScriptedInterceptor identity = new ScriptedInterceptor(config.name(), (self, topic, partition, batch, record, headers) -> record);
        identity.stash = false;
        MemoryRecords original = idempotent(record("k0", "v0", "h", "1"), record("k1", "v1"));

        InterceptResult result = identity.intercept(request().add(t, 0, original).build(), VERSION);

        List<Record> in = recordList(original);
        List<Record> out = recordList(records(result.data(), t, 0));
        assertEquals(in.size(), out.size());
        for (int i = 0; i < in.size(); i++) {
            assertEquals(in.get(i).offset(), out.get(i).offset());
            assertEquals(key(in.get(i)), key(out.get(i)));
            assertEquals(value(in.get(i)), value(out.get(i)));
            assertEquals(header(in.get(i), "h"), header(out.get(i), "h"));
        }
        assertSame(Errors.NONE, result.partitionErrors().getOrDefault(new TopicPartition(t, 0), Errors.NONE));
    }
}
