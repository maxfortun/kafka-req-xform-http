package org.apache.kafka.common.requests.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.Test;

public class BatchPreservationTest {
    private static final long PRODUCER_ID = 4242L;
    private static final short PRODUCER_EPOCH = 7;
    private static final int BASE_SEQUENCE = 100;
    private static final int LEADER_EPOCH = 3;

    private static Record addHeader(AbstractTransformer transformer, RecordBatch recordBatch, Record record, RecordHeaders recordHeaders) {
        try {
            recordHeaders.add("x-test", "1".getBytes());
            return transformer.newRecord(recordBatch, record, recordHeaders.toArray(), record.value());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class HeaderProduceTransformer extends AbstractProduceRequestDataTransformer {
        HeaderProduceTransformer() {
            super("batch-preservation-test");
        }

        @Override
        protected Record transform(ProduceRequestData.TopicProduceData topicProduceData, ProduceRequestData.PartitionProduceData partitionProduceData,
                RecordBatch recordBatch, Record record, RecordHeaders recordHeaders, short version) {
            return addHeader(this, recordBatch, record, recordHeaders);
        }
    }

    private static class HeaderFetchTransformer extends AbstractFetchResponseDataTransformer {
        HeaderFetchTransformer() {
            super("batch-preservation-test");
        }

        @Override
        protected Record transform(FetchResponseData.FetchableTopicResponse fetchableTopicResponse, FetchResponseData.PartitionData partitionData,
                RecordBatch recordBatch, Record record, RecordHeaders recordHeaders, short version) {
            return addHeader(this, recordBatch, record, recordHeaders);
        }
    }

    private static SimpleRecord[] simpleRecords() {
        return new SimpleRecord[] {
            new SimpleRecord(1000L, "k0".getBytes(), "v0".getBytes()),
            new SimpleRecord(1001L, "k1".getBytes(), "v1".getBytes()),
            new SimpleRecord(1002L, "k2".getBytes(), "v2".getBytes())
        };
    }

    private static List<RecordBatch> batches(MemoryRecords memoryRecords) {
        List<RecordBatch> batches = new ArrayList<>();
        memoryRecords.batches().forEach(batches::add);
        return batches;
    }

    private static void assertSameProducerState(RecordBatch expected, RecordBatch actual) {
        assertEquals(expected.producerId(), actual.producerId());
        assertEquals(expected.producerEpoch(), actual.producerEpoch());
        assertEquals(expected.baseSequence(), actual.baseSequence());
        assertEquals(expected.lastSequence(), actual.lastSequence());
        assertEquals(expected.isTransactional(), actual.isTransactional());
        assertEquals(expected.isControlBatch(), actual.isControlBatch());
        assertEquals(expected.baseOffset(), actual.baseOffset());
        assertEquals(expected.lastOffset(), actual.lastOffset());
        assertEquals(expected.partitionLeaderEpoch(), actual.partitionLeaderEpoch());
    }

    private static void assertTransformed(RecordBatch batch) {
        for (Record record : batch) {
            boolean found = false;
            for (Header header : record.headers()) {
                found |= "x-test".equals(header.key());
            }
            assertTrue("record at offset " + record.offset() + " was not transformed", found);
        }
    }

    private MemoryRecords produce(MemoryRecords in) {
        ProduceRequestData.PartitionProduceData partition = new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(in);
        ProduceRequestData.TopicProduceDataCollection topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(new ProduceRequestData.TopicProduceData().setName("t").setPartitionData(Collections.singletonList(partition)));
        ProduceRequestData out = new HeaderProduceTransformer().transform(new ProduceRequestData().setAcks((short) -1).setTopicData(topics), (short) 9);
        return (MemoryRecords) out.topicData().find("t").partitionData().get(0).records();
    }

    private MemoryRecords fetch(MemoryRecords in) {
        FetchResponseData.PartitionData partition = new FetchResponseData.PartitionData().setPartitionIndex(0).setRecords(in);
        FetchResponseData.FetchableTopicResponse topic = new FetchResponseData.FetchableTopicResponse().setTopic("t")
            .setPartitions(Collections.singletonList(partition));
        FetchResponseData out = new HeaderFetchTransformer().transform(new FetchResponseData().setResponses(Collections.singletonList(topic)), (short) 12);
        return (MemoryRecords) out.responses().get(0).partitions().get(0).records();
    }

    @Test
    public void produceKeepsIdempotentProducerState() {
        MemoryRecords in = MemoryRecords.withIdempotentRecords(CompressionType.NONE, PRODUCER_ID, PRODUCER_EPOCH, BASE_SEQUENCE, simpleRecords());
        List<RecordBatch> outBatches = batches(produce(in));

        assertEquals(1, outBatches.size());
        assertSameProducerState(batches(in).get(0), outBatches.get(0));
        assertFalse(outBatches.get(0).isTransactional());
        assertTransformed(outBatches.get(0));
    }

    @Test
    public void produceKeepsTransactionalFlag() {
        MemoryRecords in = MemoryRecords.withTransactionalRecords(CompressionType.NONE, PRODUCER_ID, PRODUCER_EPOCH, BASE_SEQUENCE, simpleRecords());
        List<RecordBatch> outBatches = batches(produce(in));

        assertEquals(1, outBatches.size());
        assertTrue(outBatches.get(0).isTransactional());
        assertSameProducerState(batches(in).get(0), outBatches.get(0));
        assertTransformed(outBatches.get(0));
    }

    @Test
    public void fetchKeepsOffsetsProducerStateAndControlBatches() {
        long baseOffset = 5_000_000_000L; // beyond int range, to catch offset truncation
        MemoryRecords data = MemoryRecords.withTransactionalRecords(baseOffset, CompressionType.NONE, PRODUCER_ID, PRODUCER_EPOCH,
            BASE_SEQUENCE, LEADER_EPOCH, simpleRecords());
        MemoryRecords marker = MemoryRecords.withEndTransactionMarker(baseOffset + 3, 2000L, LEADER_EPOCH, PRODUCER_ID, PRODUCER_EPOCH,
            new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        ByteBuffer buffer = ByteBuffer.allocate(data.sizeInBytes() + marker.sizeInBytes());
        buffer.put(data.buffer().duplicate()).put(marker.buffer().duplicate()).flip();
        MemoryRecords in = MemoryRecords.readableRecords(buffer);

        List<RecordBatch> inBatches = batches(in);
        List<RecordBatch> outBatches = batches(fetch(in));

        assertEquals(2, outBatches.size());
        assertSameProducerState(inBatches.get(0), outBatches.get(0));
        assertTransformed(outBatches.get(0));

        long expectedOffset = baseOffset;
        for (Record record : outBatches.get(0)) {
            assertEquals(expectedOffset++, record.offset());
        }

        // Control batch is passed through byte for byte.
        assertSameProducerState(inBatches.get(1), outBatches.get(1));
        assertTrue(outBatches.get(1).isControlBatch());
        assertEquals(inBatches.get(1).checksum(), outBatches.get(1).checksum());
    }
}
