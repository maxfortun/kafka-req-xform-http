package org.apache.kafka.common.requests.transform;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;

/** Builds produce requests and reads records back out of them. */
final class ProduceRequests {
    static final long PRODUCER_ID = 4242L;
    static final short PRODUCER_EPOCH = 7;
    static final int BASE_SEQUENCE = 100;

    private final ProduceRequestData data = new ProduceRequestData().setAcks((short) -1).setTimeoutMs(30000);

    private ProduceRequests() {
    }

    static ProduceRequests request() {
        return new ProduceRequests();
    }

    ProduceRequests add(String topic, int partition, MemoryRecords records) {
        ProduceRequestData.TopicProduceData topicData = data.topicData().find(topic);
        if (null == topicData) {
            topicData = new ProduceRequestData.TopicProduceData().setName(topic);
            data.topicData().add(topicData);
        }
        topicData.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(partition).setRecords(records));
        return this;
    }

    ProduceRequestData build() {
        return data;
    }

    static SimpleRecord record(String key, String value, String... headerKeyValues) {
        Header[] headers = new Header[headerKeyValues.length / 2];
        for (int i = 0; i < headers.length; i++) {
            headers[i] = new RecordHeader(headerKeyValues[2 * i], bytes(headerKeyValues[2 * i + 1]));
        }
        return new SimpleRecord(1000L, bytes(key), bytes(value), headers);
    }

    static MemoryRecords plain(SimpleRecord... records) {
        return MemoryRecords.withRecords(CompressionType.NONE, records);
    }

    static MemoryRecords idempotent(SimpleRecord... records) {
        return MemoryRecords.withIdempotentRecords(CompressionType.NONE, PRODUCER_ID, PRODUCER_EPOCH, BASE_SEQUENCE, records);
    }

    static MemoryRecords transactional(SimpleRecord... records) {
        return MemoryRecords.withTransactionalRecords(CompressionType.NONE, PRODUCER_ID, PRODUCER_EPOCH, BASE_SEQUENCE, records);
    }

    static MemoryRecords concat(MemoryRecords... parts) {
        int size = 0;
        for (MemoryRecords part : parts) {
            size += part.sizeInBytes();
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (MemoryRecords part : parts) {
            buffer.put(part.buffer().duplicate());
        }
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    static MemoryRecords records(ProduceRequestData data, String topic, int partition) {
        ProduceRequestData.TopicProduceData topicData = data.topicData().find(topic);
        if (null == topicData) {
            return null;
        }
        for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
            if (partitionData.index() == partition) {
                return (MemoryRecords) partitionData.records();
            }
        }
        return null;
    }

    static List<RecordBatch> batches(MemoryRecords records) {
        List<RecordBatch> batches = new ArrayList<>();
        records.batches().forEach(batches::add);
        return batches;
    }

    static List<Record> recordList(MemoryRecords records) {
        List<Record> list = new ArrayList<>();
        records.records().forEach(list::add);
        return list;
    }

    static String header(Record record, String key) {
        String value = null;
        for (Header header : record.headers()) {
            if (key.equals(header.key())) {
                value = null == header.value() ? null : new String(header.value(), StandardCharsets.UTF_8);
            }
        }
        return value;
    }

    static String key(Record record) {
        return Utils.utf8(record.key());
    }

    static String value(Record record) {
        return Utils.utf8(record.value());
    }

    static byte[] bytes(String value) {
        return null == value ? null : value.getBytes(StandardCharsets.UTF_8);
    }
}
