package org.apache.kafka.common.requests.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.json.JSONObject;
import org.junit.Test;

public class ProducedRecordsTest {
    private static final String UID = "content-lake-api-rec-uid";
    private static final TopicPartition TP0 = new TopicPartition("article.in.djml", 0);
    private static final TopicPartition TP1 = new TopicPartition("article.in.djml", 1);

    private static SimpleRecord record(String uid, String value) {
        Header[] headers = null == uid ? new Header[0] : new Header[] {new RecordHeader(UID, uid.getBytes()), new RecordHeader("other", "x".getBytes())};
        return new SimpleRecord(1234L, "k".getBytes(), value.getBytes(), headers);
    }

    private static ProduceRequestData.PartitionProduceData partition(int index, SimpleRecord... records) {
        return new ProduceRequestData.PartitionProduceData().setIndex(index)
            .setRecords(MemoryRecords.withIdempotentRecords(CompressionType.NONE, 42L, (short) 0, 0, records));
    }

    private static ProduceRequestData requestData(ProduceRequestData.PartitionProduceData... partitions) {
        ProduceRequestData.TopicProduceDataCollection topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(new ProduceRequestData.TopicProduceData().setName(TP0.topic()).setPartitionData(Arrays.asList(partitions)));
        return new ProduceRequestData().setAcks((short) -1).setTopicData(topics);
    }

    private static ProduceResponse.PartitionResponse appended(long baseOffset) {
        return new ProduceResponse.PartitionResponse(Errors.NONE, baseOffset, -1L, 0L);
    }

    private static List<ProducedRecords.ProducedRecord> stashAndPop(ProduceRequestData data, boolean withValues,
            Map<TopicPartition, ProduceResponse.PartitionResponse> responses) {
        ProduceRequest request = new ProduceRequest(data, (short) 9);
        ProducedRecords.stash(request, data, withValues);
        return ProducedRecords.pop(request, responses);
    }

    @Test
    public void returnsEveryRecordAtItsAssignedOffset() {
        ProduceRequestData data = requestData(partition(0, record("a", "v0"), record(null, "v1"), record("c", "v2")), partition(1, record("d", "v3")));
        Map<TopicPartition, ProduceResponse.PartitionResponse> responses = new HashMap<>();
        responses.put(TP0, appended(1000));
        responses.put(TP1, appended(7));

        List<ProducedRecords.ProducedRecord> records = stashAndPop(data, false, responses);

        Map<Long, ProducedRecords.ProducedRecord> byOffset = new HashMap<>();
        for (ProducedRecords.ProducedRecord record : records) {
            byOffset.put(record.offset + (record.topicPartition.equals(TP1) ? 1_000_000 : 0), record);
        }
        assertEquals(4, records.size());
        assertEquals("a", byOffset.get(1000L).header(UID));
        assertNull(byOffset.get(1001L).header(UID));
        assertEquals("c", byOffset.get(1002L).header(UID));
        assertEquals("d", byOffset.get(1_000_007L).header(UID));
        assertEquals(1234L, byOffset.get(1000L).timestamp);
        assertEquals("k", Utils.utf8(byOffset.get(1000L).key));
    }

    @Test
    public void skipsPartitionsThatFailedOrAreMissing() {
        ProduceRequestData data = requestData(partition(0, record("a", "v")), partition(1, record("b", "v")));

        List<ProducedRecords.ProducedRecord> records = stashAndPop(data, false,
            Collections.singletonMap(TP0, new ProduceResponse.PartitionResponse(Errors.NOT_ENOUGH_REPLICAS)));

        assertTrue(records.isEmpty());
    }

    @Test
    public void stashesValuesOnlyWhenAsked() {
        ProduceRequestData data = requestData(partition(0, record("a", "v0")));

        assertNull(stashAndPop(data, false, Collections.singletonMap(TP0, appended(0))).get(0).value);
        assertEquals("v0", Utils.utf8(stashAndPop(data, true, Collections.singletonMap(TP0, appended(0))).get(0).value));
    }

    @Test
    public void stashIsKeyedByRequestIdentityAndPoppedOnce() {
        ProduceRequestData data = requestData(partition(0, record("a", "v")));
        ProduceRequest first = new ProduceRequest(data, (short) 9);
        ProduceRequest second = new ProduceRequest(data, (short) 9);
        Map<TopicPartition, ProduceResponse.PartitionResponse> responses = Collections.singletonMap(TP0, appended(0));

        ProducedRecords.stash(first, data, false);

        assertTrue(ProducedRecords.pop(second, responses).isEmpty());
        assertEquals(1, ProducedRecords.pop(first, responses).size());
        assertTrue(ProducedRecords.pop(first, responses).isEmpty());
    }

    @Test
    public void jsonHasOnlyConfiguredHeadersAndOptionalBody() {
        ProduceRequestData data = requestData(partition(0, record("a", "v0"), record(null, "v1")));
        List<ProducedRecords.ProducedRecord> records = stashAndPop(data, true, Collections.singletonMap(TP0, appended(1000)));

        JSONObject withoutBody = new JSONObject(HttpProduceResponseListener.toJson("broker-1", records, Arrays.asList(UID), false, false));
        assertEquals("broker-1", withoutBody.getString("hostname"));
        JSONObject partition = withoutBody.getJSONArray("partitions").getJSONObject(0);
        assertEquals(TP0.topic(), partition.getString("topic"));
        assertEquals(0, partition.getInt("partition"));
        JSONObject first = partition.getJSONArray("records").getJSONObject(0);
        assertEquals(1000, first.getLong("offset"));
        assertEquals("a", first.getJSONObject("headers").getString(UID));
        assertFalse(first.getJSONObject("headers").has("other"));
        assertFalse(first.has("body"));
        assertTrue(partition.getJSONArray("records").getJSONObject(1).getJSONObject("headers").isEmpty());

        JSONObject withBody = new JSONObject(HttpProduceResponseListener.toJson(null, records, Arrays.asList(UID), true, false));
        assertEquals("v0", withBody.getJSONArray("partitions").getJSONObject(0).getJSONArray("records").getJSONObject(0).getString("body"));

        JSONObject base64 = new JSONObject(HttpProduceResponseListener.toJson(null, records, Arrays.asList(UID), true, true));
        assertEquals(Base64.getEncoder().encodeToString("v0".getBytes()),
            base64.getJSONArray("partitions").getJSONObject(0).getJSONArray("records").getJSONObject(0).getString("body"));
    }

    @Test
    public void requireHeadersMatchesAnyConfiguredHeader() {
        ProduceRequestData data = requestData(partition(0, record("a", "v0"), record(null, "v1")));
        List<ProducedRecords.ProducedRecord> records = stashAndPop(data, false, Collections.singletonMap(TP0, appended(0)));

        assertTrue(HttpProduceResponseListener.hasAnyHeader(records.get(0), Arrays.asList("missing", UID.toUpperCase())));
        assertFalse(HttpProduceResponseListener.hasAnyHeader(records.get(1), Arrays.asList("missing", UID)));
        assertFalse(HttpProduceResponseListener.hasAnyHeader(records.get(0), Collections.emptyList()));
    }

    @Test
    public void lastPerPartitionKeepsHighestOffset() {
        ProduceRequestData data = requestData(partition(0, record("a", "v0"), record("b", "v1")), partition(1, record("c", "v2")));
        Map<TopicPartition, ProduceResponse.PartitionResponse> responses = new HashMap<>();
        responses.put(TP0, appended(1000));
        responses.put(TP1, appended(7));
        List<ProducedRecords.ProducedRecord> newer = stashAndPop(data, false, responses);

        responses.put(TP0, appended(500));
        List<ProducedRecords.ProducedRecord> older = stashAndPop(data, false, responses);

        Map<TopicPartition, ProducedRecords.ProducedRecord> latest = new HashMap<>();
        newer.forEach(record -> HttpProduceResponseListener.keepLatest(latest, record));
        // An earlier request completing late must not replace a later offset.
        older.forEach(record -> HttpProduceResponseListener.keepLatest(latest, record));

        assertEquals(2, latest.size());
        assertEquals(1001, latest.get(TP0).offset);
        assertEquals("b", latest.get(TP0).header(UID));
        assertEquals(7, latest.get(TP1).offset);
    }

    @Test
    public void parsesHeaderList() {
        assertEquals(Arrays.asList("a", "b"), HttpProduceResponseListener.parseList(" a, ,b "));
        assertTrue(HttpProduceResponseListener.parseList(null).isEmpty());
    }
}
