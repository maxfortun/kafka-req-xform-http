package org.apache.kafka.common.requests.transform;

import static org.apache.kafka.common.requests.transform.ProduceRequests.header;
import static org.apache.kafka.common.requests.transform.ProduceRequests.idempotent;
import static org.apache.kafka.common.requests.transform.ProduceRequests.key;
import static org.apache.kafka.common.requests.transform.ProduceRequests.plain;
import static org.apache.kafka.common.requests.transform.ProduceRequests.record;
import static org.apache.kafka.common.requests.transform.ProduceRequests.recordList;
import static org.apache.kafka.common.requests.transform.ProduceRequests.records;
import static org.apache.kafka.common.requests.transform.ProduceRequests.request;
import static org.apache.kafka.common.requests.transform.ProduceRequests.value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.ServerSocket;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.InterceptResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class HttpProduceRequestDataInterceptorTest {
    private static final short VERSION = 9;

    @Rule
    public TransformerConfig config = new TransformerConfig("http-interceptor");

    private StubHttpServer lake;
    private String topic;

    @Before
    public void startLake() throws Exception {
        lake = new StubHttpServer().respond(request -> new StubHttpServer.Response(200, "lake:" + request.bodyString())
            .header("X-Lake-Uid", "uid-" + request.header("kafka.KEY")));
        topic = config.name() + "-t";
    }

    @After
    public void stopLake() {
        lake.close();
    }

    private HttpProduceRequestDataInterceptor interceptor() throws Exception {
        config.setIfAbsent("enable", "true")
            .setIfAbsent("uri", lake.url("/lake"))
            .set("httpClient.class", AHC5HttpClient.class.getName())
            .set("httpClient.connectTimeout", "5")
            .set("httpClient.responseTimeout", "5");
        return new HttpProduceRequestDataInterceptor(config.name());
    }

    private InterceptResult send(HttpProduceRequestDataInterceptor interceptor, MemoryRecords records) {
        return interceptor.intercept(request().add(topic, 0, records).build(), VERSION);
    }

    private Record only(InterceptResult result) {
        List<Record> out = recordList(records(result.data(), topic, 0));
        assertEquals(1, out.size());
        return out.get(0);
    }

    private static String headerIgnoreCase(Record record, String key) {
        for (Header header : record.headers()) {
            if (key.equalsIgnoreCase(header.key())) {
                return Utils.utf8(header.value());
            }
        }
        return null;
    }

    // Errors.NONE marks a partition the interceptor finished without a write, e.g. all records sent to the DLQ.
    private static boolean failed(InterceptResult result) {
        return result.partitionErrors().values().stream().anyMatch(error -> Errors.NONE != error);
    }

    private void lakeFails() {
        lake.respond(request -> new StubHttpServer.Response(500, "lake is down"));
    }

    @Test
    public void replacesRecordWithLakeResponse() throws Exception {
        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        assertFalse(failed(result));
        Record out = only(result);
        assertEquals("lake:v0", value(out));
        assertEquals("k0", key(out));
        assertEquals("uid-k0", headerIgnoreCase(out, "X-Lake-Uid"));
    }

    @Test
    public void requestCarriesRecordAndBrokerContext() throws Exception {
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        send(interceptor, plain(record("k0", "v0", "uid", "u0")));

        StubHttpServer.Request request = lake.requests().get(0);
        String prefix = interceptor.headerPrefix;
        assertEquals("/lake", request.path);
        assertEquals("v0", request.bodyString());
        assertEquals("k0", request.header("kafka.KEY"));
        assertEquals("u0", request.header("uid"));
        assertEquals(topic, request.header(prefix + "topic-name"));
        assertEquals("0", request.header(prefix + "partition-index"));
        assertEquals("0", request.header(prefix + "record-offset"));
        assertNotNull(request.header(prefix + "req-time"));
    }

    @Test
    public void prefixedRecordHeadersAreNotSentToLake() throws Exception {
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        send(interceptor, plain(record("k0", "v0", interceptor.headerPrefix + "secret", "x", "uid", "u0")));

        StubHttpServer.Request request = lake.requests().get(0);
        assertNull(request.header(interceptor.headerPrefix + "secret"));
        assertEquals("u0", request.header("uid"));
    }

    @Test
    public void persistentPatternLimitsForwardedHeaders() throws Exception {
        config.set("headers.persistentPattern", "^keep-.*$");
        send(interceptor(), plain(record("k0", "v0", "keep-a", "1", "drop-b", "2")));

        StubHttpServer.Request request = lake.requests().get(0);
        assertEquals("1", request.header("keep-a"));
        assertNull(request.header("drop-b"));
    }

    @Test
    public void staticHttpHeadersAreAdded() throws Exception {
        config.set("headers.http", "x-static=1, x-other=2");
        send(interceptor(), plain(record("k0", "v0")));

        StubHttpServer.Request request = lake.requests().get(0);
        assertEquals("1", request.header("x-static"));
        assertEquals("2", request.header("x-other"));
    }

    @Test
    public void outputHeadersComeFromLakeResponse() throws Exception {
        Record out = only(send(interceptor(), plain(record("k0", "v0", "uid", "u0"))));

        assertNull("request headers are not copied unless transient", header(out, "uid"));
        assertEquals("uid-k0", headerIgnoreCase(out, "X-Lake-Uid"));
    }

    @Test
    public void transientPatternCopiesRequestHeadersToOutput() throws Exception {
        config.set("headers.transientPattern", "(?i)^(lineage).*$");
        Record out = only(send(interceptor(), plain(record("k0", "v0", "lineage", "/a:t:1", "uid", "u0"))));

        assertEquals("/a:t:1", header(out, "lineage"));
        assertNull(header(out, "uid"));
    }

    @Test
    public void prefixedLakeResponseHeadersAreDropped() throws Exception {
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        lake.respond(request -> new StubHttpServer.Response(200, "ok").header(interceptor.headerPrefix + "spoofed", "1"));

        Record out = only(send(interceptor, plain(record("k0", "v0"))));

        assertNull(headerIgnoreCase(out, interceptor.headerPrefix + "spoofed"));
    }

    @Test
    public void timingHeadersAreAddedWhenConfigured() throws Exception {
        config.set("headers.res", "(?i)^(time|timespan)$");
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        Record out = only(send(interceptor, plain(record("k0", "v0"))));

        for (String name : List.of("in-time", "req-time", "res-time", "out-time", "req-timespan", "run-timespan")) {
            assertNotNull(name, header(out, interceptor.headerPrefix + name));
        }
    }

    @Test
    public void lakeErrorFailsPartitionWithRetriableError() throws Exception {
        lakeFails();
        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        assertEquals(Map.of(new TopicPartition(topic, 0), Errors.REQUEST_TIMED_OUT), result.partitionErrors());
        assertEquals(0, records(result.data(), topic, 0).sizeInBytes());
    }

    @Test
    public void retriableErrorCodeIsConfigurable() throws Exception {
        config.set("error.retriable", "NOT_ENOUGH_REPLICAS");
        lakeFails();

        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        assertEquals(Errors.NOT_ENOUGH_REPLICAS, result.partitionErrors().get(new TopicPartition(topic, 0)));
    }

    @Test
    public void unreachableLakeIsRetriable() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        config.set("uri", "http://127.0.0.1:" + closedPort + "/lake");

        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        assertEquals(Errors.REQUEST_TIMED_OUT, result.partitionErrors().get(new TopicPartition(topic, 0)));
    }

    @Test
    public void onlyFailingPartitionIsRejected() throws Exception {
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        lake.respond(request -> "1".equals(request.header(interceptor.headerPrefix + "partition-index"))
            ? new StubHttpServer.Response(500, "down") : new StubHttpServer.Response(200, "ok"));

        InterceptResult result = interceptor.intercept(request()
            .add(topic, 0, plain(record("k0", "v0")))
            .add(topic, 1, plain(record("k1", "v1")))
            .build(), VERSION);

        assertEquals(Map.of(new TopicPartition(topic, 1), Errors.REQUEST_TIMED_OUT), result.partitionErrors());
        assertEquals("ok", value(recordList(records(result.data(), topic, 0)).get(0)));
    }

    @Test
    public void onExceptionOriginalKeepsRecord() throws Exception {
        config.set("onException", "original");
        lakeFails();

        InterceptResult result = send(interceptor(), plain(record("k0", "v0", "uid", "u0")));

        assertFalse(failed(result));
        Record out = only(result);
        assertEquals("v0", value(out));
        assertEquals("u0", header(out, "uid"));
    }

    @Test
    public void onExceptionHeadersKeepsRecordWithErrorHeaders() throws Exception {
        config.set("onException", "headers");
        lakeFails();
        HttpProduceRequestDataInterceptor interceptor = interceptor();

        InterceptResult result = send(interceptor, plain(record("k0", "v0")));

        assertFalse(failed(result));
        Record out = only(result);
        assertEquals("v0", value(out));
        assertEquals("true", header(out, interceptor.headerPrefix + "error"));
        assertEquals(HttpResponseException.class.getName(), header(out, interceptor.headerPrefix + "error-class"));
        assertNotNull(header(out, interceptor.headerPrefix + "error-message"));
    }

    @Test
    public void onExceptionIsRequestScopedWhenConfigured() throws Exception {
        config.set("onException.scopes", "(?i)^(app|request)$");
        lakeFails();
        HttpProduceRequestDataInterceptor interceptor = interceptor();

        InterceptResult result = send(interceptor, plain(record("k0", "v0", interceptor.headerPrefix + "onException", "original")));

        assertFalse(failed(result));
        assertEquals("v0", value(only(result)));
    }

    @Test
    public void onExceptionDlqRoutesToDefaultDlqTopic() throws Exception {
        config.set("onException", "dlq");
        lakeFails();
        HttpProduceRequestDataInterceptor interceptor = interceptor();

        InterceptResult result = send(interceptor, plain(record("k0", "v0")));

        assertFalse(failed(result));
        assertEquals("done without a write", Errors.NONE, result.partitionErrors().get(new TopicPartition(topic, 0)));
        List<Record> dlq = recordList(records(result.data(), "__" + topic + "-dlq", 0));
        assertEquals(1, dlq.size());
        assertEquals("v0", value(dlq.get(0)));
        assertEquals(topic, header(dlq.get(0), interceptor.headerPrefix + "original-topic"));
        assertEquals("true", header(dlq.get(0), interceptor.headerPrefix + "error"));
    }

    @Test
    public void onExceptionDlqTopicTemplateIsConfigurable() throws Exception {
        config.set("onException", "dlq").set("onException.dlqTopic", "dead-{topic-name}");
        lakeFails();

        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        assertEquals(1, recordList(records(result.data(), "dead-" + topic, 0)).size());
    }

    @Test
    public void routesToDlqAfterMaxAttemptsForSameTransaction() throws Exception {
        config.set("retry.txnIdHeader", "cl-api-txn-id").set("retry.maxAttempts", "3");
        lakeFails();
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        MemoryRecords records = idempotent(record("k0", "v0", "cl-api-txn-id", "tx-1"));

        assertTrue(failed(send(interceptor, records)));
        assertTrue(failed(send(interceptor, records)));
        InterceptResult third = send(interceptor, records);

        assertFalse("third attempt goes to the DLQ instead of failing", failed(third));
        List<Record> dlq = recordList(records(third.data(), "__" + topic + "-dlq", 0));
        assertEquals(1, dlq.size());
        assertEquals("3", header(dlq.get(0), interceptor.headerPrefix + "retry-attempts"));
        assertEquals(3, lake.requests().size());
    }

    @Test
    public void txnIdHeaderIsMatchedIgnoringCase() throws Exception {
        config.set("retry.txnIdHeader", "cl-api-txn-id").set("retry.maxAttempts", "2");
        lakeFails();
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        MemoryRecords records = plain(record("k0", "v0", "CL-API-TXN-ID", "tx-1"));

        assertTrue(failed(send(interceptor, records)));
        assertFalse(failed(send(interceptor, records)));
    }

    @Test
    public void successResetsAttemptCount() throws Exception {
        config.set("retry.txnIdHeader", "cl-api-txn-id").set("retry.maxAttempts", "2");
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        MemoryRecords records = plain(record("k0", "v0", "cl-api-txn-id", "tx-1"));

        lakeFails();
        assertTrue(failed(send(interceptor, records)));
        lake.respond(request -> new StubHttpServer.Response(200, "ok"));
        assertFalse(failed(send(interceptor, records)));
        lakeFails();
        InterceptResult afterReset = send(interceptor, records);

        assertTrue("count restarted at 1, so this is an error, not a DLQ route", failed(afterReset));
    }

    @Test
    public void recordsWithoutTxnIdAreNeverRoutedToDlq() throws Exception {
        config.set("retry.txnIdHeader", "cl-api-txn-id").set("retry.maxAttempts", "2");
        lakeFails();
        HttpProduceRequestDataInterceptor interceptor = interceptor();
        MemoryRecords records = plain(record("k0", "v0"));

        for (int i = 0; i < 4; i++) {
            assertTrue(failed(send(interceptor, records)));
        }
    }

    @Test
    public void httpOnExceptionOriginalKeepsRecordOnLakeError() throws Exception {
        config.set("httpClient.onException", "original");
        lakeFails();

        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        assertFalse(failed(result));
        assertEquals("v0", value(only(result)));
    }

    @Test
    public void httpOnExceptionPassThruUsesErrorResponse() throws Exception {
        config.set("httpClient.onException", "pass-thru");
        lake.respond(request -> new StubHttpServer.Response(503, "partial"));

        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        assertFalse(failed(result));
        assertEquals("partial", value(only(result)));
    }

    @Test
    public void disabledInterceptorSkipsLakeAndStripsBrokerHeaders() throws Exception {
        config.set("enable", "false");
        HttpProduceRequestDataInterceptor interceptor = interceptor();

        Record out = only(send(interceptor, plain(record("k0", "v0", interceptor.headerPrefix + "x", "1", "uid", "u0"))));

        assertTrue(lake.requests().isEmpty());
        assertEquals("v0", value(out));
        assertEquals("u0", header(out, "uid"));
        assertNull(header(out, interceptor.headerPrefix + "x"));
    }

    @Test
    public void enableCanBeTurnedOnPerRequest() throws Exception {
        config.set("enable", "false").set("enable.scopes", "(?i)^(app|request)$");
        HttpProduceRequestDataInterceptor interceptor = interceptor();

        Record out = only(send(interceptor, plain(record("k0", "v0", interceptor.headerPrefix + "enable", "true"))));

        assertEquals(1, lake.requests().size());
        assertEquals("lake:v0", value(out));
    }

    @Test
    public void enableSendFalseSkipsTheCall() throws Exception {
        config.set("enable-send", "false");

        Record out = only(send(interceptor(), plain(record("k0", "v0"))));

        assertTrue(lake.requests().isEmpty());
        assertEquals("", value(out));
    }

    @Test
    public void everyRecordOfABatchIsSentInOrder() throws Exception {
        InterceptResult result = send(interceptor(), idempotent(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")));

        assertEquals(List.of("v0", "v1", "v2"), lake.requests().stream().map(StubHttpServer.Request::bodyString).collect(java.util.stream.Collectors.toList()));
        List<Record> out = recordList(records(result.data(), topic, 0));
        assertEquals(List.of(0L, 1L, 2L), out.stream().map(Record::offset).collect(java.util.stream.Collectors.toList()));
        assertEquals("lake:v2", value(out.get(2)));
    }

    @Test
    public void lakeResponseRecordsAreStashedForOffsets() throws Exception {
        InterceptResult result = send(interceptor(), plain(record("k0", "v0")));

        InterceptorStash.StashedRequest stash = InterceptorStash.pop(result.data());
        assertNotNull(stash);
        Map<String, String> stashed = stash.partitions.get(new TopicPartition(topic, 0)).records.get(0).headers;
        assertTrue(stashed.entrySet().stream().anyMatch(e -> e.getKey().equalsIgnoreCase("X-Lake-Uid") && "uid-k0".equals(e.getValue())));
    }

    @Test
    public void untouchedTopicsSkipTheLake() throws Exception {
        config.set("topics.namePattern", "^(?!__).*$");
        HttpProduceRequestDataInterceptor interceptor = interceptor();

        ProduceRequestData out = interceptor.intercept(request().add("__internal", 0, plain(record("k0", "v0"))).build(), VERSION).data();

        assertTrue(lake.requests().isEmpty());
        assertEquals("v0", value(recordList(records(out, "__internal", 0)).get(0)));
    }
}
