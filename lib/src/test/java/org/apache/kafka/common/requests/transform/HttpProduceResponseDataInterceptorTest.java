package org.apache.kafka.common.requests.transform;

import static org.apache.kafka.common.requests.transform.ProduceRequests.plain;
import static org.apache.kafka.common.requests.transform.ProduceRequests.record;
import static org.apache.kafka.common.requests.transform.ProduceRequests.request;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class HttpProduceResponseDataInterceptorTest {
    private static final short VERSION = 9;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Rule
    public TransformerConfig config = new TransformerConfig("offsets-interceptor");

    private StubHttpServer lake;
    private final List<HttpProduceResponseDataInterceptor> interceptors = new ArrayList<>();
    private String topic;

    @Before
    public void startLake() throws Exception {
        lake = new StubHttpServer().respond(request -> new StubHttpServer.Response(200, "{}"));
        topic = config.name() + "-t";
    }

    @After
    public void stop() {
        interceptors.forEach(HttpProduceResponseDataInterceptor::shutdown);
        lake.close();
    }

    private HttpProduceResponseDataInterceptor interceptor() {
        config.setIfAbsent("offsets.uri", lake.url("/offsets"))
            .setIfAbsent("offsets.batchIntervalMs", "50")
            .set("httpClient.class", AHC5HttpClient.class.getName())
            .set("httpClient.connectTimeout", "5")
            .set("httpClient.responseTimeout", "5");
        HttpProduceResponseDataInterceptor interceptor = new HttpProduceResponseDataInterceptor(config.name());
        interceptors.add(interceptor);
        return interceptor;
    }

    // Distinct per call, since the stash is keyed by request equality.
    private ProduceRequestData requestData(String tag) {
        return request().add(topic + "-" + tag, 0, plain(record(tag, "v"))).build();
    }

    private static InterceptorStash.StashedRecord stashed(String key, String... headerKeyValues) {
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < headerKeyValues.length; i += 2) {
            headers.put(headerKeyValues[i], headerKeyValues[i + 1]);
        }
        return new InterceptorStash.StashedRecord(key, 1000L, headers, null);
    }

    private static void stash(ProduceRequestData data, TopicPartition partition, InterceptorStash.StashedRecord... records) {
        stash(data, Map.of(partition, List.of(records)));
    }

    private static void stash(ProduceRequestData data, Map<TopicPartition, List<InterceptorStash.StashedRecord>> byPartition) {
        Map<TopicPartition, InterceptorStash.StashedPartition> partitions = new HashMap<>();
        byPartition.forEach((tp, records) -> partitions.put(tp, new InterceptorStash.StashedPartition(tp, records)));
        InterceptorStash.stash(data, new InterceptorStash.StashedRequest(partitions));
    }

    private static PartitionResponse ok(long baseOffset) {
        return new PartitionResponse(Errors.NONE, baseOffset, -1L, 0L);
    }

    private static List<JSONObject> offsets(List<StubHttpServer.Request> requests) {
        List<JSONObject> all = new ArrayList<>();
        for (StubHttpServer.Request request : requests) {
            JSONArray array = new JSONObject(request.bodyString()).getJSONArray("offsets");
            for (int i = 0; i < array.length(); i++) {
                all.add(array.getJSONObject(i));
            }
        }
        return all;
    }

    @Test
    public void postsEachRecordAtBaseOffsetPlusIndex() throws Exception {
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition tp = new TopicPartition(topic, 3);
        ProduceRequestData data = requestData("a");
        stash(data, tp, stashed("k0", "uid", "u0"), stashed("k1", "uid", "u1"));

        interceptor.onAppend(data, Map.of(tp, ok(42L)), VERSION);

        List<StubHttpServer.Request> requests = lake.awaitRequests(1, TIMEOUT);
        List<JSONObject> offsets = offsets(requests);
        assertEquals(2, offsets.size());
        for (int i = 0; i < 2; i++) {
            JSONObject offset = offsets.get(i);
            assertEquals(topic, offset.getString("topic"));
            assertEquals(3, offset.getInt("partition"));
            assertEquals(42L + i, offset.getLong("offset"));
            assertEquals("k" + i, offset.getString("key"));
            assertEquals("u" + i, offset.getJSONObject("headers").getString("uid"));
            assertEquals(1000L, offset.getLong("timestamp"));
        }
        assertEquals("/offsets", requests.get(0).path);
        assertTrue(requests.get(0).header("Content-Type").startsWith("application/json"));
        assertEquals("2", requests.get(0).header(interceptor.headerPrefix + "batch-size"));
    }

    @Test
    public void popsTheStash() throws Exception {
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition tp = new TopicPartition(topic, 0);
        ProduceRequestData data = requestData("a");
        stash(data, tp, stashed("k0"));

        interceptor.onAppend(data, Map.of(tp, ok(0L)), VERSION);

        assertFalse(InterceptorStash.hasStash(data));
    }

    @Test
    public void skipsPartitionsThatFailed() throws Exception {
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition failed = new TopicPartition(topic, 0);
        TopicPartition written = new TopicPartition(topic, 1);
        ProduceRequestData data = requestData("a");
        stash(data, Map.of(failed, List.of(stashed("lost")), written, List.of(stashed("kept"))));

        interceptor.onAppend(data, Map.of(failed, new PartitionResponse(Errors.REQUEST_TIMED_OUT), written, ok(7L)), VERSION);

        List<JSONObject> offsets = offsets(lake.awaitRequests(1, TIMEOUT));
        assertEquals(1, offsets.size());
        assertEquals("kept", offsets.get(0).getString("key"));
        assertEquals(7L, offsets.get(0).getLong("offset"));
    }

    @Test
    public void requestsWithoutStashPostNothing() throws Exception {
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition tp = new TopicPartition(topic, 0);
        ProduceRequestData unstashed = requestData("a");
        ProduceRequestData stashed = requestData("b");
        stash(stashed, tp, stashed("only"));

        interceptor.onAppend(unstashed, Map.of(tp, ok(0L)), VERSION);
        interceptor.onAppend(stashed, Map.of(tp, ok(5L)), VERSION);

        List<JSONObject> offsets = offsets(lake.awaitRequests(1, TIMEOUT));
        Thread.sleep(200);
        assertEquals(1, offsets(lake.requests()).size());
        assertEquals("only", offsets.get(0).getString("key"));
    }

    @Test
    public void jsonIsEscaped() throws Exception {
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition tp = new TopicPartition(topic, 0);
        ProduceRequestData data = requestData("a");
        String key = "quote\" back\\slash\nnewline\ttab";
        stash(data, tp, stashed(key, "h\"1", "v\\1\r\n"));

        interceptor.onAppend(data, Map.of(tp, ok(0L)), VERSION);

        JSONObject offset = offsets(lake.awaitRequests(1, TIMEOUT)).get(0);
        assertEquals(key, offset.getString("key"));
        assertEquals("v\\1\r\n", offset.getJSONObject("headers").getString("h\"1"));
    }

    @Test
    public void nullKeysAreOmitted() throws Exception {
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition tp = new TopicPartition(topic, 0);
        ProduceRequestData data = requestData("a");
        stash(data, tp, stashed(null));

        interceptor.onAppend(data, Map.of(tp, ok(0L)), VERSION);

        assertFalse(offsets(lake.awaitRequests(1, TIMEOUT)).get(0).has("key"));
    }

    @Test
    public void batchesAreCappedAtBatchSize() throws Exception {
        config.set("offsets.batchSize", "2");
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition tp = new TopicPartition(topic, 0);
        ProduceRequestData data = requestData("a");
        stash(data, tp, stashed("k0"), stashed("k1"), stashed("k2"), stashed("k3"), stashed("k4"));

        interceptor.onAppend(data, Map.of(tp, ok(0L)), VERSION);

        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        while (offsets(lake.requests()).size() < 5 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        List<StubHttpServer.Request> requests = lake.requests();
        assertEquals(5, offsets(requests).size());
        for (StubHttpServer.Request request : requests) {
            assertTrue(offsets(List.of(request)).size() <= 2);
        }
    }

    @Test
    public void lakeErrorsDoNotStopLaterPosts() throws Exception {
        lake.respond(request -> new StubHttpServer.Response(500, "down"));
        HttpProduceResponseDataInterceptor interceptor = interceptor();
        TopicPartition tp = new TopicPartition(topic, 0);
        ProduceRequestData first = requestData("a");
        ProduceRequestData second = requestData("b");
        stash(first, tp, stashed("k0"));
        stash(second, tp, stashed("k1"));

        interceptor.onAppend(first, Map.of(tp, ok(0L)), VERSION);
        lake.awaitRequests(1, TIMEOUT);
        lake.respond(request -> new StubHttpServer.Response(200, "{}"));
        interceptor.onAppend(second, Map.of(tp, ok(1L)), VERSION);

        List<StubHttpServer.Request> requests = lake.awaitRequests(2, TIMEOUT);
        assertEquals("k1", offsets(requests.subList(1, 2)).get(0).getString("key"));
    }

    @Test
    public void disabledWithoutUri() {
        HttpProduceResponseDataInterceptor interceptor = new HttpProduceResponseDataInterceptor(config.name());
        interceptors.add(interceptor);
        TopicPartition tp = new TopicPartition(topic, 0);

        interceptor.onAppend(requestData("a"), Map.of(tp, ok(0L)), VERSION);

        assertTrue(lake.requests().isEmpty());
    }
}
