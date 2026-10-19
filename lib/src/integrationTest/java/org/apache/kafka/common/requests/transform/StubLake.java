package org.apache.kafka.common.requests.transform;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Stands in for the lake, in the test JVM.
 * /lake answers content-lake calls; record headers reach it as HTTP headers, and steer it:
 *   it-lake-id    identifies the record (defaults to the record key)
 *   it-lake-fail  "always" answers 500; a number N answers 500 to the first N calls for that id
 * On success it returns "lake:" + body, with header it-rec-uid: uid-{id}.
 * /offsets records every entry HttpProduceResponseDataInterceptor posts.
 */
public class StubLake {
    public static final String ID = "it-lake-id";
    public static final String FAIL = "it-lake-fail";
    public static final String REC_UID = "it-rec-uid";

    private final HttpServer server;
    private final Queue<JSONObject> offsets = new ConcurrentLinkedQueue<>();
    private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> lastHeaders = new ConcurrentHashMap<>();

    public StubLake() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/offsets", this::handleOffsets);
        server.createContext("/lake", this::handleLake);
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public int calls(String id) {
        AtomicInteger count = calls.get(id);
        return null == count ? 0 : count.get();
    }

    // Headers of the latest /lake call for id, with lower-cased names.
    public Map<String, String> lastHeaders(String id) {
        return lastHeaders.get(id);
    }

    public List<JSONObject> received(String topic) {
        return offsets.stream()
            .filter(offset -> topic.equals(offset.getString("topic")))
            .collect(Collectors.toList());
    }

    // Waits until an entry has been posted for each record key, returning the entries by key.
    public Map<String, JSONObject> awaitKeys(String topic, Collection<String> keys, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (true) {
            Map<String, JSONObject> byKey = new HashMap<>();
            for (JSONObject offset : received(topic)) {
                byKey.put(offset.optString("key"), offset);
            }
            if (byKey.keySet().containsAll(keys)) {
                byKey.keySet().retainAll(keys);
                return byKey;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Expected offsets for keys " + keys + " on " + topic + " within " + timeout + ", got " + received(topic));
            }
            Thread.sleep(100);
        }
    }

    private void handleLake(HttpExchange exchange) throws IOException {
        Map<String, String> headers = new HashMap<>();
        exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(), String.join(",", values)));
        String body = new String(readBody(exchange), StandardCharsets.UTF_8);
        String id = headers.getOrDefault(ID, headers.get("kafka.key"));
        int call = calls.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
        lastHeaders.put(id, headers);

        String fail = headers.get(FAIL);
        boolean failing = "always".equals(fail) || (null != fail && !"always".equals(fail) && call <= Integer.parseInt(fail));
        if (failing) {
            respond(exchange, 500, "lake failure " + call + " for " + id);
            return;
        }
        exchange.getResponseHeaders().add(REC_UID, "uid-" + id);
        respond(exchange, 200, "lake:" + body);
    }

    private void handleOffsets(HttpExchange exchange) throws IOException {
        JSONArray posted = new JSONObject(new String(readBody(exchange), StandardCharsets.UTF_8)).getJSONArray("offsets");
        for (int i = 0; i < posted.length(); i++) {
            offsets.add(posted.getJSONObject(i));
        }
        respond(exchange, 200, "{}");
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            return in.readAllBytes();
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
