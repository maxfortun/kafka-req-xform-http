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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Stands in for the lake's offsets endpoint, recording every entry HttpProduceResponseDataInterceptor posts.
 */
public class OffsetsLake {
    private final HttpServer server;
    private final Queue<JSONObject> offsets = new ConcurrentLinkedQueue<>();

    public OffsetsLake() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/offsets", this::handle);
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
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

    private void handle(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            JSONArray posted = new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8)).getJSONArray("offsets");
            for (int i = 0; i < posted.length(); i++) {
                offsets.add(posted.getJSONObject(i));
            }
        }
        byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(response);
        }
    }
}
