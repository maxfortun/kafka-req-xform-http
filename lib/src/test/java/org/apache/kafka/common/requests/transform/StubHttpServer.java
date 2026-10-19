package org.apache.kafka.common.requests.transform;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/** Local HTTP server that records requests and answers with a scripted response. */
final class StubHttpServer implements AutoCloseable {
    static final class Request {
        final String path;
        final Map<String, String> headers;
        final byte[] body;

        Request(String path, Map<String, String> headers, byte[] body) {
            this.path = path;
            this.headers = headers;
            this.body = body;
        }

        // Header names are case-insensitive over HTTP, so lookups are too.
        String header(String name) {
            return headers.get(name.toLowerCase());
        }

        String bodyString() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    static final class Response {
        final int status;
        final Map<String, String> headers = new HashMap<>();
        final byte[] body;

        Response(int status, String body) {
            this.status = status;
            this.body = body.getBytes(StandardCharsets.UTF_8);
        }

        Response header(String name, String value) {
            headers.put(name, value);
            return this;
        }
    }

    private final HttpServer server;
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private volatile Function<Request, Response> responder = request -> new Response(200, request.bodyString());

    StubHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    StubHttpServer respond(Function<Request, Response> responder) {
        this.responder = responder;
        return this;
    }

    String url(String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    List<Request> requests() {
        return new ArrayList<>(requests);
    }

    List<Request> awaitRequests(int count, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (requests.size() < count) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Expected " + count + " requests within " + timeout + ", got " + requests.size());
            }
            Thread.sleep(20);
        }
        return requests();
    }

    private void handle(HttpExchange exchange) throws IOException {
        Map<String, String> headers = new HashMap<>();
        exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(), String.join(",", values)));
        byte[] body;
        try (InputStream in = exchange.getRequestBody()) {
            body = in.readAllBytes();
        }
        Request request = new Request(exchange.getRequestURI().getPath(), headers, body);
        requests.add(request);

        Response response = responder.apply(request);
        response.headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        exchange.sendResponseHeaders(response.status, response.body.length == 0 ? -1 : response.body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(response.body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
