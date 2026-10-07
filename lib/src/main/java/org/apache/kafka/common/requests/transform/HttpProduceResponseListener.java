/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests.transform;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.json.JSONArray;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POSTs appended records, with their assigned offsets, to a service.
 * Which record headers are posted is set by records.headers, and whether the record value is posted by records.body.
 * Records are queued and sent in batches from a background thread, so the broker thread never waits.
 */
public class HttpProduceResponseListener extends AbstractProduceResponseListener {
    private static final Logger log = LoggerFactory.getLogger(HttpProduceResponseListener.class);

    private static final String brokerHostname = System.getenv("HOSTNAME");

    private final List<String> recordHeaders;
    private final boolean requireHeaders;
    // records.mode=last-per-partition: post only each partition's latest record per flush.
    private final boolean lastPerPartition;
    private final boolean postBody;
    private final boolean base64Body;
    private final String uri;
    private final int batchMaxRecords;
    private final int queueMaxRecords;

    private final ConcurrentLinkedQueue<ProducedRecords.ProducedRecord> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicBoolean flushPending = new AtomicBoolean();
    private final ConcurrentHashMap<TopicPartition, ProducedRecords.ProducedRecord> latest = new ConcurrentHashMap<>();
    private final ScheduledExecutorService sender;

    public HttpProduceResponseListener() {
        this(configName());
    }

    public HttpProduceResponseListener(String name) {
        super(name);

        recordHeaders = parseList(appConfig("records.headers"));
        requireHeaders = configured("records.requireHeaders", "true", false);
        String mode = appConfig("records.mode", "all");
        lastPerPartition = "last-per-partition".equalsIgnoreCase(mode);
        if (!lastPerPartition && !"all".equalsIgnoreCase(mode)) {
            log.warn("{}: unknown records.mode {}, using all", transformerName, mode);
        }
        postBody = configured("records.body", "true", false);
        base64Body = "base64".equalsIgnoreCase(appConfig("records.bodyEncoding", "utf8"));
        uri = appConfig("uri");
        batchMaxRecords = Integer.parseInt(appConfig("batch.maxRecords", "5000"));
        queueMaxRecords = Integer.parseInt(appConfig("queue.maxRecords", "100000"));
        long batchIntervalMs = Long.parseLong(appConfig("batch.intervalMs", "1000"));

        if (null == uri || uri.isBlank() || !configured("enable", "true", true)) {
            log.warn("{}: disabled, uri={}", transformerName, uri);
            sender = null;
            return;
        }

        sender = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, transformerName + "-sender");
            thread.setDaemon(true);
            return thread;
        });
        sender.scheduleWithFixedDelay(this::flush, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
        log.info("{}: sending to {} every {}ms, up to {} records per call, mode={}, headers={}, requireHeaders={}, body={}",
            transformerName, uri, batchIntervalMs, batchMaxRecords, lastPerPartition ? "last-per-partition" : "all", recordHeaders, requireHeaders, postBody);
    }

    static List<String> parseList(String value) {
        if (null == value) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(","))
            .map(String::trim)
            .filter(item -> !item.isEmpty())
            .collect(Collectors.toList());
    }

    @Override
    protected void onAppended(List<ProducedRecords.ProducedRecord> records) {
        if (null == sender) {
            return;
        }

        int dropped = 0;
        for (ProducedRecords.ProducedRecord record : records) {
            if (requireHeaders && !hasAnyHeader(record, recordHeaders)) {
                continue;
            }
            if (lastPerPartition) {
                keepLatest(latest, record);
                continue;
            }
            if (queueSize.get() >= queueMaxRecords) {
                dropped++;
                continue;
            }
            queue.add(record);
            queueSize.incrementAndGet();
        }

        if (dropped > 0) {
            log.warn("{}: queue full ({} records), dropped {} records", transformerName, queueMaxRecords, dropped);
        }

        if (queueSize.get() >= batchMaxRecords && flushPending.compareAndSet(false, true)) {
            sender.execute(this::flush);
        }
    }

    static boolean hasAnyHeader(ProducedRecords.ProducedRecord record, List<String> names) {
        for (String name : names) {
            if (null != record.header(name)) {
                return true;
            }
        }
        return false;
    }

    private void flush() {
        flushPending.set(false);
        try {
            List<ProducedRecords.ProducedRecord> batch;
            do {
                batch = drain();
                if (!batch.isEmpty()) {
                    send(batch);
                }
            } while (batch.size() >= batchMaxRecords);
        } catch (Throwable t) {
            // Never let an exception cancel the scheduled flush.
            log.warn("{}: flush failed", transformerName, t);
        }
    }

    /** Keeps the record with the highest offset per partition. Requests for a partition can complete out of order. */
    static void keepLatest(Map<TopicPartition, ProducedRecords.ProducedRecord> latest, ProducedRecords.ProducedRecord record) {
        latest.merge(record.topicPartition, record, (current, candidate) -> candidate.offset > current.offset ? candidate : current);
    }

    private List<ProducedRecords.ProducedRecord> drain() {
        List<ProducedRecords.ProducedRecord> batch = new ArrayList<>();

        if (lastPerPartition) {
            for (TopicPartition topicPartition : latest.keySet()) {
                if (batch.size() >= batchMaxRecords) {
                    break;
                }
                ProducedRecords.ProducedRecord record = latest.remove(topicPartition);
                if (null != record) {
                    batch.add(record);
                }
            }
            return batch;
        }

        ProducedRecords.ProducedRecord record;
        while (batch.size() < batchMaxRecords && null != (record = queue.poll())) {
            queueSize.decrementAndGet();
            batch.add(record);
        }
        return batch;
    }

    private void send(List<ProducedRecords.ProducedRecord> batch) throws Exception {
        RecordHeaders noRecordHeaders = new RecordHeaders();
        AbstractHttpClient httpClient = HttpClients.getHttpClient(noRecordHeaders, this);
        AbstractHttpRequest httpRequest = httpClient.newHttpRequest(uri);

        httpRequest.header("Content-Type", "application/json");
        httpRequest.header(headerPrefix + "hostname", brokerHostname);
        httpRequest.header(headerPrefix + "api-type", "ProduceResponse");
        httpRequest.header(headerPrefix + "record-count", String.valueOf(batch.size()));

        String httpHeadersString = appConfig("headers.http");
        if (null != httpHeadersString) {
            for (String httpHeaderString : httpHeadersString.split("[,\\s]+")) {
                try {
                    String[] tokens = httpHeaderString.split("\\s*=\\s*");
                    httpRequest.header(tokens[0], tokens[1]);
                } catch (Exception e) {
                    log.warn("{}: failed to parse header: {}", transformerName, httpHeaderString, e);
                }
            }
        }

        httpRequest.body(null, ByteBuffer.wrap(toJson(brokerHostname, batch, recordHeaders, postBody, base64Body).getBytes(StandardCharsets.UTF_8)));

        long start = System.currentTimeMillis();
        HttpResponse httpResponse = httpClient.send(httpRequest);
        if (httpResponse.statusCode() != 200) {
            log.warn("{}: {} records not accepted\n{}", transformerName, batch.size(), HttpResponseException.describe(httpResponse));
            return;
        }
        log.debug("{}: sent {} records in {}ms", transformerName, batch.size(), System.currentTimeMillis() - start);
    }

    /**
     * {"hostname": "...", "partitions": [{"topic": "t", "partition": 0, "records": [
     *   {"offset": 123, "timestamp": 1700000000000, "headers": {"name": "value"}, "body": "..."}]}]}
     * Only the configured headers that a record has are included. body is included only when postBody is set.
     */
    static String toJson(String hostname, List<ProducedRecords.ProducedRecord> batch, List<String> recordHeaders, boolean postBody, boolean base64Body) {
        Map<TopicPartition, JSONArray> recordsByPartition = new LinkedHashMap<>();
        for (ProducedRecords.ProducedRecord record : batch) {
            JSONObject json = new JSONObject()
                .put("offset", record.offset)
                .put("timestamp", record.timestamp);

            JSONObject headers = new JSONObject();
            for (String name : recordHeaders) {
                String value = record.header(name);
                if (null != value) {
                    headers.put(name, value);
                }
            }
            json.put("headers", headers);

            if (postBody && null != record.value) {
                byte[] bytes = new byte[record.value.remaining()];
                record.value.duplicate().get(bytes);
                json.put("body", base64Body ? Base64.getEncoder().encodeToString(bytes) : new String(bytes, StandardCharsets.UTF_8));
            }

            recordsByPartition.computeIfAbsent(record.topicPartition, k -> new JSONArray()).put(json);
        }

        JSONArray partitions = new JSONArray();
        for (Map.Entry<TopicPartition, JSONArray> entry : recordsByPartition.entrySet()) {
            partitions.put(new JSONObject()
                .put("topic", entry.getKey().topic())
                .put("partition", entry.getKey().partition())
                .put("records", entry.getValue()));
        }

        JSONObject json = new JSONObject();
        if (null != hostname) {
            json.put("hostname", hostname);
        }
        return json.put("partitions", partitions).toString();
    }
}
