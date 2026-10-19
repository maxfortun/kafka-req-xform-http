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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP interceptor for produce responses. Sends offset information to an external service
 * after records have been written to the log.
 */
public class HttpProduceResponseDataInterceptor extends AbstractTransformer implements ProduceResponseDataInterceptor {
    private static final Logger log = LoggerFactory.getLogger(HttpProduceResponseDataInterceptor.class);

    private static final String brokerHostname = System.getenv("HOSTNAME");

    private final String offsetsUri;
    private final long batchIntervalMs;
    private final int batchSize;
    private final boolean enabled;

    // Background sender for batching offset notifications
    private final BlockingQueue<OffsetRecord> pendingRecords = new LinkedBlockingQueue<>();
    private final Thread senderThread;
    private volatile boolean running = true;

    public HttpProduceResponseDataInterceptor(String transformerName) {
        super(transformerName);

        this.offsetsUri = appConfig("offsets.uri");
        this.batchIntervalMs = Long.parseLong(appConfig("offsets.batchIntervalMs", "1000"));
        this.batchSize = Integer.parseInt(appConfig("offsets.batchSize", "100"));
        this.enabled = offsetsUri != null && !offsetsUri.isBlank();

        if (enabled) {
            this.senderThread = new Thread(this::senderLoop, transformerName + "-offset-sender");
            this.senderThread.setDaemon(true);
            this.senderThread.start();
            log.info("{}: offset notification enabled, uri={}, batchIntervalMs={}, batchSize={}",
                transformerName, offsetsUri, batchIntervalMs, batchSize);
        } else {
            this.senderThread = null;
            log.info("{}: offset notification disabled (no offsets.uri configured)", transformerName);
        }
    }

    @Override
    public void onAppend(ProduceRequestData requestData,
                         Map<TopicPartition, PartitionResponse> responses,
                         short version) {
        if (!enabled) {
            return;
        }

        // Get stashed data from the pre-write interceptor
        InterceptorStash.StashedRequest stashedRequest = InterceptorStash.pop(requestData);

        for (Map.Entry<TopicPartition, PartitionResponse> entry : responses.entrySet()) {
            TopicPartition tp = entry.getKey();
            PartitionResponse response = entry.getValue();

            // Skip partitions with errors
            if (response.error != Errors.NONE) {
                log.debug("{}: skipping partition {} with error {}", transformerName, tp, response.error);
                continue;
            }

            // Get stashed records for this partition
            List<InterceptorStash.StashedRecord> stashedRecords = null;
            if (stashedRequest != null) {
                InterceptorStash.StashedPartition stashedPartition = stashedRequest.partitions.get(tp);
                if (stashedPartition != null) {
                    stashedRecords = stashedPartition.records;
                }
            }

            // Calculate offsets for each record
            long baseOffset = response.baseOffset;
            int recordCount = stashedRecords != null ? stashedRecords.size() : 0;

            if (recordCount > 0) {
                for (int i = 0; i < recordCount; i++) {
                    InterceptorStash.StashedRecord stashed = stashedRecords.get(i);
                    long offset = baseOffset + i;

                    OffsetRecord offsetRecord = new OffsetRecord(
                        tp.topic(),
                        tp.partition(),
                        offset,
                        stashed.key,
                        stashed.timestamp,
                        stashed.headers
                    );

                    pendingRecords.offer(offsetRecord);
                }
                log.debug("{}: queued {} offset records for partition {}", transformerName, recordCount, tp);
            } else {
                log.debug("{}: no stashed records for partition {}, baseOffset={}", transformerName, tp, baseOffset);
            }
        }
    }

    private void senderLoop() {
        List<OffsetRecord> batch = new ArrayList<>(batchSize);

        while (running) {
            try {
                // Wait for first record or timeout
                OffsetRecord first = pendingRecords.poll(batchIntervalMs, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }

                batch.clear();
                batch.add(first);

                // Drain up to batchSize more records
                pendingRecords.drainTo(batch, batchSize - 1);

                // Send the batch
                sendBatch(batch);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("{}: error in sender loop", transformerName, e);
            }
        }

        // Drain remaining records on shutdown
        batch.clear();
        pendingRecords.drainTo(batch);
        if (!batch.isEmpty()) {
            try {
                sendBatch(batch);
            } catch (Exception e) {
                log.error("{}: error sending final batch", transformerName, e);
            }
        }
    }

    private void sendBatch(List<OffsetRecord> batch) {
        if (batch.isEmpty()) {
            return;
        }

        try {
            // Build JSON payload
            StringBuilder json = new StringBuilder();
            json.append("{\"offsets\":[");
            boolean first = true;
            for (OffsetRecord record : batch) {
                if (!first) {
                    json.append(",");
                }
                first = false;
                json.append("{");
                json.append("\"topic\":\"").append(escapeJson(record.topic)).append("\",");
                json.append("\"partition\":").append(record.partition).append(",");
                json.append("\"offset\":").append(record.offset).append(",");
                json.append("\"timestamp\":").append(record.timestamp);
                if (record.key != null) {
                    json.append(",\"key\":\"").append(escapeJson(record.key)).append("\"");
                }
                if (record.headers != null && !record.headers.isEmpty()) {
                    json.append(",\"headers\":{");
                    boolean firstHeader = true;
                    for (Map.Entry<String, String> h : record.headers.entrySet()) {
                        if (!firstHeader) {
                            json.append(",");
                        }
                        firstHeader = false;
                        json.append("\"").append(escapeJson(h.getKey())).append("\":\"")
                            .append(escapeJson(h.getValue())).append("\"");
                    }
                    json.append("}");
                }
                json.append("}");
            }
            json.append("]}");

            // Send HTTP request
            AbstractHttpClient httpClient = HttpClients.getHttpClient(null, this);
            AbstractHttpRequest httpRequest = httpClient.newHttpRequest(offsetsUri);
            httpRequest.header("Content-Type", "application/json");
            httpRequest.header(headerPrefix + "hostname", brokerHostname);
            httpRequest.header(headerPrefix + "batch-size", String.valueOf(batch.size()));
            httpRequest.body(null, java.nio.ByteBuffer.wrap(json.toString().getBytes(StandardCharsets.UTF_8)));

            HttpResponse httpResponse = httpClient.send(httpRequest);
            if (httpResponse.statusCode() != 200) {
                log.warn("{}: offset notification failed: {} {}", transformerName, httpResponse.statusCode(),
                    new String(httpResponse.body(), StandardCharsets.UTF_8));
            } else {
                log.debug("{}: sent {} offset records", transformerName, batch.size());
            }

        } catch (Exception e) {
            log.error("{}: failed to send offset batch", transformerName, e);
        }
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static class OffsetRecord {
        final String topic;
        final int partition;
        final long offset;
        final String key;
        final long timestamp;
        final Map<String, String> headers;

        OffsetRecord(String topic, int partition, long offset, String key, long timestamp, Map<String, String> headers) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.timestamp = timestamp;
            this.headers = headers;
        }
    }

    public void shutdown() {
        running = false;
        if (senderThread != null) {
            senderThread.interrupt();
            try {
                senderThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
