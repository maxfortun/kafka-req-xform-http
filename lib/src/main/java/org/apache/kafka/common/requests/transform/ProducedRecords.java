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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponseListenerFactory;

/**
 * Stashes the records of each produce request at parse time, and hands them back with their assigned offsets
 * once the broker has appended them. The broker clears a request's records after appending, so they have to be
 * captured while parsing.
 *
 * Keys, headers and timestamps are kept, copied out of the request buffer. Values are kept only when the listener's
 * records.body is set, since they're usually the bulk of the request, which the broker frees after the append.
 */
public final class ProducedRecords {

    /** A record as appended to the log. */
    public static final class ProducedRecord {
        public final TopicPartition topicPartition;
        public final long offset;
        public final long timestamp;
        public final ByteBuffer key;
        public final Header[] headers;
        /** Null unless values are stashed. */
        public final ByteBuffer value;

        ProducedRecord(TopicPartition topicPartition, long offset, long timestamp, ByteBuffer key, Header[] headers, ByteBuffer value) {
            this.topicPartition = topicPartition;
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.headers = headers;
            this.value = value;
        }

        /** Value of the last header with this name, matched case-insensitively, or null. */
        public String header(String name) {
            String value = null;
            for (Header header : headers) {
                if (name.equalsIgnoreCase(header.key())) {
                    value = Utils.utf8(header.value());
                }
            }
            return value;
        }
    }

    /** A record as parsed, with its offset relative to the start of the partition's appended records. */
    private static final class StashedRecord {
        final long relativeOffset;
        final long timestamp;
        final ByteBuffer key;
        final Header[] headers;
        final ByteBuffer value;

        StashedRecord(long relativeOffset, long timestamp, ByteBuffer key, Header[] headers, ByteBuffer value) {
            this.relativeOffset = relativeOffset;
            this.timestamp = timestamp;
            this.key = key;
            this.headers = headers;
            this.value = value;
        }
    }

    // ProduceRequest doesn't override equals/hashCode, so entries are keyed by identity.
    // Weak keys drop requests that never reach the response callback, e.g. rejected before append.
    private static final Map<ProduceRequest, Map<TopicPartition, List<StashedRecord>>> requests = Collections.synchronizedMap(new WeakHashMap<>());

    private static final boolean enabled = isListenerConfigured();

    // The listener is created lazily by the broker, possibly after the first request is parsed, so its setting is resolved here.
    private static final boolean stashValues = new AbstractTransformer(AbstractProduceResponseListener.configName()) {}
        .configured("records.body", "true", false);

    private ProducedRecords() {
    }

    /** True when the broker has a ProduceResponseListener configured, i.e. someone will pop the stash. */
    public static boolean enabled() {
        return enabled;
    }

    private static boolean isListenerConfigured() {
        String listener = System.getProperty(ProduceResponseListenerFactory.PRODUCE_RESPONSE_LISTENER_PROPERTY);
        if (null == listener) {
            listener = System.getenv(ProduceResponseListenerFactory.PRODUCE_RESPONSE_LISTENER_ENV);
        }
        return null != listener && !listener.isBlank() && !ProduceResponseListenerFactory.PRODUCE_RESPONSE_LISTENER_DEFAULT.equals(listener.trim());
    }

    public static void stash(ProduceRequest produceRequest, ProduceRequestData produceRequestData) {
        stash(produceRequest, produceRequestData, stashValues);
    }

    static void stash(ProduceRequest produceRequest, ProduceRequestData produceRequestData, boolean withValues) {
        Map<TopicPartition, List<StashedRecord>> partitions = new HashMap<>();

        for (ProduceRequestData.TopicProduceData topicProduceData : produceRequestData.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {
                List<StashedRecord> records = new ArrayList<>();
                Long firstOffset = null;

                for (RecordBatch recordBatch : ((MemoryRecords) partitionProduceData.records()).batches()) {
                    if (null == firstOffset) {
                        firstOffset = recordBatch.baseOffset();
                    }
                    for (Record record : recordBatch) {
                        records.add(new StashedRecord(record.offset() - firstOffset, record.timestamp(), copy(record.key()), copy(record.headers()),
                            withValues ? copy(record.value()) : null));
                    }
                }

                if (!records.isEmpty()) {
                    partitions.put(new TopicPartition(topicProduceData.name(), partitionProduceData.index()), records);
                }
            }
        }

        if (!partitions.isEmpty()) {
            requests.put(produceRequest, partitions);
        }
    }

    /**
     * Removes the request's stash and returns its records with their assigned offsets.
     * Records of partitions that failed to append are left out.
     */
    public static List<ProducedRecord> pop(ProduceRequest produceRequest, Map<TopicPartition, ProduceResponse.PartitionResponse> responses) {
        Map<TopicPartition, List<StashedRecord>> partitions = requests.remove(produceRequest);
        if (null == partitions) {
            return Collections.emptyList();
        }

        List<ProducedRecord> produced = new ArrayList<>();
        for (Map.Entry<TopicPartition, List<StashedRecord>> entry : partitions.entrySet()) {
            ProduceResponse.PartitionResponse response = responses.get(entry.getKey());
            if (null == response || response.error != Errors.NONE || response.baseOffset < 0) {
                continue;
            }
            for (StashedRecord record : entry.getValue()) {
                produced.add(new ProducedRecord(entry.getKey(), response.baseOffset + record.relativeOffset, record.timestamp, record.key, record.headers, record.value));
            }
        }
        return produced;
    }

    // Copies detach the stash from the request buffer, so the broker can still free it.
    private static ByteBuffer copy(ByteBuffer buffer) {
        if (null == buffer) {
            return null;
        }
        ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
        copy.put(buffer.duplicate()).flip();
        return copy.asReadOnlyBuffer();
    }

    private static Header[] copy(Header[] headers) {
        Header[] copies = new Header[headers.length];
        for (int i = 0; i < headers.length; i++) {
            // value() copies the bytes out of the request buffer.
            copies[i] = new RecordHeader(headers[i].key(), headers[i].value());
        }
        return copies;
    }
}
