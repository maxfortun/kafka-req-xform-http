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

import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stash for passing data from the pre-write interceptor to the post-write interceptor.
 * Uses the ProduceRequestData identity as the key, so the post-write interceptor can
 * retrieve data stashed during the pre-write phase.
 */
public class InterceptorStash {
    private static final Logger log = LoggerFactory.getLogger(InterceptorStash.class);

    /**
     * Record data stashed during pre-write, keyed by partition-relative offset.
     */
    public static class StashedRecord {
        public final String key;
        public final long timestamp;
        public final Map<String, String> headers;
        public final byte[] value;

        public StashedRecord(String key, long timestamp, Map<String, String> headers, byte[] value) {
            this.key = key;
            this.timestamp = timestamp;
            this.headers = headers;
            this.value = value;
        }
    }

    /**
     * Partition data with list of stashed records.
     */
    public static class StashedPartition {
        public final TopicPartition topicPartition;
        public final List<StashedRecord> records;

        public StashedPartition(TopicPartition topicPartition, List<StashedRecord> records) {
            this.topicPartition = topicPartition;
            this.records = records;
        }
    }

    /**
     * All stashed data for a request.
     */
    public static class StashedRequest {
        public final Map<TopicPartition, StashedPartition> partitions;

        public StashedRequest(Map<TopicPartition, StashedPartition> partitions) {
            this.partitions = partitions;
        }
    }

    // Use WeakHashMap so entries are garbage collected when the ProduceRequestData is no longer referenced
    private static final Map<ProduceRequestData, StashedRequest> stash =
        java.util.Collections.synchronizedMap(new WeakHashMap<>());

    /**
     * Stash data for a request during the pre-write phase.
     */
    public static void stash(ProduceRequestData requestData, StashedRequest data) {
        stash.put(requestData, data);
        log.debug("Stashed data for request with {} partitions", data.partitions.size());
    }

    /**
     * Retrieve and remove stashed data for a request during the post-write phase.
     */
    public static StashedRequest pop(ProduceRequestData requestData) {
        StashedRequest data = stash.remove(requestData);
        if (data != null) {
            log.debug("Popped stashed data with {} partitions", data.partitions.size());
        }
        return data;
    }

    /**
     * Check if there is stashed data for a request.
     */
    public static boolean hasStash(ProduceRequestData requestData) {
        return stash.containsKey(requestData);
    }
}
