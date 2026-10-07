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
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponseListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for listeners that act on records after the broker has appended them.
 * Gets the records stashed at parse time, with their assigned offsets, filtered by topics.namePattern.
 */
public abstract class AbstractProduceResponseListener extends AbstractTransformer implements ProduceResponseListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractProduceResponseListener.class);

    public static final String NAME_ENV = "KAFKA_PRODUCE_RESPONSE_LISTENER_NAME";
    public static final String NAME_DEFAULT = "produce-response-listener";

    public AbstractProduceResponseListener(String name) {
        super(name);
    }

    /** The name a listener's settings are resolved under. */
    public static String configName() {
        String name = System.getenv(NAME_ENV);
        return null != name ? name : NAME_DEFAULT;
    }

    @Override
    public final void onProduceResponse(ProduceRequest request, Map<TopicPartition, ProduceResponse.PartitionResponse> responses) {
        List<ProducedRecords.ProducedRecord> records = ProducedRecords.pop(request, responses);

        if (null != topicNamePattern) {
            records = records.stream()
                .filter(record -> record.topicPartition.topic().matches(topicNamePattern))
                .collect(Collectors.toList());
        }

        if (records.isEmpty()) {
            return;
        }

        try {
            onAppended(records);
        } catch (Exception e) {
            log.warn("{}: failed to handle {} appended records", transformerName, records.size(), e);
        }
    }

    /**
     * Called with records that were appended and met the request's acks.
     * Runs on a broker request handler or purgatory thread: must return quickly and must not block on I/O.
     */
    protected abstract void onAppended(List<ProducedRecords.ProducedRecord> records);
}
