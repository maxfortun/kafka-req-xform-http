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

import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOffsetFetchResponseDataTransformer extends AbstractTransformer implements OffsetFetchResponseDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(AbstractOffsetFetchResponseDataTransformer.class);

    protected final String groupIdPattern;

    public AbstractOffsetFetchResponseDataTransformer(String transformerName) {
        super(transformerName);
        groupIdPattern = appConfig("groups.idPattern");
    }

    public OffsetFetchResponseData transform(OffsetFetchResponseData offsetFetchResponseDataIn, short version) {
        OffsetFetchResponseData offsetFetchResponseDataOut = offsetFetchResponseDataIn.duplicate();

        for (RawTaggedField rawTaggedField : offsetFetchResponseDataOut.unknownTaggedFields()) {
            log.debug("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), Utils.utf8(rawTaggedField.data()));
        }

        // Handle v8+ format with groups
        if (!offsetFetchResponseDataOut.groups().isEmpty()) {
            for (OffsetFetchResponseData.OffsetFetchResponseGroup group : offsetFetchResponseDataOut.groups()) {
                if (null != groupIdPattern && !group.groupId().matches(groupIdPattern)) {
                    log.debug("{}: groupIdPattern {} != {}", transformerName, group.groupId(), groupIdPattern);
                    continue;
                }

                for (OffsetFetchResponseData.OffsetFetchResponseTopics topic : group.topics()) {
                    if (null != topicNamePattern && !topic.name().matches(topicNamePattern)) {
                        log.debug("{}: topicNamePattern {} != {}", transformerName, topic.name(), topicNamePattern);
                        continue;
                    }

                    for (OffsetFetchResponseData.OffsetFetchResponsePartitions partition : topic.partitions()) {
                        if (log.isTraceEnabled()) {
                            log.trace("{}: group={} topic={} partition={} offset={} metadata={} before transform",
                                transformerName, group.groupId(), topic.name(),
                                partition.partitionIndex(), partition.committedOffset(), partition.metadata());
                        }

                        transform(group, topic, partition, version);

                        if (log.isTraceEnabled()) {
                            log.trace("{}: group={} topic={} partition={} offset={} metadata={} after transform",
                                transformerName, group.groupId(), topic.name(),
                                partition.partitionIndex(), partition.committedOffset(), partition.metadata());
                        }
                    }
                }
            }
        }

        // Handle legacy format (v0-v7) with topics directly on the response
        for (OffsetFetchResponseData.OffsetFetchResponseTopic offsetFetchResponseTopic : offsetFetchResponseDataOut.topics()) {
            if (null != topicNamePattern && !offsetFetchResponseTopic.name().matches(topicNamePattern)) {
                log.debug("{}: topicNamePattern {} != {}", transformerName, offsetFetchResponseTopic.name(), topicNamePattern);
                continue;
            }

            for (OffsetFetchResponseData.OffsetFetchResponsePartition offsetFetchResponsePartition : offsetFetchResponseTopic.partitions()) {
                if (log.isTraceEnabled()) {
                    log.trace("{}: topic={} partition={} offset={} metadata={} before transform",
                        transformerName, offsetFetchResponseTopic.name(),
                        offsetFetchResponsePartition.partitionIndex(), offsetFetchResponsePartition.committedOffset(),
                        offsetFetchResponsePartition.metadata());
                }

                transform(offsetFetchResponseTopic, offsetFetchResponsePartition, version);

                if (log.isTraceEnabled()) {
                    log.trace("{}: topic={} partition={} offset={} metadata={} after transform",
                        transformerName, offsetFetchResponseTopic.name(),
                        offsetFetchResponsePartition.partitionIndex(), offsetFetchResponsePartition.committedOffset(),
                        offsetFetchResponsePartition.metadata());
                }
            }
        }

        return offsetFetchResponseDataOut;
    }

    /**
     * Transform a partition's offset data for v8+ format (with group context).
     */
    protected abstract void transform(
        OffsetFetchResponseData.OffsetFetchResponseGroup group,
        OffsetFetchResponseData.OffsetFetchResponseTopics topic,
        OffsetFetchResponseData.OffsetFetchResponsePartitions partition,
        short version
    );

    /**
     * Transform a partition's offset data for legacy format (v0-v7).
     */
    protected abstract void transform(
        OffsetFetchResponseData.OffsetFetchResponseTopic topic,
        OffsetFetchResponseData.OffsetFetchResponsePartition partition,
        short version
    );
}

