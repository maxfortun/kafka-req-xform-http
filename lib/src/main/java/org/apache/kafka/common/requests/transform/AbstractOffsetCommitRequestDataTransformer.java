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

import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOffsetCommitRequestDataTransformer extends AbstractTransformer implements OffsetCommitRequestDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(AbstractOffsetCommitRequestDataTransformer.class);

    protected final String groupIdPattern;

    public AbstractOffsetCommitRequestDataTransformer(String transformerName) {
        super(transformerName);
        groupIdPattern = appConfig("groups.idPattern");
    }

    public OffsetCommitRequestData transform(OffsetCommitRequestData offsetCommitRequestDataIn, short version) {
        OffsetCommitRequestData offsetCommitRequestDataOut = offsetCommitRequestDataIn.duplicate();

        for (RawTaggedField rawTaggedField : offsetCommitRequestDataOut.unknownTaggedFields()) {
            log.debug("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), Utils.utf8(rawTaggedField.data()));
        }

        String groupId = offsetCommitRequestDataOut.groupId();

        if (null != groupIdPattern && !groupId.matches(groupIdPattern)) {
            log.debug("{}: groupIdPattern {} != {}", transformerName, groupId, groupIdPattern);
            return offsetCommitRequestDataOut;
        }

        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : offsetCommitRequestDataOut.topics()) {
            if (null != topicNamePattern && !topic.name().matches(topicNamePattern)) {
                log.debug("{}: topicNamePattern {} != {}", transformerName, topic.name(), topicNamePattern);
                continue;
            }

            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                if (log.isTraceEnabled()) {
                    log.trace("{}: group={} topic={} partition={} offset={} leaderEpoch={} metadata={} before transform",
                        transformerName, groupId, topic.name(),
                        partition.partitionIndex(), partition.committedOffset(),
                        partition.committedLeaderEpoch(), partition.committedMetadata());
                }

                transform(offsetCommitRequestDataOut, topic, partition, version);

                if (log.isTraceEnabled()) {
                    log.trace("{}: group={} topic={} partition={} offset={} leaderEpoch={} metadata={} after transform",
                        transformerName, groupId, topic.name(),
                        partition.partitionIndex(), partition.committedOffset(),
                        partition.committedLeaderEpoch(), partition.committedMetadata());
                }
            }
        }

        return offsetCommitRequestDataOut;
    }

    /**
     * Transform a partition's offset commit data.
     *
     * @param request The full request data (contains groupId, memberId, etc.)
     * @param topic The topic being committed
     * @param partition The partition data being committed
     * @param version The API version
     */
    protected abstract void transform(
        OffsetCommitRequestData request,
        OffsetCommitRequestData.OffsetCommitRequestTopic topic,
        OffsetCommitRequestData.OffsetCommitRequestPartition partition,
        short version
    );
}
