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

import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProduceRequestDataTransformer extends AbstractTransformer implements ProduceRequestDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(AbstractProduceRequestDataTransformer.class);

    public AbstractProduceRequestDataTransformer(String transformerName) {
        super(transformerName);
    }

    public ProduceRequestData transform(ProduceRequestData produceRequestDataIn, short version) {
        ProduceRequestData produceRequestDataOut = produceRequestDataIn.duplicate();

        // Track DLQ topics: dlqTopic -> partitionIndex -> MemoryRecordsBuilder
        Map<String, Map<Integer, MemoryRecordsBuilder>> dlqBuilders = new HashMap<>();
        String dlqTopicHeader = headerPrefix + "dlq-topic";

        for (RawTaggedField rawTaggedField : produceRequestDataOut.unknownTaggedFields()) {
            log.debug("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), Utils.utf8(rawTaggedField.data()));
        }

        for (ProduceRequestData.TopicProduceData topicProduceData : produceRequestDataOut.topicData()) {

            if(null != topicNamePattern && !topicProduceData.name().matches(topicNamePattern)) {
                log.debug("{}: topicNamePattern {} != {}", transformerName, topicProduceData.name(), topicNamePattern);
                continue;
            }

            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {

                MemoryRecords memoryRecords = (MemoryRecords)partitionProduceData.records();

                MemoryRecordsBuilder memoryRecordsBuilder = MemoryRecords.builder(
                    ByteBuffer.allocate(memoryRecords.sizeInBytes()),
                    CompressionType.NONE,
                    TimestampType.CREATE_TIME,
                    0L
                );


                int batchId = 0;
                for (RecordBatch recordBatch : memoryRecords.batches()) {

                    int recordId = 0;
                    for (Record record : recordBatch) {

                        Record transformedRecord = null;
                        try {
                            transformedRecord = transform(topicProduceData, partitionProduceData, recordBatch, record, new RecordHeaders(record.headers()), version);
                        } catch(Throwable t) {
                            log.warn("{}: topicProduceData.partitionData.recordBatch[{}].record[{}] in:\n{}\n{}  B:{}={}, {}",
                                transformerName, batchId, recordId, record,
                                Utils.toString(record.headers()), Utils.utf8(record.key()), Utils.utf8(record.value()), t
                            );

                            throw t;
                        }

                        // Check if record should be routed to DLQ
                        String dlqTopic = getDlqTopic(transformedRecord, dlqTopicHeader);
                        if (dlqTopic != null) {
                            // Route to DLQ topic
                            log.debug("{}: routing record to DLQ topic: {}", transformerName, dlqTopic);
                            Record dlqRecord = removeDlqHeader(recordBatch, transformedRecord, dlqTopicHeader);
                            MemoryRecordsBuilder dlqBuilder = getDlqBuilder(dlqBuilders, dlqTopic, partitionProduceData.index(), memoryRecords.sizeInBytes());
                            dlqBuilder.append(dlqRecord);
                        } else {
                            // Normal routing to original topic
                            memoryRecordsBuilder.append(transformedRecord);
                        }

                        if(log.isTraceEnabled()) {
                            log.trace("{}: topicProduceData.partitionData.recordBatch[{}].record[{}] in:\n{}\n{}  B:{}={}",
                                transformerName, batchId, recordId, record,
                                Utils.toString(record.headers()), Utils.utf8(record.key()), Utils.utf8(record.value())
                            );

                            log.trace("{}: topicProduceData.partitionData.recordBatch[{}].record[{}] out:\n{}\n{}  B:{}={}",
                                transformerName, batchId, recordId, transformedRecord,
                                Utils.toString(transformedRecord.headers()), Utils.utf8(transformedRecord.key()), Utils.utf8(transformedRecord.value())
                            );
                        }

                        recordId++;
                    }

                    batchId++;
                }

                partitionProduceData.setRecords(memoryRecordsBuilder.build());
            }
        }

        // Add DLQ topics to the output
        for (Map.Entry<String, Map<Integer, MemoryRecordsBuilder>> dlqEntry : dlqBuilders.entrySet()) {
            String dlqTopicName = dlqEntry.getKey();
            ProduceRequestData.TopicProduceData dlqTopicData = new ProduceRequestData.TopicProduceData();
            dlqTopicData.setName(dlqTopicName);

            for (Map.Entry<Integer, MemoryRecordsBuilder> partitionEntry : dlqEntry.getValue().entrySet()) {
                ProduceRequestData.PartitionProduceData dlqPartitionData = new ProduceRequestData.PartitionProduceData();
                dlqPartitionData.setIndex(partitionEntry.getKey());
                dlqPartitionData.setRecords(partitionEntry.getValue().build());
                dlqTopicData.partitionData().add(dlqPartitionData);
            }

            log.debug("{}: adding DLQ topic {} with {} partitions", transformerName, dlqTopicName, dlqTopicData.partitionData().size());
            produceRequestDataOut.topicData().add(dlqTopicData);
        }

        return produceRequestDataOut;
    }

    private String getDlqTopic(Record record, String dlqTopicHeader) {
        for (Header header : record.headers()) {
            if (dlqTopicHeader.equals(header.key())) {
                return Utils.utf8(header.value());
            }
        }
        return null;
    }

    private MemoryRecordsBuilder getDlqBuilder(Map<String, Map<Integer, MemoryRecordsBuilder>> dlqBuilders, String dlqTopic, int partitionIndex, int initialSize) {
        return dlqBuilders
            .computeIfAbsent(dlqTopic, k -> new HashMap<>())
            .computeIfAbsent(partitionIndex, k -> MemoryRecords.builder(
                ByteBuffer.allocate(initialSize),
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L
            ));
    }

    private Record removeDlqHeader(RecordBatch recordBatch, Record record, String dlqTopicHeader) {
        try {
            RecordHeaders headers = new RecordHeaders(record.headers());
            headers.remove(dlqTopicHeader);
            return newRecord(recordBatch, record, headers.toArray(), record.value());
        } catch (Exception e) {
            log.warn("{}: failed to remove DLQ header, using original record", transformerName, e);
            return record;
        }
    }

    protected abstract Record transform(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    );
}

