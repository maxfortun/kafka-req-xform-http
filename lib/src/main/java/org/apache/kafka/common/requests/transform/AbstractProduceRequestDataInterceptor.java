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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.InterceptResult;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for produce request interceptors.
 * Handles per-partition error tracking and transforms records within each partition.
 * If any record in a partition fails to transform, the entire partition is marked with an error
 * and won't be written to the log.
 */
public abstract class AbstractProduceRequestDataInterceptor extends AbstractTransformer implements ProduceRequestDataInterceptor {
    private static final Logger log = LoggerFactory.getLogger(AbstractProduceRequestDataInterceptor.class);

    public AbstractProduceRequestDataInterceptor(String transformerName) {
        super(transformerName);
    }

    @Override
    public InterceptResult intercept(ProduceRequestData produceRequestDataIn, short version) {
        ProduceRequestData produceRequestDataOut = produceRequestDataIn.duplicate();
        Map<TopicPartition, Errors> partitionErrors = new HashMap<>();

        // Track DLQ topics: dlqTopic -> partitionIndex -> MemoryRecordsBuilder
        Map<String, Map<Integer, MemoryRecordsBuilder>> dlqBuilders = new HashMap<>();
        String dlqTopicHeader = headerPrefix + "dlq-topic";

        for (RawTaggedField rawTaggedField : produceRequestDataOut.unknownTaggedFields()) {
            log.debug("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), Utils.utf8(rawTaggedField.data()));
        }

        for (ProduceRequestData.TopicProduceData topicProduceData : produceRequestDataOut.topicData()) {

            if (null != topicNamePattern && !topicProduceData.name().matches(topicNamePattern)) {
                log.debug("{}: topicNamePattern {} != {}", transformerName, topicProduceData.name(), topicNamePattern);
                continue;
            }

            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {
                TopicPartition topicPartition = new TopicPartition(topicProduceData.name(), partitionProduceData.index());

                try {
                    interceptPartition(topicProduceData, partitionProduceData, dlqBuilders, dlqTopicHeader, version);
                } catch (PartitionErrorException e) {
                    log.warn("{}: partition {} failed with {}: {}", transformerName, topicPartition, e.error(), e.getMessage());
                    partitionErrors.put(topicPartition, e.error());
                    partitionProduceData.setRecords(MemoryRecords.EMPTY);
                } catch (Throwable t) {
                    log.error("{}: partition {} failed unexpectedly", transformerName, topicPartition, t);
                    partitionErrors.put(topicPartition, Errors.REQUEST_TIMED_OUT);
                    partitionProduceData.setRecords(MemoryRecords.EMPTY);
                }
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

        return new InterceptResult(produceRequestDataOut, partitionErrors);
    }

    private void interceptPartition(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        Map<String, Map<Integer, MemoryRecordsBuilder>> dlqBuilders,
        String dlqTopicHeader,
        short version
    ) throws Exception {
        MemoryRecords memoryRecords = (MemoryRecords) partitionProduceData.records();
        ByteBufferOutputStream recordsOut = new ByteBufferOutputStream(memoryRecords.sizeInBytes());

        int batchId = 0;
        for (MutableRecordBatch recordBatch : memoryRecords.batches()) {

            if (recordBatch.isControlBatch()) {
                recordBatch.writeTo(recordsOut);
                batchId++;
                continue;
            }

            MemoryRecordsBuilder memoryRecordsBuilder = newBatchBuilder(recordBatch, recordBatch.sizeInBytes());

            int recordId = 0;
            for (Record record : recordBatch) {
                RecordHeaders recordHeaders = new RecordHeaders(record.headers());

                Record transformedRecord = intercept(
                    topicProduceData,
                    partitionProduceData,
                    recordBatch,
                    record,
                    recordHeaders,
                    version
                );

                // Check if record should be routed to DLQ
                String dlqTopic = getDlqTopic(transformedRecord, dlqTopicHeader);
                if (dlqTopic != null) {
                    log.debug("{}: routing record to DLQ topic: {}", transformerName, dlqTopic);
                    Record dlqRecord = removeDlqHeader(recordBatch, transformedRecord, dlqTopicHeader);
                    MemoryRecordsBuilder dlqBuilder = getDlqBuilder(dlqBuilders, dlqTopic, partitionProduceData.index(), memoryRecords.sizeInBytes());
                    dlqBuilder.append(dlqRecord.timestamp(), dlqRecord.key(), dlqRecord.value(), dlqRecord.headers());

                    if (recordBatch.hasProducerId()) {
                        memoryRecordsBuilder.appendWithOffset(record.offset(), dlqRecord);
                    }
                } else {
                    memoryRecordsBuilder.appendWithOffset(record.offset(), transformedRecord);
                }

                if (log.isTraceEnabled()) {
                    log.trace("{}: batch[{}].record[{}] transformed", transformerName, batchId, recordId);
                }

                recordId++;
            }

            recordsOut.write(memoryRecordsBuilder.build().buffer());
            batchId++;
        }

        ByteBuffer recordsBuffer = recordsOut.buffer();
        recordsBuffer.flip();
        partitionProduceData.setRecords(MemoryRecords.readableRecords(recordsBuffer));
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

    /**
     * Intercept a single record. Implementations can transform the record and/or throw exceptions.
     *
     * @throws PartitionErrorException to signal a specific error code for this partition
     * @throws Exception any other exception will be treated as REQUEST_TIMED_OUT
     */
    protected abstract Record intercept(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    ) throws Exception;

    /**
     * Exception that signals a specific error code should be returned for a partition.
     */
    public static class PartitionErrorException extends Exception {
        private final Errors error;

        public PartitionErrorException(Errors error, String message) {
            super(message);
            this.error = error;
        }

        public PartitionErrorException(Errors error, String message, Throwable cause) {
            super(message, cause);
            this.error = error;
        }

        public Errors error() {
            return error;
        }
    }
}
