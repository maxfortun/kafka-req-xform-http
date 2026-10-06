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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFetchResponseDataTransformer extends AbstractTransformer implements FetchResponseDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(AbstractFetchResponseDataTransformer.class);

    public AbstractFetchResponseDataTransformer(String transformerName) {
        super(transformerName);
    }

    public FetchResponseData transform(FetchResponseData fetchResponseDataIn, short version) {
        FetchResponseData fetchResponseDataOut = fetchResponseDataIn.duplicate();

        for (RawTaggedField rawTaggedField : fetchResponseDataOut.unknownTaggedFields()) {
            log.debug("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), Utils.utf8(rawTaggedField.data()));
        }

        for (FetchResponseData.FetchableTopicResponse fetchableTopicResponse : fetchResponseDataOut.responses()) {

            if(null != topicNamePattern && !fetchableTopicResponse.topic().matches(topicNamePattern)) {
                log.debug("{}: topicNamePattern {} != {}", transformerName, fetchableTopicResponse.topic(), topicNamePattern);
                continue;
            }

            for (FetchResponseData.PartitionData partitionData : fetchableTopicResponse.partitions()) {

                MemoryRecords memoryRecords = (MemoryRecords)partitionData.records();

                ByteBufferOutputStream recordsOut = new ByteBufferOutputStream(memoryRecords.sizeInBytes());

                int batchId = 0;
                for (MutableRecordBatch recordBatch : memoryRecords.batches()) {

                    // Transaction markers must reach the consumer unchanged for read_committed to work.
                    if (recordBatch.isControlBatch()) {
                        recordBatch.writeTo(recordsOut);
                        batchId++;
                        continue;
                    }

                    // One output batch per input batch, keeping offsets, producer ids and the transactional flag.
                    MemoryRecordsBuilder memoryRecordsBuilder = newBatchBuilder(recordBatch, recordBatch.sizeInBytes());

                    int recordId = 0;
                    for (Record record : recordBatch) {

                        Record transformedRecord = transform(fetchableTopicResponse, partitionData, recordBatch, record, new RecordHeaders(record.headers()), version);
                        memoryRecordsBuilder.appendWithOffset(record.offset(), transformedRecord);

                        if(log.isTraceEnabled()) {
                            log.trace("{}: fetchableTopicResponse.partitionData.recordBatch[{}].record[{}] in:\n{}\n{}  B:{}={}",
                                transformerName, batchId, recordId, record,
                                Utils.toString(record.headers()), Utils.utf8(record.key()), Utils.utf8(record.value())
                            );

                            log.trace("{}: fetchableTopicResponse.partitionData.recordBatch[{}].record[{}] out:\n{}\n{}  B:{}={}",
                                transformerName, batchId, recordId, transformedRecord,
                                Utils.toString(transformedRecord.headers()), Utils.utf8(transformedRecord.key()), Utils.utf8(transformedRecord.value())
                            );
                        }

                        recordId++;
                    }

                    recordsOut.write(memoryRecordsBuilder.build().buffer());
                    batchId++;
                }

                ByteBuffer recordsBuffer = recordsOut.buffer();
                recordsBuffer.flip();
                partitionData.setRecords(MemoryRecords.readableRecords(recordsBuffer));
            }
        }

        return fetchResponseDataOut;
    }

    protected abstract Record transform(
        FetchResponseData.FetchableTopicResponse fetchableTopicResponse,
        FetchResponseData.PartitionData partitionData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    );
}

