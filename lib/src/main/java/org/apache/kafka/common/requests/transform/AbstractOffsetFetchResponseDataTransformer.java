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
import org.apache.kafka.common.message.OffsetFetchResponseData;
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

public abstract class AbstractOffsetFetchResponseDataTransformer extends AbstractTransformer implements OffsetFetchResponseDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(AbstractOffsetFetchResponseDataTransformer.class);

    public AbstractOffsetFetchResponseDataTransformer(String transformerName) {
        super(transformerName);
    }

    public OffsetFetchResponseData transform(OffsetFetchResponseData offsetFetchResponseDataIn, short version) {
        OffsetFetchResponseData offsetFetchResponseDataOut = offsetFetchResponseDataIn.duplicate();

        for (RawTaggedField rawTaggedField : offsetFetchResponseDataOut.unknownTaggedFields()) {
            log.debug("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), Utils.utf8(rawTaggedField.data()));
        }

        for (OffsetFetchResponseData.OffsetFetchResponseTopic offsetFetchResponseTopic : offsetFetchResponseDataOut.topics()) {

            if(null != topicNamePattern && !offsetFetchResponseTopic.name().matches(topicNamePattern)) {
                log.debug("{}: topicNamePattern {} != {}", transformerName, offsetFetchResponseTopic.name(), topicNamePattern);
                continue;
            }

            for (OffsetFetchResponseData.OffsetFetchResponsePartition offsetFetchResponsePartition : offsetFetchResponseTopic.partitions()) {

/*
                MemoryRecords memoryRecords = (MemoryRecords)offsetFetchResponsePartition.records();

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

                        Record transformedRecord = transform(offsetFetchResponseTopic, offsetFetchResponsePartition, recordBatch, record, new RecordHeaders(record.headers()), version);
                        memoryRecordsBuilder.append(transformedRecord);

                        if(log.isTraceEnabled()) {
                            log.trace("{}: offsetFetchResponseTopic.offsetFetchResponsePartition.recordBatch[{}].record[{}] in:\n{}\n{}  B:{}={}",
                                transformerName, batchId, recordId, record,
                                Utils.toString(record.headers()), Utils.utf8(record.key()), Utils.utf8(record.value())
                            );

                            log.trace("{}: offsetFetchResponseTopic.offsetFetchResponsePartition.recordBatch[{}].record[{}] out:\n{}\n{}  B:{}={}",
                                transformerName, batchId, recordId, transformedRecord,
                                Utils.toString(transformedRecord.headers()), Utils.utf8(transformedRecord.key()), Utils.utf8(transformedRecord.value())
                            );
                        }

                        recordId++;
                    }

                    batchId++;
                }

                offsetFetchResponsePartition.setRecords(memoryRecordsBuilder.build());
*/
            }
        }

        return offsetFetchResponseDataOut;
    }

    protected abstract Record transform(
        OffsetFetchResponseData.OffsetFetchResponseTopic offsetFetchResponseTopic,
        OffsetFetchResponseData.OffsetFetchResponsePartition offsetFetchResponsePartition,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    );
}

