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

import java.util.Arrays;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.Optional;
import java.util.Map;
import java.util.List;

import java.io.IOException;
import java.io.DataOutputStream;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProduceRequestDataTransformer implements ProduceRequestDataTransformer {
    public static final Logger log = LoggerFactory.getLogger(AbstractProduceRequestDataTransformer.class);

    protected String transformerName;

    private ResourceBundle resources = null;

    private final String topicNamePattern;

    public AbstractProduceRequestDataTransformer(String transformerName) {
        this.transformerName = transformerName;

        topicNamePattern = getConfig("topicNamePattern");
    }

    protected String getConfig(String key, String defaultValue) {
        String value = getConfig(key);
        if(null != value) {
            return value;
        }
        return defaultValue;
    }

    protected String getConfig(String key) {
        String fullKey = transformerName+"-"+key;
        String value = System.getProperty(fullKey);
        if(null != value) {
            log.trace("{}: getConfig prop {} = {}", transformerName, fullKey, value);
            return value;
        }
        
        fullKey = transformerName.replaceAll("[.-]", "_")+"_"+key;
        value = System.getenv(fullKey);
        if(null != value) {
            log.trace("{}: getConfig env {} = {}", transformerName, fullKey, value);
            return value;
        }
        
        try {
            if(null == resources) {
                resources = ResourceBundle.getBundle(transformerName);
            }

            fullKey = key;
            value = resources.getString(key);
        } catch(Exception e) {
        }

        if(null != value) {
            log.trace("{}: getConfig bundle {} = {}", transformerName, fullKey, value);
            return value;
        }

        log.trace("{}: getConfig {} = null", transformerName, key);
        return null;
    }

    public ProduceRequestData transform(ProduceRequestData produceRequestDataIn, short version) {
        ProduceRequestData produceRequestDataOut = produceRequestDataIn.duplicate();

        for (RawTaggedField rawTaggedField : produceRequestDataOut.unknownTaggedFields()) {
            log.trace("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), LogUtils.toString(rawTaggedField.data()));
        }

        for (ProduceRequestData.TopicProduceData topicProduceData : produceRequestDataOut.topicData()) {

            if(null != topicNamePattern && !topicProduceData.name().matches(topicNamePattern)) {
                log.trace("{}: topicNamePattern {} != {}", transformerName, topicProduceData.name(), topicNamePattern);
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

                        Record transformedRecord = transform(topicProduceData, partitionProduceData, recordBatch, record, version);
                        memoryRecordsBuilder.append(transformedRecord);

                        log.trace("{}: topicProduceData.partitionData.recordBatch[{}].record[{}] in:\n{}\n{}  B:{}={}",
                            transformerName, batchId, recordId, record,
                            LogUtils.toString(record.headers()), LogUtils.toString(record.key()), LogUtils.toString(record.value())
                        );

                        log.trace("{}: topicProduceData.partitionData.recordBatch[{}].record[{}] out:\n{}\n{}  B:{}={}",
                            transformerName, batchId, recordId, transformedRecord,
                            LogUtils.toString(transformedRecord.headers()), LogUtils.toString(transformedRecord.key()), LogUtils.toString(transformedRecord.value())
                        );

                        recordId++;
                    }

                    batchId++;
                }

                partitionProduceData.setRecords(memoryRecordsBuilder.build());
            }
        }

        return produceRequestDataOut;
    }

    protected Header lastHeader(Record record, String key) {
        Optional<Header> optional = Arrays.stream(record.headers())
                                   .filter(header -> key.equals(header.key()))
                                   .reduce((a, b) -> b);

        if(optional.isPresent()) {
            return optional.get();//get it from optional
        }

        return null;
    }

    protected abstract Record transform(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        RecordBatch recordBatch,
        Record record,
        short version
    );

	protected Header[] headers(Map<String, List<String>> map) {
        Set<String> keys = map.keySet();
        Header[] headers = new Header[keys.size()];
        int headerId = 0;
        for(String key : keys) {
            String value = String.join(",", map.get(key));
            log.trace("{}: header {}={}", transformerName, key, value);
            headers[headerId++] = new RecordHeader(key, value.getBytes());
        }
		return headers;
	}

	protected Record newRecord(RecordBatch recordBatch, Record record, Header[] headers, byte[] body) throws IOException {
        ByteBufferOutputStream out = new ByteBufferOutputStream(1024);
        DefaultRecord.writeTo(
            new DataOutputStream(out),
            (int)record.offset(),
            record.timestamp(),
            record.key(),
            ByteBuffer.wrap(body),
            headers
        );
        ByteBuffer buffer = out.buffer();
        buffer.flip();
        
        long timestamp = recordBatch.timestampType() == TimestampType.LOG_APPEND_TIME ?
            recordBatch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;

        DefaultRecord newRecord = DefaultRecord.readFrom(
            buffer,
            recordBatch.baseOffset(),
            timestamp,
            recordBatch.baseSequence(),
            null
        );

        log.trace("{}: newRecord {}", transformerName, newRecord);
        return newRecord;
    }
}

