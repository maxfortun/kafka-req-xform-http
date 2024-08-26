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

import java.util.ResourceBundle;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.Iterator;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.header.Header;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPProduceRequestDataTransformer implements ProduceRequestDataTransformer {
    public static final Logger log = LoggerFactory.getLogger(HTTPProduceRequestDataTransformer.class);

    private String transformerName;

    private ResourceBundle resources = null;

    private HttpClient httpClient = HttpClient.newHttpClient();
    private URI uri;

    public HTTPProduceRequestDataTransformer(String transformerName) {
        this.transformerName = transformerName;
        uri = URI.create(getConfig("uri"));
    }

    private String getConfig(String key) {
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
        
        if(null == resources) {
            resources = ResourceBundle.getBundle(transformerName);
        }

        fullKey = key;
        value = resources.getString(key);
        if(null != value) {
            log.trace("{}: getConfig env {} = {}", transformerName, fullKey, value);
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
            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {
                int batchId = 0;

                // may need to create new records here

                MemoryRecords memoryRecords = (MemoryRecords)partitionProduceData.records();
                for (Iterator<? extends RecordBatch> iter = memoryRecords.batchIterator(); iter.hasNext(); batchId++) {
                    RecordBatch recordBatch = iter.next();
                    int recordId = 0;
                    for (Record record : recordBatch) {
                        transform(record, version);
                        log.trace("{}: topicProduceData.partitionData.recordBatch[{}].record[{}]:\n{}  B:{}={}",
                            transformerName, batchId, recordId++,
                            LogUtils.toString(record.headers()), LogUtils.toString(record.key()), LogUtils.toString(record.value())
                        );
                    }
                }
            }
        }

        return produceRequestDataOut;
    }

    // break the infinite loop breaker kafka -> lake -> kafka

    private void transform(Record record, short version) {
        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(uri);

        for(Header header : record.headers()) {
			String key = header.key();
			String value = LogUtils.toString(header.value());
        	log.trace("{}: header {} {}", transformerName, key, value);
            httpRequestBuilder.header(key, value);
        }

        ByteBuffer bodyByteBuffer = record.value();
		int position = bodyByteBuffer.position();
		int arrayOffset = bodyByteBuffer.arrayOffset();
        log.trace("{}: bodyByteBuffer {} {} {}", transformerName, position, arrayOffset, bodyByteBuffer.array());

        byte[] bodyArray = new byte[bodyByteBuffer.remaining()];
		bodyByteBuffer.get(bodyArray, 0, bodyArray.length); 
        log.trace("{}: bodyArray {} {} {}", transformerName, bodyArray.length, bodyArray, new String(bodyArray, StandardCharsets.UTF_8));

		HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(bodyArray);
        log.trace("{}: bodyPublisher {}", transformerName, bodyPublisher);
        httpRequestBuilder.POST(bodyPublisher);

        HttpRequest httpRequest = httpRequestBuilder.build();
        log.trace("{}: httpRequest {}", transformerName, httpRequest);

        try {
            HttpResponse<byte[]> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            log.trace("{}: httpResponse {}", transformerName, httpResponse);
            for(String name : httpResponse.headers().map().keySet()) {
                String value = String.join(",", httpResponse.headers().allValues(name));
                // record set header to value.get();
            }

            byte[] body = httpResponse.body();
            ByteBuffer responseBuffer = ByteBuffer.allocate(body.length);
            responseBuffer.put(body);
    
            log.trace("{}: responseBuffer {}", transformerName, LogUtils.toString(responseBuffer));
            // record set key value
        } catch(Exception e) {
            log.debug("{}: httpRequest {}", transformerName, httpRequest, e);
            // throw new InvalidRequestException(httpRequest.toString(), e);
        }
    }
}

