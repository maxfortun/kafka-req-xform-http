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

    private ResourceBundle resources;

    private HttpClient httpClient = HttpClient.newHttpClient();
    private URI uri;

    public HTTPProduceRequestDataTransformer(String transformerName) {
        this.transformerName = transformerName;
        resources = ResourceBundle.getBundle("HTTPProduceRequestDataTransformer");
        uri = URI.create(resources.getString("uri"));
    }

    public ProduceRequestData transform(ProduceRequestData produceRequestData, short version) {
        for (RawTaggedField rawTaggedField : produceRequestData.unknownTaggedFields()) {
            log.trace("{}: rawTaggedField {} = {}", transformerName, rawTaggedField.tag(), LogUtils.toString(rawTaggedField.data()));
        }

        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(uri);

        for (ProduceRequestData.TopicProduceData topicProduceData : produceRequestData.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {
                int batchId = 0;
                for (Iterator<? extends RecordBatch> iter = ((MemoryRecords)partitionProduceData.records()).batchIterator(); iter.hasNext(); batchId++) {
                    RecordBatch recordBatch = iter.next();
                    int recordId = 0;
                    for (Record record : recordBatch) {
                        for(Header header : record.headers()) {
                            httpRequestBuilder.header(header.key(), LogUtils.toString(header.value()));
                        }
						ByteBuffer bodyByteBuffer = record.value();
						byte[] bodyArray = bodyByteBuffer.array();
                        httpRequestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(bodyArray, bodyByteBuffer.arrayOffset(), bodyArray.length));

                        log.trace("{}: topicProduceData.partitionData.recordBatch[{}].record[{}]:\n{}  B:{}={}",
                            transformerName, batchId, recordId++,
                            LogUtils.toString(record.headers()), LogUtils.toString(record.key()), LogUtils.toString(record.value())
                        );
                    }
                }
            }
        }

        HttpRequest httpRequest = httpRequestBuilder.build();
        log.trace("{}: httpRequest {}", transformerName, httpRequest);
/*
            HttpResponse<byte[]> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());

            byte[] body = httpResponse.body();
            ByteBuffer responseBuffer = ByteBuffer.allocate(body.length);
            responseBuffer.put(body);

            return new ProduceRequest(new ProduceRequestData(new ByteBufferAccessor(responseBuffer), version), version);
*/

        return produceRequestData;
    }
}