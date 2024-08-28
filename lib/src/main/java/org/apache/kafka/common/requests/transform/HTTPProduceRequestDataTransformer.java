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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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

public class HTTPProduceRequestDataTransformer extends AbstractProduceRequestDataTransformer {
    public static final Logger log = LoggerFactory.getLogger(HTTPProduceRequestDataTransformer.class);

    private HttpClient httpClient = HttpClient.newHttpClient();
    private URI uri;
    private final String onHttpExceptionConfig;
    private final String httpHeaderPrefix;

    public HTTPProduceRequestDataTransformer(String transformerName) {
		super(transformerName);
        uri = URI.create(getConfig("uri"));

        // Valid values:
        //   fail:       fail the request
        //   pass-thru:  return the response as-is
        //   original:   return the original request
        onHttpExceptionConfig = getConfig("onHttpException", "fail");
        httpHeaderPrefix = getConfig("httpHeaderPrefix", transformerName+"-");
    }

    protected Record transform(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        RecordBatch recordBatch,
        Record record,
        short version
    ) {
        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(uri);

        for(Header header : record.headers()) {
            String key = header.key();
			if(key.matches("(?i)^"+httpHeaderPrefix)) {
            	log.trace("{}: req header {} skipped, because it starts with the http header prefix {}", transformerName, key, httpHeaderPrefix);
				continue;
			}
            String value = LogUtils.toString(header.value());
            log.trace("{}: req header added {}={}", transformerName, key, value);
            httpRequestBuilder.header(key, value);
        }
        httpRequestBuilder.header(httpHeaderPrefix+"topic-name", topicProduceData.name());

        ByteBuffer bodyByteBuffer = record.value();
        int position = bodyByteBuffer.position();
        int arrayOffset = bodyByteBuffer.arrayOffset();
        log.trace("{}: req bodyByteBuffer {} {} {}", transformerName, position, arrayOffset, bodyByteBuffer.array());

        byte[] bodyArray = new byte[bodyByteBuffer.remaining()];
        bodyByteBuffer.get(bodyArray, 0, bodyArray.length); 
        log.trace("{}: req bodyArray {} {} {}", transformerName, bodyArray.length, bodyArray, new String(bodyArray, StandardCharsets.UTF_8));

        HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(bodyArray);
        log.trace("{}: bodyPublisher {}", transformerName, bodyPublisher);
        httpRequestBuilder.POST(bodyPublisher);

        HttpRequest httpRequest = httpRequestBuilder.build();
        log.trace("{}: httpRequest {}", transformerName, httpRequest);

        try {
            HttpResponse<byte[]> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            log.trace("{}: httpResponse {}", transformerName, httpResponse);
            if(httpResponse.statusCode() != 200) {

                String onHttpException = onHttpExceptionConfig;
                Header onHttpExceptionHeader = lastHeader(record, transformerName+"-onHttpException");
                if(null != onHttpExceptionHeader) {
                    onHttpException = new String(onHttpExceptionHeader.value(), StandardCharsets.UTF_8);
                }
    
                if("original".equalsIgnoreCase(onHttpException)) {
                    return record;
                }
    
                if(!"pass-thru".equalsIgnoreCase(onHttpException)) {
                    throw new HttpResponseException(httpResponse);
                }
            }

			Header[] headers = headers(httpResponse.headers().map());
            byte[] body = httpResponse.body();
            log.trace("{}: res body {} {}", transformerName, body.length, body, new String(body, StandardCharsets.UTF_8) );
			return newRecord(recordBatch, record, headers, body);
        } catch(Exception e) {
            log.debug("{}: httpRequest {}", transformerName, httpRequest, e);
            throw new InvalidRequestException(httpRequest.toString(), e);
        }
    }
}

