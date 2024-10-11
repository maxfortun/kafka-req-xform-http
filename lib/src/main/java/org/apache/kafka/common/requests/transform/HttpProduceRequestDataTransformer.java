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

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.net.URISyntaxException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.time.Duration;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
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

public class HttpProduceRequestDataTransformer extends AbstractProduceRequestDataTransformer {
    public static final Logger log = LoggerFactory.getLogger(HttpProduceRequestDataTransformer.class);

    private HttpClient httpClient = HttpClient.newHttpClient();
    private URI uri;
    private Duration requestTimeout;
    private final String onHttpExceptionConfig;
    private final String httpHeaderPrefix;
    private final String brokerHostname;

    public HttpProduceRequestDataTransformer(String transformerName) {
        super(transformerName);
        uri = URI.create(getConfig("uri"));
        String requestTimeoutString = getConfig("requestTimeout");
        if(null != requestTimeoutString) {
            requestTimeout = Duration.parse(requestTimeoutString);
        }

        // Valid values:
        //   fail:       fail the request
        //   pass-thru:  return the response as-is
        //   original:   return the original request
        onHttpExceptionConfig = getConfig("onHttpException", "fail");
        httpHeaderPrefix = getConfig("httpHeaderPrefix", transformerName+"-");
        brokerHostname = System.getenv("HOSTNAME"); 
    }

    protected boolean shouldBypass(RecordHeaders recordHeaders) {
        String shouldBypassKey = httpHeaderPrefix+"broker-bypass";
        Header shouldBypassHeader = recordHeaders.lastHeader(shouldBypassKey);
        if(null == shouldBypassHeader) {
            log.trace("{}: No header {}", transformerName, shouldBypassKey);
            return false;
        }

        String shouldBypassValue = Utils.utf8(shouldBypassHeader.value());
        Boolean shouldBypassBool = Boolean.parseBoolean(shouldBypassValue);
        if(shouldBypassBool) {
            log.debug("{}: Bypassing record. Header {} is {}.", transformerName, shouldBypassKey, shouldBypassValue);
        } else {
            log.trace("{}: Not bypassing record. Header {} is {}.", transformerName, shouldBypassKey, shouldBypassValue);
        }

        return shouldBypassBool;
    }

    protected Record transform(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    ) {
        try {
            return transformImpl(
                topicProduceData,
                partitionProduceData,
                recordBatch,
                record,
                recordHeaders,
                version
            );
        } catch(Exception e) {
            log.debug("{}", transformerName, e);
            throw new InvalidRequestException(transformerName, e);
        }
    }

    protected Record transformImpl(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    ) throws Exception {

        if(shouldBypass(recordHeaders)) {
            return record;
        }

        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(getURI(record));
        if(null != requestTimeout) {
            httpRequestBuilder.timeout(requestTimeout);
        }

        for(Header header : record.headers()) {
            String key = header.key();
            if(key.matches("(?i)^"+httpHeaderPrefix)) {
                log.debug("{}: req header {} skipped, because it starts with the http header prefix {}", transformerName, key, httpHeaderPrefix);
                continue;
            }
            String value = Utils.utf8(header.value());

            try {
                httpRequestBuilder.header(key, value);
                log.debug("{}: req header added {}={}", transformerName, key, value);
            } catch(java.lang.IllegalArgumentException e) {
                log.debug("{}: req header added {}={}", transformerName, key, value, e);
            }
        }

        String recordKey = null;
        if(null != record.key()) {
            recordKey = Utils.utf8(record.key());
        }
        if(!org.apache.kafka.common.utils.Utils.isBlank(recordKey)) {
            httpRequestBuilder.header(httpHeaderPrefix+"broker-message-key", recordKey);
        }

        httpRequestBuilder.header(httpHeaderPrefix+"broker-hostname", brokerHostname);
        httpRequestBuilder.header(httpHeaderPrefix+"broker-topic-name", topicProduceData.name());

        ByteBuffer bodyByteBuffer = record.value();
        int position = bodyByteBuffer.position();
        int arrayOffset = bodyByteBuffer.arrayOffset();
        log.trace("{}: req bodyByteBuffer {} {} {}", transformerName, position, arrayOffset, bodyByteBuffer.array());

        byte[] bodyArray = new byte[bodyByteBuffer.remaining()];
        bodyByteBuffer.get(bodyArray, 0, bodyArray.length); 
        log.trace("{}: req bodyArray {} {}", transformerName, bodyArray.length, bodyArray);
        log.debug("{}: req bodyArray String {} {}", transformerName, bodyArray.length, new String(bodyArray, StandardCharsets.UTF_8));

        HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(bodyArray);
        log.trace("{}: bodyPublisher {}", transformerName, bodyPublisher);
        httpRequestBuilder.POST(bodyPublisher);

        Date reqDate = new Date();
        httpRequestBuilder.header(httpHeaderPrefix+"broker-req-time", ""+reqDate.getTime());

        HttpRequest httpRequest = httpRequestBuilder.build();
        log.debug("{}: httpRequest {}", transformerName, httpRequest);

        HttpResponse<byte[]> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
        log.debug("{}: httpResponse {}", transformerName, httpResponse);
        if(httpResponse.statusCode() != 200) {

            String onHttpException = onHttpExceptionConfig;
            Header onHttpExceptionHeader = lastHeader(record, transformerName+"-onHttpException");
            if(null != onHttpExceptionHeader) {
                onHttpException = Utils.utf8(onHttpExceptionHeader.value());
            }

            if("original".equalsIgnoreCase(onHttpException)) {
                return record;
            }

            if(!"pass-thru".equalsIgnoreCase(onHttpException)) {
                throw new HttpResponseException(httpResponse);
            }
        }

        Date resDate = new Date();
        long runTime = resDate.getTime() - reqDate.getTime();

        Map<String, List<String>> headersMap = new HashMap<>(httpResponse.headers().map());

        // Broker headers should never be returned by the called service.
        headersMap.entrySet().removeIf(entry -> entry.getKey().startsWith(httpHeaderPrefix+"broker-"));

        headersMap.put(httpHeaderPrefix+"broker-hostname", Arrays.asList(brokerHostname));
        headersMap.put(httpHeaderPrefix+"broker-req-time", Arrays.asList(""+reqDate.getTime()));
        headersMap.put(httpHeaderPrefix+"broker-res-time", Arrays.asList(""+resDate.getTime()));
        headersMap.put(httpHeaderPrefix+"broker-run-timespan", Arrays.asList(""+runTime));

        Header[] headers = headers(headersMap);

        byte[] body = httpResponse.body();

        log.trace("{}: res body {}", transformerName, body.length, body);
        log.debug("{}: res body String {}", transformerName, body.length, new String(body, StandardCharsets.UTF_8) );

        return newRecord(recordBatch, record, headers, body);
    }

    private URI getURI(Record record) throws URISyntaxException {
        Header recordURIHeader = lastHeader(record, transformerName+"broker-uri");
        if(null != recordURIHeader) {
            return new URI(Utils.utf8(recordURIHeader.value()));
        }
        return uri;
    }
}

