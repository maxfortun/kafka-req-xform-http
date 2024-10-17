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
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpProduceRequestDataTransformer extends AbstractProduceRequestDataTransformer {
    public static final Logger log = LoggerFactory.getLogger(HttpProduceRequestDataTransformer.class);

    private final String brokerHostname;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final Duration requestTimeout;

    private final String envPattern;

    public HttpProduceRequestDataTransformer(String transformerName) {
        super(transformerName);

        brokerHostname = System.getenv("HOSTNAME"); 

        String requestTimeoutString = appConfig("requestTimeout");
        if(null != requestTimeoutString) {
            requestTimeout = Duration.parse(requestTimeoutString);
        } else {
            requestTimeout = null;
        }

        envPattern = appConfig("envPattern");
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

        if(!configured(recordHeaders, "enable", "true")) {
            return record;
        }
        Date inDate = new Date();

        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(new URI(reqConfig(recordHeaders, "uri")));
        if(null != requestTimeout) {
            httpRequestBuilder.timeout(requestTimeout);
        }

        for(Header header : record.headers()) {
            String key = header.key();
            if(key.matches("(?i)^"+headerPrefix)) {
                log.debug("{}: req header {} skipped, because it starts with the http header prefix {}", transformerName, key, headerPrefix);
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
            httpRequestBuilder.header(headerPrefix+"broker-message-key", recordKey);
        }

        httpRequestBuilder.header(headerPrefix+"broker-hostname", brokerHostname);
        httpRequestBuilder.header(headerPrefix+"broker-topic-name", topicProduceData.name());

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
        httpRequestBuilder.header(headerPrefix+"broker-req-time", ""+reqDate.getTime());

        HttpRequest httpRequest = httpRequestBuilder.build();
        log.debug("{}: httpRequest {}", transformerName, httpRequest);

        Map<String, List<String>> headersMap = new HashMap<>();
        byte[] body = new byte[0];

        if(configured(recordHeaders, "enable-send", "true")) {
            HttpResponse<byte[]> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            log.debug("{}: httpResponse {}", transformerName, httpResponse);
            if(httpResponse.statusCode() != 200) {
                String onHttpException = reqConfig(recordHeaders, "onHttpException");

                if("original".equalsIgnoreCase(onHttpException)) {
                    return record;
                }

                if(!"pass-thru".equalsIgnoreCase(onHttpException)) {
                    throw new HttpResponseException(httpResponse);
                }
            }
            headersMap.putAll(httpResponse.headers().map());
            body = httpResponse.body();
        }

        Date resDate = new Date();
        long reqRunTime = resDate.getTime() - reqDate.getTime();

        // Broker headers should never be returned by the called service.
        headersMap.entrySet().removeIf(entry -> entry.getKey().startsWith(headerPrefix+"broker-"));

        if(configured("in-headers", "hostname")) {
            headersMap.put(headerPrefix+"broker-hostname", Arrays.asList(brokerHostname));
        }

        if(null != envPattern && configured(recordHeaders, "in-headers", "env")) {
            Map<String,String> env = new HashMap<>(System.getenv());
            env.entrySet().removeIf(entry -> !entry.getKey().matches(envPattern));
            env.forEach( (key, value) -> headersMap.put(headerPrefix+"broker-env-"+key.replaceAll("_","-"), Arrays.asList(value)) );
        }

        Date outDate = new Date();
        long runTime = outDate.getTime() - inDate.getTime();

        if(configured("in-headers", "time")) {
            headersMap.put(headerPrefix+"broker-in-time", Arrays.asList(""+inDate.getTime()));
            headersMap.put(headerPrefix+"broker-req-time", Arrays.asList(""+reqDate.getTime()));
            headersMap.put(headerPrefix+"broker-res-time", Arrays.asList(""+resDate.getTime()));
            headersMap.put(headerPrefix+"broker-out-time", Arrays.asList(""+outDate.getTime()));
        }

        if(configured("in-headers", "timespan")) {
            headersMap.put(headerPrefix+"broker-req-timespan", Arrays.asList(""+reqRunTime));
            headersMap.put(headerPrefix+"broker-run-timespan", Arrays.asList(""+runTime));
        }

        Header[] headers = headers(headersMap);

        log.trace("{}: res body {}", transformerName, body.length, body);
        log.debug("{}: res body String {}", transformerName, body.length, new String(body, StandardCharsets.UTF_8) );

        return newRecord(recordBatch, record, headers, body);
    }
}

