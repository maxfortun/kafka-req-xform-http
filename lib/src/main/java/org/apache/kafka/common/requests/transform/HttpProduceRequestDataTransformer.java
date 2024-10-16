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

    private final String brokerHostname;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final Duration requestTimeout;

    private final String httpHeaderPrefix;
    private final String envRegex;

	private final Map<String,String> config = new HashMap<>();

    public HttpProduceRequestDataTransformer(String transformerName) {
        super(transformerName);

        brokerHostname = System.getenv("HOSTNAME"); 


        String requestTimeoutString = getConfig("requestTimeout");
        if(null != requestTimeoutString) {
            requestTimeout = Duration.parse(requestTimeoutString);
        } else {
            requestTimeout = null;
        }

        httpHeaderPrefix = getConfig("httpHeaderPrefix", transformerName+"-");
        envRegex = getConfig("envRegex");

		/* Configuration that may be overwritten from request */

        config.put("uri", getConfig("uri"));

        // Valid values:
        //   fail:       fail the request
        //   pass-thru:  return the response as-is
        //   original:   return the original request
        config.put("onHttpException", getConfig("onHttpException", "fail"));


    }

    protected String config(RecordHeaders recordHeaders, String name) {
        String key = httpHeaderPrefix+"broker-"+name;
        Header header = recordHeaders.lastHeader(key);
        if(null == header) {
            log.trace("{}: No header {}", transformerName, key);
            return config.get(name);
        }

        String value = Utils.utf8(header.value());
        log.debug("{}: Header {} is {}.", transformerName, key, value);
        return value;
     }

    protected boolean should(RecordHeaders recordHeaders, String name) {
        String value = config(recordHeaders, name);
        if(null == value) {
            return false;
        }
        return Boolean.parseBoolean(value);
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

        if(should(recordHeaders, "bypass")) {
            return record;
        }

        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(new URI(config(recordHeaders, "uri")));
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
            String onHttpException = config(recordHeaders, "onHttpException");

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

        if(null != envRegex && should(recordHeaders, "env")) {
            Map<String,String> env = System.getenv();
            env.entrySet().removeIf(entry -> !entry.getKey().matches(envRegex));
            env.forEach( (key, value) ->  headersMap.put(httpHeaderPrefix+"broker-env-"+key.replaceAll("_","-"), Arrays.asList(value)) );
        }

        Header[] headers = headers(headersMap);

        byte[] body = httpResponse.body();

        log.trace("{}: res body {}", transformerName, body.length, body);
        log.debug("{}: res body String {}", transformerName, body.length, new String(body, StandardCharsets.UTF_8) );

        return newRecord(recordBatch, record, headers, body);
    }
}

