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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
    private static final Logger log = LoggerFactory.getLogger(HttpProduceRequestDataTransformer.class);

    private final String brokerHostname;

    private final String persistentHeadersPattern;
    private final String transientHeadersPattern;
    private final String envHeadersPattern;

    public HttpProduceRequestDataTransformer(String transformerName) throws Exception {
        super(transformerName);

        brokerHostname = System.getenv("HOSTNAME"); 

        persistentHeadersPattern = appConfig("headers.persistentPattern");
        transientHeadersPattern = appConfig("headers.transientPattern");
        envHeadersPattern = appConfig("headers.envPattern");
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

		AbstractHttpClient httpClient = HttpClients.getHttpClient(recordHeaders, this);
        AbstractHttpRequest httpRequest = httpClient.newHttpRequest(reqConfig(recordHeaders, "uri"));

        Map<String, List<String>> resHeadersMap = new HashMap<>();

        for(Header header : record.headers()) {
            String key = header.key();

            if(key.matches("(?i)^"+headerPrefix)) {
                log.debug("{}: request header {} skipped, because it starts with the http header prefix {}", transformerName, key, headerPrefix);
                continue;
            }

            String value = Utils.utf8(header.value());

            if(null == persistentHeadersPattern || key.matches(persistentHeadersPattern)) {
                try {
                    httpRequest.header(key, value);
                    log.debug("{}: persistent header added to request {}={}", transformerName, key, value);
                } catch(java.lang.IllegalArgumentException e) {
                    log.debug("{}: persistent header not added to request {}={}", transformerName, key, value, e);
                }
            }

            if(null != transientHeadersPattern && key.matches(transientHeadersPattern)) {
                resHeadersMap.put(key, Arrays.asList(value));
                log.debug("{}: transient header retained for response {}={}", transformerName, key, value);
            }
        }

        httpRequest.header(headerPrefix+"broker-hostname", brokerHostname);
        httpRequest.header(headerPrefix+"broker-topic-name", topicProduceData.name());

        String recordKey = null;
        if(null != record.key()) {
            recordKey = Utils.utf8(record.key());
        }
        httpRequest.body(recordKey, record.value());

        Date reqDate = new Date();
        httpRequest.header(headerPrefix+"broker-req-time", ""+reqDate.getTime());

        byte[] body = new byte[0];

        if(configured(recordHeaders, "enable-send", "true")) {
            HttpResponse httpResponse = httpClient.send(httpRequest);
            log.debug("{}: httpResponse {}", transformerName, httpResponse);
            if(httpResponse.statusCode() != 200) {
                String onHttpException = reqConfig(recordHeaders, "httpClient.onException");

                if("original".equalsIgnoreCase(onHttpException)) {
                    return record;
                }

                if(!"pass-thru".equalsIgnoreCase(onHttpException)) {
                    throw new HttpResponseException(httpResponse);
                }
            }
            resHeadersMap.putAll(httpResponse.headers());
            body = httpResponse.body();
        }

        Date resDate = new Date();
        long reqRunTime = resDate.getTime() - reqDate.getTime();

        // Broker headers should never be returned by the called service.
        resHeadersMap.entrySet().removeIf(entry -> entry.getKey().startsWith(headerPrefix+"broker-"));

        if(configured("in-headers", "hostname")) {
            resHeadersMap.put(headerPrefix+"broker-hostname", Arrays.asList(brokerHostname));
        }

        if(null != envHeadersPattern && configured(recordHeaders, "in-headers", "env")) {
            System.getenv().entrySet().stream()
                .filter( entry -> entry.getKey().matches(envHeadersPattern) )
                .forEach( entry -> resHeadersMap.put(headerPrefix+"broker-env-"+entry.getKey().replaceAll("_","-"), Arrays.asList(entry.getValue())) );
        }

        Date outDate = new Date();
        long runTime = outDate.getTime() - inDate.getTime();

        if(configured("in-headers", "time")) {
            resHeadersMap.put(headerPrefix+"broker-in-time", Arrays.asList(""+inDate.getTime()));
            resHeadersMap.put(headerPrefix+"broker-req-time", Arrays.asList(""+reqDate.getTime()));
            resHeadersMap.put(headerPrefix+"broker-res-time", Arrays.asList(""+resDate.getTime()));
            resHeadersMap.put(headerPrefix+"broker-out-time", Arrays.asList(""+outDate.getTime()));
        }

        if(configured("in-headers", "timespan")) {
            resHeadersMap.put(headerPrefix+"broker-req-timespan", Arrays.asList(""+reqRunTime));
            resHeadersMap.put(headerPrefix+"broker-run-timespan", Arrays.asList(""+runTime));
        }

        Header[] headers = headers(resHeadersMap);

        log.trace("{}: res body {}", transformerName, body.length, body);
        log.trace("{}: res body String {}", transformerName, body.length, new String(body, StandardCharsets.UTF_8) );

        return newRecord(recordBatch, record, headers, body);
    }
}

