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
import java.util.stream.Collectors;

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

    private static final String brokerHostname = System.getenv("HOSTNAME");

    private final String headerPrefixPattern;
    private final String persistentHeadersPattern;
    private final String envHeadersPattern;

    public HttpProduceRequestDataTransformer(String transformerName) throws Exception {
        super(transformerName);

        headerPrefixPattern = "(?i)^"+headerPrefix+".*$";
        persistentHeadersPattern = appConfig("headers.persistentPattern");
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
            log.warn("{}: transform failed", transformerName, e);

            String onException = reqConfig(recordHeaders, "onException");

            if ("original".equalsIgnoreCase(onException)) {
                log.debug("{}: onException=original, returning original record", transformerName);
                return record;
            }

            if ("headers".equalsIgnoreCase(onException)) {
                log.debug("{}: onException=headers, returning record with error headers", transformerName);
                try {
                    addErrorHeaders(recordHeaders, e);
                    return newRecord(recordBatch, record, recordHeaders.toArray(), record.value());
                } catch (Exception innerEx) {
                    log.error("{}: failed to create error record", transformerName, innerEx);
                    throw new InvalidRequestException(transformerName, e);
                }
            }

            if ("dlq".equalsIgnoreCase(onException)) {
                log.debug("{}: onException=dlq, routing record to dead-letter queue", transformerName);
                try {
                    addErrorHeaders(recordHeaders, e);
                    String dlqTopic = reqConfig(recordHeaders, "onException.dlqTopic");
                    if (dlqTopic == null || dlqTopic.isEmpty()) {
                        dlqTopic = topicProduceData.name() + "-dlq";
                    }
                    recordHeaders.remove(headerPrefix + "dlq-topic");
                    recordHeaders.add(headerPrefix + "dlq-topic", dlqTopic.getBytes(StandardCharsets.UTF_8));
                    recordHeaders.remove(headerPrefix + "original-topic");
                    recordHeaders.add(headerPrefix + "original-topic", topicProduceData.name().getBytes(StandardCharsets.UTF_8));
                    log.debug("{}: record will be routed to DLQ topic: {}", transformerName, dlqTopic);
                    return newRecord(recordBatch, record, recordHeaders.toArray(), record.value());
                } catch (Exception innerEx) {
                    log.error("{}: failed to create DLQ record", transformerName, innerEx);
                    throw new InvalidRequestException(transformerName, e);
                }
            }

            // Default behavior: throw
            throw new InvalidRequestException(transformerName, e);
        }
    }

    private void addErrorHeaders(RecordHeaders recordHeaders, Exception e) {
        recordHeaders.remove(headerPrefix + "error");
        recordHeaders.remove(headerPrefix + "error-class");
        recordHeaders.remove(headerPrefix + "error-message");
        recordHeaders.add(headerPrefix + "error", "true".getBytes(StandardCharsets.UTF_8));
        recordHeaders.add(headerPrefix + "error-class", e.getClass().getName().getBytes(StandardCharsets.UTF_8));
        if (e.getMessage() != null) {
            recordHeaders.add(headerPrefix + "error-message", e.getMessage().getBytes(StandardCharsets.UTF_8));
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

        if(configured(recordHeaders, "enable", "false")) {
            Header[] headers = Arrays.stream(recordHeaders.toArray())
                .filter( header -> {
                    String key = header.key();
                    String value = Utils.utf8(header.value());

                    if(key.matches(headerPrefixPattern)) {
                        log.debug("{}: request header {}={} not added to request, matches headerPrefixPattern {}", transformerName, key, value, headerPrefixPattern);
                        return false;
                    }

                    log.debug("{}: request header {}={} added to request, doesn't match headerPrefixPattern {}", transformerName, key, value, headerPrefixPattern);
                    return true;
                } )
                .toArray(Header[]::new);
            return newRecord(recordBatch, record, headers, record.value());
        }

        Date inDate = new Date();

        AbstractHttpClient httpClient = HttpClients.getHttpClient(recordHeaders, this);
        AbstractHttpRequest httpRequest = httpClient.newHttpRequest(reqConfig(recordHeaders, "uri"));

        Map<String, List<String>> resHeadersMap = new HashMap<>();
        Map<String, List<String>> origHeadersMap = new HashMap<>();

        for(Header header : record.headers()) {
            String key = header.key();
            String value = Utils.utf8(header.value());

            if(key.matches(headerPrefixPattern)) {
                log.debug("{}: request header {}={} not added to request, matches headerPrefixPattern {}", transformerName, key, value, headerPrefixPattern);
                continue;
            }

            if(null == persistentHeadersPattern || key.matches(persistentHeadersPattern)) {
                try {
                    origHeadersMap.put(key, Arrays.asList(value));
                    httpRequest.header(key, value);
                    log.debug("{}: persistent header {}={} added to request, matches headers.persistentPattern {}", transformerName, key, value, persistentHeadersPattern);
                } catch(java.lang.IllegalArgumentException e) {
                    log.debug("{}: persistent header {}={} not added to request", transformerName, key, value, e);
                }
            } else {
                log.debug("{}: persistent header {}={} not added to request, doesn't match pattern headers.persistentPattern {}", transformerName, key, value, persistentHeadersPattern);
            }

            String transientHeadersPattern = reqConfig(recordHeaders, "headers.transientPattern");
            if(null != transientHeadersPattern && key.matches(transientHeadersPattern)) {
                resHeadersMap.put(key, Arrays.asList(value));
                log.debug("{}: transient header {}={} added to response, matches headers.transientPattern {}", transformerName, key, value, transientHeadersPattern);
            } else {
                log.debug("{}: transient header {}={} not added to response, doesn't match headers.transientPattern {}", transformerName, key, value, transientHeadersPattern);
            }
        }

        httpRequest.header(headerPrefix+"hostname", brokerHostname);
        httpRequest.header(headerPrefix+"topic-name", topicProduceData.name());
        httpRequest.header(headerPrefix+"partition-index", String.valueOf(partitionProduceData.index()));
        httpRequest.header(headerPrefix+"record-offset", String.valueOf(record.offset()));

        String httpHeadersString = reqConfig(recordHeaders, "headers.http");
        if(null != httpHeadersString) {
            String[] httpHeadersStrings = httpHeadersString.split("[,\\s]+");
            for(String httpHeaderString : httpHeadersStrings) {
                try {
                    String[] tokens = httpHeaderString.split("\\s*=\\s*");
                    httpRequest.header(tokens[0], tokens[1]);
                } catch(Exception e) {
                    log.warn("{}", httpHeaderString, e);
                }
            }
        }

        String recordKey = null;
        if(null != record.key()) {
            recordKey = Utils.utf8(record.key());
            httpRequest.header("kafka.KEY", recordKey);
        }
        httpRequest.body(recordKey, record.value());

        Date reqDate = new Date();
        httpRequest.header(headerPrefix+"req-time", ""+reqDate.getTime());

        byte[] body = new byte[0];

        if(configured(recordHeaders, "enable-send", "true", true)) {
            HttpResponse httpResponse = httpClient.send(httpRequest);
            log.debug("{}: httpResponse {}", transformerName, httpResponse);
            if(httpResponse.statusCode() != 200) {
                String headersString = httpResponse.headers().entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + String.join(", ", entry.getValue()))
                    .collect(Collectors.joining("\n"));

                log.warn("{}: httpResponse {}\n{}\n{}", transformerName, httpResponse, headersString, new String(httpResponse.body()));

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

/*
        if(configured(recordHeaders, "response-mode", "original", false)) {
            resHeadersMap.putAll(origHeadersMap);
            // body = record.value().toArray();
        }
*/

        Date resDate = new Date();
        long reqRunTime = resDate.getTime() - reqDate.getTime();

        // Broker headers should never be returned by the called service.
        resHeadersMap.entrySet().removeIf(entry -> {
            boolean shouldRemove = entry.getKey().matches(headerPrefixPattern);
            if(shouldRemove) {
                log.debug("{}: response header {} not added, matches headerPrefixPattern {}", transformerName, entry.getKey(), headerPrefixPattern);
            }
            return shouldRemove;
        });

        if(configured("headers.res", "hostname", false)) {
            resHeadersMap.put(headerPrefix+"hostname", Arrays.asList(brokerHostname));
        }

        if(null != envHeadersPattern && configured(recordHeaders, "headers.res", "env", false)) {
            System.getenv().entrySet().stream()
                .filter( entry -> {
                    if(entry.getKey().matches(envHeadersPattern)) {
                        log.debug("{}: env header {} added, matches headers.envPattern {}", transformerName, entry.getKey(), envHeadersPattern);
                        return true;
                    }
                    return false;
                } )
                .forEach( entry -> resHeadersMap.put(headerPrefix+"env-"+entry.getKey().replaceAll("_","-"), Arrays.asList(entry.getValue())) );
        }

        Date outDate = new Date();
        long runTime = outDate.getTime() - inDate.getTime();

        if(configured("headers.res", "time", false)) {
            resHeadersMap.put(headerPrefix+"in-time", Arrays.asList(""+inDate.getTime()));
            resHeadersMap.put(headerPrefix+"req-time", Arrays.asList(""+reqDate.getTime()));
            resHeadersMap.put(headerPrefix+"res-time", Arrays.asList(""+resDate.getTime()));
            resHeadersMap.put(headerPrefix+"out-time", Arrays.asList(""+outDate.getTime()));
        }

        if(configured("headers.res", "timespan", false)) {
            resHeadersMap.put(headerPrefix+"req-timespan", Arrays.asList(""+reqRunTime));
            resHeadersMap.put(headerPrefix+"run-timespan", Arrays.asList(""+runTime));
        }

        Header[] headers = headers(resHeadersMap);

        log.trace("{}: res body {}", transformerName, body.length, body);
        log.trace("{}: res body String {}", transformerName, body.length, new String(body, StandardCharsets.UTF_8) );

        return newRecord(recordBatch, record, headers, body);
    }
}

