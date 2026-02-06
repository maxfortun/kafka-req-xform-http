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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.OffsetCommitRequestData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpOffsetCommitRequestDataTransformer extends AbstractOffsetCommitRequestDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(HttpOffsetCommitRequestDataTransformer.class);

    private static final String brokerHostname = System.getenv("HOSTNAME");

    private final String headerPrefixPattern;

    public HttpOffsetCommitRequestDataTransformer(String transformerName) throws Exception {
        super(transformerName);
        headerPrefixPattern = "(?i)^" + headerPrefix + ".*$";
    }

    @Override
    protected void transform(
        OffsetCommitRequestData request,
        OffsetCommitRequestData.OffsetCommitRequestTopic topic,
        OffsetCommitRequestData.OffsetCommitRequestPartition partition,
        short version
    ) {
        try {
            transformImpl(request, topic, partition, version);
        } catch (Exception e) {
            log.warn("{}: transform failed for group={} topic={} partition={}",
                transformerName, request.groupId(), topic.name(), partition.partitionIndex(), e);
            handleException(e);
        }
    }

    private void handleException(Exception e) {
        String onException = appConfig("onException");
        if ("ignore".equalsIgnoreCase(onException)) {
            log.debug("{}: onException=ignore, continuing without modification", transformerName);
            return;
        }
        throw new InvalidRequestException(transformerName, e);
    }

    private void transformImpl(
        OffsetCommitRequestData request,
        OffsetCommitRequestData.OffsetCommitRequestTopic topic,
        OffsetCommitRequestData.OffsetCommitRequestPartition partition,
        short version
    ) throws Exception {
        RecordHeaders recordHeaders = new RecordHeaders();

        if (!configured(recordHeaders, "enable", "true", true)) {
            log.debug("{}: transformation disabled", transformerName);
            return;
        }

        Date inDate = new Date();

        AbstractHttpClient httpClient = HttpClients.getHttpClient(recordHeaders, this);
        AbstractHttpRequest httpRequest = httpClient.newHttpRequest(appConfig("uri"));

        // Add context headers
        httpRequest.header(headerPrefix + "hostname", brokerHostname);
        httpRequest.header(headerPrefix + "api-type", "OffsetCommit");
        httpRequest.header(headerPrefix + "group-id", request.groupId());
        httpRequest.header(headerPrefix + "member-id", request.memberId());
        httpRequest.header(headerPrefix + "generation-id", String.valueOf(request.generationIdOrMemberEpoch()));
        if (request.groupInstanceId() != null) {
            httpRequest.header(headerPrefix + "group-instance-id", request.groupInstanceId());
        }
        httpRequest.header(headerPrefix + "topic-name", topic.name());
        httpRequest.header(headerPrefix + "partition-index", String.valueOf(partition.partitionIndex()));
        httpRequest.header(headerPrefix + "committed-offset", String.valueOf(partition.committedOffset()));
        httpRequest.header(headerPrefix + "committed-leader-epoch", String.valueOf(partition.committedLeaderEpoch()));
        if (partition.committedMetadata() != null) {
            httpRequest.header(headerPrefix + "committed-metadata", partition.committedMetadata());
        }

        // Add custom HTTP headers from config
        String httpHeadersString = appConfig("headers.http");
        if (null != httpHeadersString) {
            String[] httpHeadersStrings = httpHeadersString.split("[,\\s]+");
            for (String httpHeaderString : httpHeadersStrings) {
                try {
                    String[] tokens = httpHeaderString.split("\\s*=\\s*");
                    httpRequest.header(tokens[0], tokens[1]);
                } catch (Exception e) {
                    log.warn("{}: failed to parse header: {}", transformerName, httpHeaderString, e);
                }
            }
        }

        // Build request body as JSON
        String body = buildRequestBody(request, topic, partition);
        httpRequest.body(null, ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)));

        Date reqDate = new Date();
        httpRequest.header(headerPrefix + "req-time", String.valueOf(reqDate.getTime()));

        if (configured(recordHeaders, "enable-send", "true", true)) {
            HttpResponse httpResponse = httpClient.send(httpRequest);
            log.debug("{}: httpResponse {}", transformerName, httpResponse);

            if (httpResponse.statusCode() != 200) {
                String headersString = httpResponse.headers().entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + String.join(", ", entry.getValue()))
                    .collect(Collectors.joining("\n"));

                log.warn("{}: httpResponse {}\n{}\n{}", transformerName, httpResponse, headersString,
                    new String(httpResponse.body()));

                String onHttpException = appConfig("httpClient.onException");
                if (!"pass-thru".equalsIgnoreCase(onHttpException) && !"ignore".equalsIgnoreCase(onHttpException)) {
                    throw new HttpResponseException(httpResponse);
                }
            }

            // Process response headers to potentially modify commit data
            processResponseHeaders(httpResponse.headers(), partition);
        }

        Date resDate = new Date();
        long reqRunTime = resDate.getTime() - reqDate.getTime();
        log.debug("{}: request completed in {}ms", transformerName, reqRunTime);
    }

    private String buildRequestBody(
        OffsetCommitRequestData request,
        OffsetCommitRequestData.OffsetCommitRequestTopic topic,
        OffsetCommitRequestData.OffsetCommitRequestPartition partition
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"groupId\":\"").append(escapeJson(request.groupId())).append("\",");
        sb.append("\"memberId\":\"").append(escapeJson(request.memberId())).append("\",");
        sb.append("\"generationId\":").append(request.generationIdOrMemberEpoch()).append(",");
        if (request.groupInstanceId() != null) {
            sb.append("\"groupInstanceId\":\"").append(escapeJson(request.groupInstanceId())).append("\",");
        }
        sb.append("\"topicName\":\"").append(escapeJson(topic.name())).append("\",");
        sb.append("\"partitionIndex\":").append(partition.partitionIndex()).append(",");
        sb.append("\"committedOffset\":").append(partition.committedOffset()).append(",");
        sb.append("\"committedLeaderEpoch\":").append(partition.committedLeaderEpoch());
        if (partition.committedMetadata() != null) {
            sb.append(",\"committedMetadata\":\"").append(escapeJson(partition.committedMetadata())).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String value) {
        if (value == null) return "";
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private void processResponseHeaders(Map<String, List<String>> headers, OffsetCommitRequestData.OffsetCommitRequestPartition partition) {
        // Allow HTTP response to modify the committed offset
        String newOffsetStr = getHeaderValue(headers, headerPrefix + "committed-offset");
        if (newOffsetStr != null) {
            try {
                long newOffset = Long.parseLong(newOffsetStr);
                log.debug("{}: modifying committed offset from {} to {}", transformerName, partition.committedOffset(), newOffset);
                partition.setCommittedOffset(newOffset);
            } catch (NumberFormatException e) {
                log.warn("{}: invalid committed-offset header value: {}", transformerName, newOffsetStr);
            }
        }

        // Allow HTTP response to modify metadata
        String newMetadata = getHeaderValue(headers, headerPrefix + "committed-metadata");
        if (newMetadata != null) {
            log.debug("{}: modifying metadata from {} to {}", transformerName, partition.committedMetadata(), newMetadata);
            partition.setCommittedMetadata(newMetadata);
        }

        // Allow HTTP response to modify leader epoch
        String newLeaderEpochStr = getHeaderValue(headers, headerPrefix + "committed-leader-epoch");
        if (newLeaderEpochStr != null) {
            try {
                int newLeaderEpoch = Integer.parseInt(newLeaderEpochStr);
                log.debug("{}: modifying committed leader epoch from {} to {}", transformerName, partition.committedLeaderEpoch(), newLeaderEpoch);
                partition.setCommittedLeaderEpoch(newLeaderEpoch);
            } catch (NumberFormatException e) {
                log.warn("{}: invalid committed-leader-epoch header value: {}", transformerName, newLeaderEpochStr);
            }
        }
    }

    private String getHeaderValue(Map<String, List<String>> headers, String key) {
        // Try exact match first
        List<String> values = headers.get(key);
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }

        // Try case-insensitive match
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key)) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    return entry.getValue().get(0);
                }
            }
        }

        return null;
    }
}
