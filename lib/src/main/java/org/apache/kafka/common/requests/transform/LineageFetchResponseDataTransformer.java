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

import java.util.Date;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageFetchResponseDataTransformer extends AbstractFetchResponseDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(LineageFetchResponseDataTransformer.class);

    private String lineagePrefix;

    public LineageFetchResponseDataTransformer(String transformerName) {
        super(transformerName);
        lineagePrefix = appConfig("prefix", "/");
    }

    protected Record transform(
        FetchResponseData.FetchableTopicResponse fetchableTopicResponse,
        FetchResponseData.PartitionData partitionData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    ) {
        try {
            return transformImpl(
                fetchableTopicResponse,
                partitionData,
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
        FetchResponseData.FetchableTopicResponse fetchableTopicResponse,
        FetchResponseData.PartitionData partitionData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    ) throws Exception {

        if(!configured(recordHeaders, "enable", "true", true)) {
            return record;
        }

        Date inDate = new Date();

        String key = reqConfig(recordHeaders, "key");
        if(null == key || key.isBlank()) {
            key = transformerName;
        }

        String lineage = getLineage(fetchableTopicResponse, recordHeaders, key, inDate);
        setHeader(recordHeaders, key, lineage);

        Date outDate = new Date();
        long runTime = outDate.getTime() - inDate.getTime();

        if(configured("in-headers", "time", false)) {
            setHeader(recordHeaders, headerPrefix+"in-time", ""+inDate.getTime());
            setHeader(recordHeaders, headerPrefix+"out-time", ""+outDate.getTime());
        }

        if(configured("in-headers", "timespan", false)) {
            setHeader(recordHeaders, headerPrefix+"run-timespan", ""+runTime);
        }

        log.debug("{}: Updated lineage: {}", transformerName, lineage);
        return newRecord(recordBatch, record, recordHeaders.toArray(), record.value());
    }

    private String getLineage(FetchResponseData.FetchableTopicResponse fetchableTopicResponse, RecordHeaders recordHeaders, String key, Date inDate) {
        String lineage = getCurrentLineage(recordHeaders, key)+lineagePrefix+fetchableTopicResponse.topic();
        if(configured(recordHeaders, "in-time", "true", false)) {
            lineage += ":"+inDate.getTime();
        }
        return lineage;
    }

    private String getCurrentLineage(RecordHeaders recordHeaders, String key) {
        Header header = null;

        String fromKeys = reqConfig(recordHeaders, "from-keys");
        if(null != fromKeys && !fromKeys.isEmpty()) {
            for(String fromKey : fromKeys.split("[\\s,]+")) {
                header = recordHeaders.lastHeader(fromKey);
                if(null != header) {
                    break;
                }
            }
        }

        if(null == header) {
            header = recordHeaders.lastHeader(key);
        }

        if(null == header) {
            return "";
        }

        return Utils.utf8(header.value());
    }
}

