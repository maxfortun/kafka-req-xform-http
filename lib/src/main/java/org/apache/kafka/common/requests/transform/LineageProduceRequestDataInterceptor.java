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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

/**
 * Adds the lineage header, delegating to LineageProduceRequestDataTransformer so both share one implementation
 * and the same configuration (lineage.properties).
 */
public class LineageProduceRequestDataInterceptor extends AbstractProduceRequestDataInterceptor {
    private final LineageProduceRequestDataTransformer lineage;

    public LineageProduceRequestDataInterceptor(String transformerName) {
        super(transformerName);
        lineage = new LineageProduceRequestDataTransformer(transformerName);
    }

    @Override
    protected Record intercept(
        ProduceRequestData.TopicProduceData topicProduceData,
        ProduceRequestData.PartitionProduceData partitionProduceData,
        RecordBatch recordBatch,
        Record record,
        RecordHeaders recordHeaders,
        short version
    ) {
        return lineage.transform(topicProduceData, partitionProduceData, recordBatch, record, recordHeaders, version);
    }

    // Runs first in the chain; content-lake stashes the final records.
    @Override
    protected boolean shouldStash() {
        return false;
    }
}
