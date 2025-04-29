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

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.time.Duration;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageProduceRequestDataTransformer extends AbstractProduceRequestDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(LineageProduceRequestDataTransformer.class);

    private static final String brokerHostname = System.getenv("HOSTNAME");

	private ExecutorService executor = Executors.newSingleThreadExecutor();

    private String lineagePrefix;
    private String lineageTopicName;
    private Map<String, String> lineageMap;

    public LineageProduceRequestDataTransformer(String transformerName) {
        super(transformerName);

        lineagePrefix = appConfig("prefix", "/");
        lineageTopicName = appConfig("topic-name", "__lineage");

		executor.submit(new ConsumerWorker());
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

        if(!configured(recordHeaders, "enable", "true", true)) {
            return record;
        }

        Date inDate = new Date();

        String key = reqConfig(recordHeaders, "key");
        if(null == key) {
            key = transformerName;
        }

        String lineage = getLineage(topicProduceData, recordHeaders, key, inDate);
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

    private String getLineage(ProduceRequestData.TopicProduceData topicProduceData, RecordHeaders recordHeaders, String key, Date inDate) {
        String lineage = getCurrentLineage(recordHeaders, key)+lineagePrefix+topicProduceData.name();
        if(configured(recordHeaders, "in-time", "true", false)) {
            lineage += ":"+inDate.getTime();
        }
        return lineage;
    }

    private String getCurrentLineage(RecordHeaders recordHeaders, String key) {
        Header header = null;

        String fromKeys = reqConfig(recordHeaders, "from-keys");
        if(null != fromKeys) {
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

    private void updateLineageMap(ConsumerRecord record) {
        log.debug("{}", record);
    }

    private class ConsumerWorker implements Runnable {
        private final KafkaConsumer<String, String> consumer;

        private ConsumerWorker() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9093");
            props.put("group.id", brokerHostname);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.offset.reset", "earliest");

            consumer = new KafkaConsumer<>(props);
    
            consumer.subscribe(Collections.singletonList(lineageTopicName));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        updateLineageMap(record);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }
    }
}
