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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import java.time.Duration;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageProduceRequestDataTransformer extends AbstractProduceRequestDataTransformer {
    private static final Logger log = LoggerFactory.getLogger(LineageProduceRequestDataTransformer.class);

    private static final String brokerHostname = System.getenv("HOSTNAME");

    private String lineagePrefix;
    private String lineageSeparator;

    private String lineageMapBroker = null;
    private String lineageMapTopic = null;
    private Map<String, Set<String>> lineageMap = null;
	private KafkaProducer<String, String> kafkaProducer = null;

    public LineageProduceRequestDataTransformer(String transformerName) {
        super(transformerName);

        lineagePrefix = appConfig("prefix", "/");
        lineageSeparator = appConfig("separator", "/");

        if(configured("map", "true", false)) {
            lineageMapBroker = appConfig("map-broker", "localhost:9092");
            lineageMapTopic = appConfig("map-topic", "__lineage");
			lineageMap = new HashMap<>();

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(new LineageMapConsumer(getConsumerProps()));

            kafkaProducer = new KafkaProducer<>(getProducerProps());
        }
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

		updateLineageMap(lineage, true);

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

    private Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", lineageMapBroker);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private Properties getConsumerProps() {
        Properties props = getProducerProps();
        props.put("group.id", brokerHostname);
        props.put("auto.offset.reset", "earliest");
        return props;
    }


    private void updateLineageMap(String lineage, boolean shouldSync) {
        log.debug("{}", lineage);
		boolean isUpdated = false;

		String[] segments = lineage.split(lineageSeparator);

		Set<String> parentDescendents = null;
		for(String segment : segments) {
			String normalizedSegment = segment.replace(":[0-9]*$", "");
			Set<String> descendants = lineageMap.get(normalizedSegment);
			if(null == descendants) {
				descendants = new HashSet<String>();
				lineageMap.put(normalizedSegment, descendants);
				isUpdated = true;
			}

			if(null != parentDescendents) {
				if(!parentDescendents.contains(normalizedSegment)) {
					parentDescendents.add(normalizedSegment);
					isUpdated = true;
				}
			}

			parentDescendents = descendants;
			
		}

		if(!isUpdated || !shouldSync || null == kafkaProducer) {
			return;
		}

		JSONObject lineageJSONObject = new JSONObject(lineageMap);
        String lineageJSON = lineageJSONObject.toString();
	
		ProducerRecord<String, String> record = new ProducerRecord<>(lineageMapTopic, null, lineage);

		try {
			Future<RecordMetadata> future = kafkaProducer.send(record);
			RecordMetadata metadata = future.get();
		} catch(Exception e) {
        	log.warn("{}", lineage, e);
		}
    }

    private class LineageMapConsumer implements Runnable {
        private final KafkaConsumer<String, String> consumer;

        private LineageMapConsumer(Properties props) {
            consumer = new KafkaConsumer<>(props);
    
            consumer.subscribe(Collections.singletonList(lineageMapTopic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
						Headers headers = record.headers();
						Header lineageHeader = headers.lastHeader("lineage");
						if(null != lineageHeader) {
                        	updateLineageMap(Utils.utf8(lineageHeader.value()), false);
						}
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
