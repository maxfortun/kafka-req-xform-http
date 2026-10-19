package org.apache.kafka.common.requests.transform;

import static org.apache.kafka.common.requests.transform.ProduceRequests.header;
import static org.apache.kafka.common.requests.transform.ProduceRequests.plain;
import static org.apache.kafka.common.requests.transform.ProduceRequests.record;
import static org.apache.kafka.common.requests.transform.ProduceRequests.recordList;
import static org.apache.kafka.common.requests.transform.ProduceRequests.records;
import static org.apache.kafka.common.requests.transform.ProduceRequests.request;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.InterceptResult;
import org.junit.Rule;
import org.junit.Test;

// Chain comes from src/test/resources/TransformingProduceRequestInterceptor.properties: chain-first, chain-second.
public class TransformingProduceRequestInterceptorTest {
    private static final short VERSION = 9;

    @Rule
    public TransformerConfig config = new TransformerConfig("chain");

    // Appends its name to the "order" header; fails partition <name>-fail.partition if configured.
    public static class Appender extends AbstractProduceRequestDataInterceptor {
        private final Integer failPartition;

        public Appender(String name) {
            super(name);
            String fail = appConfig("fail.partition");
            failPartition = null == fail ? null : Integer.valueOf(fail);
        }

        @Override
        protected Record intercept(ProduceRequestData.TopicProduceData topic, ProduceRequestData.PartitionProduceData partition,
                RecordBatch batch, Record record, RecordHeaders headers, short version) throws Exception {
            if (null != failPartition && failPartition == partition.index()) {
                throw new PartitionErrorException(Errors.NOT_ENOUGH_REPLICAS, transformerName + " failed");
            }
            Header order = headers.lastHeader("order");
            setHeader(headers, "order", null == order ? transformerName : Utils.utf8(order.value()) + "," + transformerName);
            return newRecord(batch, record, headers.toArray(), record.value());
        }
    }

    private String topic() {
        return config.name() + "-t";
    }

    @Test
    public void runsInterceptorsInConfiguredOrder() {
        InterceptResult result = new TransformingProduceRequestInterceptor()
            .intercept(request().add(topic(), 0, plain(record("k0", "v0"))).build(), VERSION);

        assertEquals("chain-first,chain-second", header(recordList(records(result.data(), topic(), 0)).get(0), "order"));
    }

    @Test
    public void collectsErrorsFromEveryInterceptor() {
        config.set("chain-first", "fail.partition", "0").set("chain-second", "fail.partition", "1");

        InterceptResult result = new TransformingProduceRequestInterceptor().intercept(request()
            .add(topic(), 0, plain(record("k0", "v0")))
            .add(topic(), 1, plain(record("k1", "v1")))
            .add(topic(), 2, plain(record("k2", "v2")))
            .build(), VERSION);

        assertEquals(Map.of(
            new TopicPartition(topic(), 0), Errors.NOT_ENOUGH_REPLICAS,
            new TopicPartition(topic(), 1), Errors.NOT_ENOUGH_REPLICAS), result.partitionErrors());
        assertEquals("chain-first,chain-second", header(recordList(records(result.data(), topic(), 2)).get(0), "order"));
    }

    // The broker passes the chain's final data to the response interceptor, so that is what the stash must be keyed by.
    @Test
    public void stashIsKeyedByFinalData() {
        InterceptResult result = new TransformingProduceRequestInterceptor()
            .intercept(request().add(topic(), 0, plain(record("k0", "v0"))).build(), VERSION);

        InterceptorStash.StashedRequest stash = InterceptorStash.pop(result.data());
        assertNotNull(stash);
        assertEquals("chain-first,chain-second", stash.partitions.get(new TopicPartition(topic(), 0)).records.get(0).headers.get("order"));
    }
}
