package org.apache.kafka.common.requests.transform;

import static org.apache.kafka.common.requests.transform.ProduceRequests.header;
import static org.apache.kafka.common.requests.transform.ProduceRequests.plain;
import static org.apache.kafka.common.requests.transform.ProduceRequests.record;
import static org.apache.kafka.common.requests.transform.ProduceRequests.recordList;
import static org.apache.kafka.common.requests.transform.ProduceRequests.records;
import static org.apache.kafka.common.requests.transform.ProduceRequests.request;
import static org.apache.kafka.common.requests.transform.ProduceRequests.value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.InterceptResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class LineageProduceRequestDataInterceptorTest {
    private static final short VERSION = 9;

    @Rule
    public TransformerConfig config = new TransformerConfig("lineage");

    private String topic;

    @Before
    public void setUp() {
        topic = config.name() + "-t";
    }

    private InterceptResult send(MemoryRecords records) {
        return new LineageProduceRequestDataInterceptor(config.name()).intercept(request().add(topic, 0, records).build(), VERSION);
    }

    private Record only(InterceptResult result) {
        List<Record> out = recordList(records(result.data(), topic, 0));
        assertEquals(1, out.size());
        return out.get(0);
    }

    // With no key configured, the header is named after the interceptor.
    private String lineage(Record record) {
        return header(record, config.name());
    }

    @Test
    public void addsTopicWithDefaultPrefix() {
        Record out = only(send(plain(record("k0", "v0"))));

        assertEquals("/" + topic, lineage(out));
        assertEquals("v0", value(out));
    }

    @Test
    public void appendsToExistingLineage() {
        Record out = only(send(plain(record("k0", "v0", config.name(), "/upstream"))));

        assertEquals("/upstream/" + topic, lineage(out));
        assertEquals("header is replaced, not duplicated", 1, out.headers().length);
    }

    @Test
    public void keyAndPrefixAreConfigurable() {
        config.set("key", "lineage").set("prefix", "/prod-blue:");

        Record out = only(send(plain(record("k0", "v0", "lineage", "/prod-blue:a"))));

        assertEquals("/prod-blue:a/prod-blue:" + topic, header(out, "lineage"));
    }

    @Test
    public void fromKeysSeedsLineageFromFirstPresentHeader() {
        config.set("key", "lineage").set("from-keys", "missing, legacy-lineage");

        Record out = only(send(plain(record("k0", "v0", "legacy-lineage", "/old"))));

        assertEquals("/old/" + topic, header(out, "lineage"));
    }

    @Test
    public void inTimeAppendsEpochMillis() {
        config.set("in-time", "true");
        long before = System.currentTimeMillis();

        String lineage = lineage(only(send(plain(record("k0", "v0")))));

        assertTrue(lineage, lineage.matches("/" + java.util.regex.Pattern.quote(topic) + ":\\d+"));
        long stamped = Long.parseLong(lineage.substring(lineage.lastIndexOf(':') + 1));
        assertTrue(stamped >= before && stamped <= System.currentTimeMillis());
    }

    @Test
    public void inTimeFormatIsConfigurable() {
        config.set("in-time", "true").set("time-fmt", "yyyy");

        String lineage = lineage(only(send(plain(record("k0", "v0")))));

        assertTrue(lineage, lineage.matches(".*:\\d{4}"));
    }

    @Test
    public void timingHeadersWhenConfigured() {
        config.set("in-headers", "(?i)^(time|timespan)$");
        LineageProduceRequestDataInterceptor interceptor = new LineageProduceRequestDataInterceptor(config.name());

        Record out = recordList(records(interceptor.intercept(request().add(topic, 0, plain(record("k0", "v0"))).build(), VERSION).data(), topic, 0)).get(0);

        for (String name : List.of("in-time", "out-time", "run-timespan")) {
            assertTrue(name, null != header(out, interceptor.headerPrefix + name));
        }
        long runTime = Long.parseLong(header(out, interceptor.headerPrefix + "run-timespan"));
        assertTrue("run-timespan " + runTime + " is negative", runTime >= 0);
    }

    @Test
    public void disabledLeavesRecordsAlone() {
        config.set("enable", "false");

        assertNull(lineage(only(send(plain(record("k0", "v0"))))));
    }

    @Test
    public void appliesToEveryRecordAndPartition() {
        LineageProduceRequestDataInterceptor interceptor = new LineageProduceRequestDataInterceptor(config.name());

        InterceptResult result = interceptor.intercept(request()
            .add(topic, 0, plain(record("k0", "v0"), record("k1", "v1")))
            .add(topic, 1, plain(record("k2", "v2")))
            .build(), VERSION);

        assertFalse(result.hasErrors());
        for (int partition = 0; partition < 2; partition++) {
            for (Record record : recordList(records(result.data(), topic, partition))) {
                assertEquals("/" + topic, lineage(record));
            }
        }
    }

    @Test
    public void leavesStashingToTheLastInterceptor() {
        InterceptResult result = send(plain(record("k0", "v0")));

        assertFalse(InterceptorStash.hasStash(result.data()));
    }
}
