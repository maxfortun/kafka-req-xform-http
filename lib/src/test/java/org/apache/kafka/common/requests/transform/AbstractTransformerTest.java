package org.apache.kafka.common.requests.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

public class AbstractTransformerTest {
    private static final String BUNDLE = "transformer-bundle-test";

    @Rule
    public TransformerConfig config = new TransformerConfig("transformer");

    @After
    public void clearBundleOverrides() {
        System.clearProperty(BUNDLE + "-overridden");
    }

    private AbstractTransformer transformer(String name) {
        return new AbstractTransformer(name) {
        };
    }

    private AbstractTransformer transformer() {
        return transformer(config.name());
    }

    private static RecordHeaders headers(String... keyValues) {
        RecordHeaders headers = new RecordHeaders();
        for (int i = 0; i < keyValues.length; i += 2) {
            headers.add(keyValues[i], ProduceRequests.bytes(keyValues[i + 1]));
        }
        return headers;
    }

    @Test
    public void headerPrefixesDefaultToTransformerName() {
        assertEquals(List.of("x-broker-"), AbstractTransformer.parseHeaderPrefixes(null, "x-broker-"));
        assertEquals(List.of("x-broker-"), AbstractTransformer.parseHeaderPrefixes(" , ,", "x-broker-"));
    }

    @Test
    public void headerPrefixesAreTrimmedAndOrdered() {
        assertEquals(Arrays.asList("cl-brk-", "content-lake-broker-"),
            AbstractTransformer.parseHeaderPrefixes(" cl-brk- , content-lake-broker- ,", "x-"));
    }

    @Test
    public void firstPrefixIsUsedForWrittenHeaders() {
        config.set("headers.prefix", "cl-brk-,content-lake-broker-");
        AbstractTransformer transformer = transformer();

        assertEquals("cl-brk-", transformer.headerPrefix);
        assertEquals(Arrays.asList("cl-brk-", "content-lake-broker-"), transformer.headerPrefixes);
    }

    @Test
    public void headerPrefixPatternMatchesAnyPrefixIgnoringCase() {
        config.set("headers.prefix", "cl-brk-,content-lake-broker-");
        String pattern = transformer().headerPrefixPattern;

        assertTrue("cl-brk-error".matches(pattern));
        assertTrue("CL-BRK-Error".matches(pattern));
        assertTrue("content-lake-broker-topic-name".matches(pattern));
        assertFalse("cl-api-txn-id".matches(pattern));
        assertFalse("x-cl-brk-error".matches(pattern));
    }

    @Test
    public void headerPrefixIsQuotedNotTreatedAsRegex() {
        config.set("headers.prefix", "a.b-");
        String pattern = transformer().headerPrefixPattern;

        assertTrue("a.b-x".matches(pattern));
        assertFalse("aXb-x".matches(pattern));
    }

    @Test
    public void defaultHeaderPrefixIsTransformerNameBroker() {
        AbstractTransformer transformer = transformer();
        assertEquals(config.name() + "-broker-", transformer.headerPrefix);
    }

    @Test
    public void appConfigReadsSystemProperty() {
        config.set("some.key", "from-property");
        assertEquals("from-property", transformer().appConfig("some.key"));
    }

    @Test
    public void appConfigReturnsNullOrDefaultWhenMissing() {
        AbstractTransformer transformer = transformer();
        assertNull(transformer.appConfig("missing"));
        assertEquals("fallback", transformer.appConfig("missing", "fallback"));
    }

    @Test
    public void appConfigReadsResourceBundle() {
        assertEquals("from-bundle", transformer(BUNDLE).appConfig("bundled"));
    }

    @Test
    public void systemPropertyOverridesResourceBundle() {
        System.setProperty(BUNDLE + "-overridden", "from-property");
        assertEquals("from-property", transformer(BUNDLE).appConfig("overridden"));
    }

    @Test
    public void reqConfigIgnoresHeaderUnlessRequestScoped() {
        config.set("onException", "error");
        AbstractTransformer transformer = transformer();
        RecordHeaders headers = headers(transformer.headerPrefix + "onException", "dlq");

        assertEquals("error", transformer.reqConfig(headers, "onException"));
    }

    @Test
    public void reqConfigUsesHeaderWhenRequestScoped() {
        config.set("onException", "error").set("onException.scopes", "(?i)^(app|request)$");
        AbstractTransformer transformer = transformer();
        RecordHeaders headers = headers(transformer.headerPrefix + "onException", "dlq");

        assertEquals("dlq", transformer.reqConfig(headers, "onException"));
    }

    @Test
    public void reqConfigFallsBackToAppConfigWhenHeaderMissing() {
        config.set("onException", "error").set("onException.scopes", "(?i)^(app|request)$");
        assertEquals("error", transformer().reqConfig(headers(), "onException"));
    }

    @Test
    public void reqConfigHeaderNameReplacesDotsWithDashes() {
        config.set("onException.dlqTopic.scopes", "request");
        AbstractTransformer transformer = transformer();
        RecordHeaders headers = headers(transformer.headerPrefix + "onException-dlqTopic", "custom-dlq");

        assertEquals("custom-dlq", transformer.reqConfig(headers, "onException.dlqTopic"));
    }

    @Test
    public void reqConfigChecksPrefixesInOrder() {
        config.set("headers.prefix", "new-,old-").set("uri.scopes", "request");
        AbstractTransformer transformer = transformer();

        assertEquals("from-old", transformer.reqConfig(headers("old-uri", "from-old"), "uri"));
        assertEquals("from-new", transformer.reqConfig(headers("old-uri", "from-old", "new-uri", "from-new"), "uri"));
    }

    @Test
    public void configuredMatchesValueAgainstConfiguredPattern() {
        config.set("headers.res", "(?i)^(hostname|time)$");
        AbstractTransformer transformer = transformer();

        assertTrue(transformer.configured("headers.res", "hostname", false));
        assertTrue(transformer.configured("headers.res", "TIME", false));
        assertFalse(transformer.configured("headers.res", "env", true));
    }

    @Test
    public void configuredUsesDefaultWhenUnset() {
        AbstractTransformer transformer = transformer();
        assertTrue(transformer.configured("unset", "anything", true));
        assertFalse(transformer.configured("unset", "anything", false));
    }

    @Test
    public void configuredWithoutDefaultRequiresTheSetting() {
        AbstractTransformer transformer = transformer();
        assertThrows(IllegalArgumentException.class, () -> transformer.configured("required", "x", (Boolean) null));
        assertThrows(IllegalArgumentException.class, () -> transformer.configured(headers(), "required", "x"));
    }

    @Test
    public void configuredWithHeadersUsesRequestScopedValue() {
        config.set("enable", "true").set("enable.scopes", "(?i)^(app|request)$");
        AbstractTransformer transformer = transformer();

        assertTrue(transformer.configured(headers(), "enable", "true"));
        assertFalse(transformer.configured(headers(transformer.headerPrefix + "enable", "false"), "enable", "true"));
    }

    @Test
    public void newRecordKeepsTimestampKeyAndOffset() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord(1234L, ProduceRequests.bytes("k0"), ProduceRequests.bytes("v0")),
            new SimpleRecord(5678L, ProduceRequests.bytes("k1"), ProduceRequests.bytes("v1")));
        RecordBatch batch = records.batches().iterator().next();
        AbstractTransformer transformer = transformer();

        for (Record record : batch) {
            Record copy = transformer.newRecord(batch, record, record.headers(), record.value());
            assertEquals(record.timestamp(), copy.timestamp());
            assertEquals(record.offset(), copy.offset());
            assertEquals(ProduceRequests.key(record), ProduceRequests.key(copy));
            assertEquals(ProduceRequests.value(record), ProduceRequests.value(copy));
        }
    }

    @Test
    public void newRecordKeepsLogAppendTime() throws Exception {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), RecordBatch.CURRENT_MAGIC_VALUE,
            CompressionType.NONE, TimestampType.LOG_APPEND_TIME, 0L, 9999L);
        builder.append(1234L, ProduceRequests.bytes("k0"), ProduceRequests.bytes("v0"));
        RecordBatch batch = builder.build().batches().iterator().next();
        Record record = batch.iterator().next();

        Record copy = transformer().newRecord(batch, record, record.headers(), record.value());

        assertEquals(9999L, record.timestamp());
        assertEquals(record.timestamp(), copy.timestamp());
    }

    @Test
    public void newRecordReplacesHeadersAndValue() throws Exception {
        MemoryRecords records = ProduceRequests.plain(ProduceRequests.record("k0", "v0", "old", "1"));
        RecordBatch batch = records.batches().iterator().next();
        Record record = batch.iterator().next();

        Record copy = transformer().newRecord(batch, record, headers("new", "2").toArray(), ProduceRequests.bytes("v1"));

        assertNull(ProduceRequests.header(copy, "old"));
        assertEquals("2", ProduceRequests.header(copy, "new"));
        assertEquals("v1", ProduceRequests.value(copy));
        assertEquals("k0", ProduceRequests.key(copy));
    }

    @Test
    public void setHeaderReplacesExistingValues() {
        RecordHeaders headers = headers("k", "1", "k", "2");
        transformer().setHeader(headers, "k", "3");

        assertEquals(1, headers.toArray().length);
        assertEquals("3", Utils.utf8(headers.lastHeader("k").value()));
    }
}
