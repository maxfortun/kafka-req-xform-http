package org.apache.kafka.common.requests.transform;

import static org.apache.kafka.common.requests.transform.ProduceRequests.header;
import static org.apache.kafka.common.requests.transform.ProduceRequests.plain;
import static org.apache.kafka.common.requests.transform.ProduceRequests.record;
import static org.apache.kafka.common.requests.transform.ProduceRequests.recordList;
import static org.apache.kafka.common.requests.transform.ProduceRequests.records;
import static org.apache.kafka.common.requests.transform.ProduceRequests.request;
import static org.apache.kafka.common.requests.transform.ProduceRequests.value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

// The parser-path transformer, where failures can only be signalled by throwing.
public class HttpProduceRequestDataTransformerTest {
    private static final short VERSION = 9;

    @Rule
    public TransformerConfig config = new TransformerConfig("http-transformer");

    private StubHttpServer lake;
    private String topic;

    @Before
    public void startLake() throws Exception {
        lake = new StubHttpServer().respond(request -> new StubHttpServer.Response(500, "lake is down"));
        topic = config.name() + "-t";
    }

    @After
    public void stopLake() {
        lake.close();
    }

    private HttpProduceRequestDataTransformer transformer() throws Exception {
        config.setIfAbsent("enable", "true")
            .setIfAbsent("uri", lake.url("/lake"))
            .set("httpClient.class", AHC5HttpClient.class.getName())
            .set("httpClient.connectTimeout", "5")
            .set("httpClient.responseTimeout", "5");
        return new HttpProduceRequestDataTransformer(config.name());
    }

    private ProduceRequestData send(HttpProduceRequestDataTransformer transformer, MemoryRecords records) {
        return transformer.transform(request().add(topic, 0, records).build(), VERSION);
    }

    @Test
    public void lakeErrorThrowsByDefault() throws Exception {
        HttpProduceRequestDataTransformer transformer = transformer();
        assertThrows(InvalidRequestException.class, () -> send(transformer, plain(record("k0", "v0"))));
    }

    @Test
    public void lakeSuccessReplacesRecord() throws Exception {
        lake.respond(request -> new StubHttpServer.Response(200, "lake:" + request.bodyString()));
        ProduceRequestData out = send(transformer(), plain(record("k0", "v0")));

        assertEquals("lake:v0", value(recordList(records(out, topic, 0)).get(0)));
    }

    // "{topic-name}" used to go through String.replaceAll, which rejects it as a regex, so nothing ever reached the DLQ.
    @Test
    public void onExceptionDlqRoutesToDefaultDlqTopic() throws Exception {
        config.set("onException", "dlq");
        HttpProduceRequestDataTransformer transformer = transformer();

        ProduceRequestData out = send(transformer, plain(record("k0", "v0")));

        List<Record> dlq = recordList(records(out, "__" + topic + "-dlq", 0));
        assertEquals(1, dlq.size());
        assertEquals("v0", value(dlq.get(0)));
        assertEquals(topic, header(dlq.get(0), transformer.headerPrefix + "original-topic"));
        assertEquals(0, recordList(records(out, topic, 0)).size());
    }

    @Test
    public void onExceptionDlqTopicTemplateIsConfigurable() throws Exception {
        config.set("onException", "dlq").set("onException.dlqTopic", "dead-{topic-name}");

        ProduceRequestData out = send(transformer(), plain(record("k0", "v0")));

        assertEquals(1, recordList(records(out, "dead-" + topic, 0)).size());
    }

    @Test
    public void routesToDlqAfterMaxAttempts() throws Exception {
        config.set("retry.txnIdHeader", "cl-api-txn-id").set("retry.maxAttempts", "2");
        HttpProduceRequestDataTransformer transformer = transformer();
        MemoryRecords records = plain(record("k0", "v0", "cl-api-txn-id", "tx-1"));

        assertThrows(InvalidRequestException.class, () -> send(transformer, records));
        ProduceRequestData second = send(transformer, records);

        Record routed = recordList(records(second, "__" + topic + "-dlq", 0)).get(0);
        assertEquals("2", header(routed, transformer.headerPrefix + "retry-attempts"));
    }

    @Test
    public void onExceptionHeadersKeepsRecordWithErrorHeaders() throws Exception {
        config.set("onException", "headers");
        HttpProduceRequestDataTransformer transformer = transformer();

        Record out = recordList(records(send(transformer, plain(record("k0", "v0"))), topic, 0)).get(0);

        assertEquals("v0", value(out));
        assertEquals("true", header(out, transformer.headerPrefix + "error"));
    }
}
