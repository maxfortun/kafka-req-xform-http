package org.apache.kafka.common.requests.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.junit.Before;
import org.junit.Test;

// Chain comes from src/test/resources/TransformingProduceResponseInterceptor.properties: recorder-first, thrower, recorder-second.
public class TransformingProduceResponseInterceptorTest {
    static final List<String> calls = new CopyOnWriteArrayList<>();
    static volatile ProduceRequestData lastData;
    static volatile Map<TopicPartition, PartitionResponse> lastResponses;

    public static class Recorder implements ProduceResponseDataInterceptor {
        private final String name;

        public Recorder(String name) {
            this.name = name;
        }

        @Override
        public void onAppend(ProduceRequestData requestData, Map<TopicPartition, PartitionResponse> responses, short version) {
            calls.add(name);
            lastData = requestData;
            lastResponses = responses;
        }
    }

    public static class Thrower implements ProduceResponseDataInterceptor {
        public Thrower(String name) {
        }

        @Override
        public void onAppend(ProduceRequestData requestData, Map<TopicPartition, PartitionResponse> responses, short version) {
            calls.add("thrower");
            throw new IllegalStateException("lake down");
        }
    }

    @Before
    public void reset() {
        calls.clear();
    }

    @Test
    public void callsEveryInterceptorInOrderEvenWhenOneThrows() {
        ProduceRequestData data = new ProduceRequestData();
        Map<TopicPartition, PartitionResponse> responses = Map.of(new TopicPartition("t", 0), new PartitionResponse(Errors.NONE, 5L, -1L, 0L));

        new TransformingProduceResponseInterceptor().onAppend(data, responses, (short) 9);

        assertEquals(List.of("recorder-first", "thrower", "recorder-second"), calls);
        assertSame(data, lastData);
        assertSame(responses, lastResponses);
    }
}
