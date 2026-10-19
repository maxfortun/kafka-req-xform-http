package org.apache.kafka.common.requests.transform;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.junit.Test;

public class InterceptorStashTest {
    private static ProduceRequestData data(String topic) {
        return ProduceRequests.request().add(topic, 0, ProduceRequests.plain(ProduceRequests.record("k", "v"))).build();
    }

    private static InterceptorStash.StashedRequest stashed(String topic) {
        TopicPartition tp = new TopicPartition(topic, 0);
        return new InterceptorStash.StashedRequest(Map.of(tp, new InterceptorStash.StashedPartition(tp, List.of())));
    }

    @Test
    public void popReturnsStashedDataOnce() {
        ProduceRequestData data = data("stash-once");
        InterceptorStash.StashedRequest stash = stashed("stash-once");
        InterceptorStash.stash(data, stash);

        assertTrue(InterceptorStash.hasStash(data));
        assertSame(stash, InterceptorStash.pop(data));
        assertFalse(InterceptorStash.hasStash(data));
        assertNull(InterceptorStash.pop(data));
    }

    @Test
    public void unknownRequestHasNoStash() {
        assertNull(InterceptorStash.pop(data("stash-unknown")));
    }

    @Test
    public void requestsAreKeptApart() {
        ProduceRequestData a = data("stash-a");
        ProduceRequestData b = data("stash-b");
        InterceptorStash.StashedRequest stashA = stashed("stash-a");
        InterceptorStash.StashedRequest stashB = stashed("stash-b");
        InterceptorStash.stash(a, stashA);
        InterceptorStash.stash(b, stashB);

        assertSame(stashB, InterceptorStash.pop(b));
        assertSame(stashA, InterceptorStash.pop(a));
    }
}
