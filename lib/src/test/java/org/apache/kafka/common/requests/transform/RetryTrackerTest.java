package org.apache.kafka.common.requests.transform;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RetryTrackerTest {
    private RetryTracker tracker;

    @Before
    public void setUp() {
        // Short TTL for testing, large max entries
        tracker = new RetryTracker("test", 1000, 1000, 100);
    }

    @After
    public void tearDown() {
        tracker.shutdown();
    }

    @Test
    public void firstFailureReturnsOne() {
        assertEquals(1, tracker.recordFailure("txn-1"));
        assertEquals(1, tracker.getAttempts("txn-1"));
    }

    @Test
    public void subsequentFailuresIncrement() {
        assertEquals(1, tracker.recordFailure("txn-1"));
        assertEquals(2, tracker.recordFailure("txn-1"));
        assertEquals(3, tracker.recordFailure("txn-1"));
        assertEquals(3, tracker.getAttempts("txn-1"));
    }

    @Test
    public void clearRemovesEntry() {
        tracker.recordFailure("txn-1");
        tracker.recordFailure("txn-1");
        tracker.clear("txn-1");
        assertEquals(0, tracker.getAttempts("txn-1"));
        // Next failure starts fresh
        assertEquals(1, tracker.recordFailure("txn-1"));
    }

    @Test
    public void differentTxnIdsAreIndependent() {
        assertEquals(1, tracker.recordFailure("txn-1"));
        assertEquals(2, tracker.recordFailure("txn-1"));
        assertEquals(1, tracker.recordFailure("txn-2"));
        assertEquals(3, tracker.recordFailure("txn-1"));
        assertEquals(2, tracker.recordFailure("txn-2"));
    }

    @Test
    public void expiredEntriesResetCount() throws InterruptedException {
        // Use a tracker with very short TTL
        RetryTracker shortTtl = new RetryTracker("test-short", 50, 1000, 10);
        try {
            assertEquals(1, shortTtl.recordFailure("txn-1"));
            assertEquals(2, shortTtl.recordFailure("txn-1"));

            // Wait for expiry
            Thread.sleep(100);

            // Should start fresh after expiry
            assertEquals(1, shortTtl.recordFailure("txn-1"));
        } finally {
            shortTtl.shutdown();
        }
    }

    @Test
    public void evictsOldestWhenOverCapacity() {
        RetryTracker small = new RetryTracker("test-small", 60000, 3, 10000);
        try {
            small.recordFailure("txn-1");
            small.recordFailure("txn-2");
            small.recordFailure("txn-3");
            assertEquals(3, small.size());

            // Adding a 4th should evict the oldest
            small.recordFailure("txn-4");
            // Size might be 3 or 4 depending on timing, but txn-1 should be gone
            assertEquals(0, small.getAttempts("txn-1"));
        } finally {
            small.shutdown();
        }
    }
}
