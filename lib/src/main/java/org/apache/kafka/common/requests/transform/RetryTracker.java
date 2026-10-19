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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks retry attempts by transaction ID. Thread-safe.
 * Entries expire after a TTL and are evicted when a max size is reached.
 */
public class RetryTracker {
    private static final Logger log = LoggerFactory.getLogger(RetryTracker.class);

    private static class Entry {
        final AtomicInteger attempts = new AtomicInteger(1);
        volatile long lastSeenMs;

        Entry() {
            this.lastSeenMs = System.currentTimeMillis();
        }

        int incrementAndGet() {
            lastSeenMs = System.currentTimeMillis();
            return attempts.incrementAndGet();
        }

        boolean isExpired(long ttlMs) {
            return System.currentTimeMillis() - lastSeenMs > ttlMs;
        }
    }

    private final String name;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final long ttlMs;
    private final int maxEntries;
    private final ScheduledExecutorService cleaner;

    /**
     * @param name       Identifier for logging
     * @param ttlMs      Entries older than this are expired (should match producer's delivery.timeout.ms)
     * @param maxEntries Max tracked IDs; oldest are evicted when exceeded
     * @param cleanupIntervalMs How often to run the cleanup task
     */
    public RetryTracker(String name, long ttlMs, int maxEntries, long cleanupIntervalMs) {
        this.name = name;
        this.ttlMs = ttlMs;
        this.maxEntries = maxEntries;

        this.cleaner = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, name + "-retry-tracker-cleaner");
            thread.setDaemon(true);
            return thread;
        });
        cleaner.scheduleWithFixedDelay(this::cleanup, cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);

        log.info("{}: retry tracker initialized, ttlMs={}, maxEntries={}", name, ttlMs, maxEntries);
    }

    /**
     * Records a failure for the given transaction ID.
     * @return The current attempt count (1 on first failure, incremented on subsequent)
     */
    public int recordFailure(String txnId) {
        Entry existing = entries.get(txnId);
        if (existing != null && !existing.isExpired(ttlMs)) {
            int attempts = existing.incrementAndGet();
            log.debug("{}: retry {} for txnId={}", name, attempts, txnId);
            return attempts;
        }

        // New entry or expired
        Entry newEntry = new Entry();
        Entry prev = entries.putIfAbsent(txnId, newEntry);
        if (prev != null && !prev.isExpired(ttlMs)) {
            // Another thread added it first
            int attempts = prev.incrementAndGet();
            log.debug("{}: retry {} for txnId={} (race)", name, attempts, txnId);
            return attempts;
        }

        // Replace expired entry
        if (prev != null) {
            entries.put(txnId, newEntry);
        }

        log.debug("{}: first failure for txnId={}", name, txnId);

        // Evict if over capacity
        if (entries.size() > maxEntries) {
            evictOldest();
        }

        return 1;
    }

    /**
     * Clears the entry for a transaction ID (call on success or after routing to DLQ).
     */
    public void clear(String txnId) {
        if (entries.remove(txnId) != null) {
            log.debug("{}: cleared txnId={}", name, txnId);
        }
    }

    /**
     * Returns the current attempt count for a transaction ID, or 0 if not tracked.
     */
    public int getAttempts(String txnId) {
        Entry entry = entries.get(txnId);
        if (entry != null && !entry.isExpired(ttlMs)) {
            return entry.attempts.get();
        }
        return 0;
    }

    /**
     * Current number of tracked transaction IDs.
     */
    public int size() {
        return entries.size();
    }

    private void cleanup() {
        int removed = 0;
        Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().isExpired(ttlMs)) {
                it.remove();
                removed++;
            }
        }
        if (removed > 0) {
            log.debug("{}: expired {} entries, {} remaining", name, removed, entries.size());
        }
    }

    private void evictOldest() {
        // Find and remove the oldest entry
        String oldestKey = null;
        long oldestTime = Long.MAX_VALUE;

        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            if (e.getValue().lastSeenMs < oldestTime) {
                oldestTime = e.getValue().lastSeenMs;
                oldestKey = e.getKey();
            }
        }

        if (oldestKey != null) {
            entries.remove(oldestKey);
            log.debug("{}: evicted oldest entry txnId={}", name, oldestKey);
        }
    }

    public void shutdown() {
        cleaner.shutdown();
    }
}
