package org.apache.kafka.common.requests.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.rules.ExternalResource;

/**
 * Sets {@code <transformerName>-<key>} system properties, which AbstractTransformer.appConfig reads first,
 * and removes them after each test. Each test gets its own transformer name so state never leaks between tests.
 */
final class TransformerConfig extends ExternalResource {
    private static final AtomicInteger sequence = new AtomicInteger();

    private final List<String> keys = new ArrayList<>();
    private final String prefix;
    private String name;

    TransformerConfig(String prefix) {
        this.prefix = prefix;
    }

    @Override
    protected void before() {
        name = prefix + "-" + sequence.incrementAndGet();
    }

    @Override
    protected void after() {
        keys.forEach(System::clearProperty);
        keys.clear();
    }

    String name() {
        return name;
    }

    TransformerConfig set(String key, String value) {
        return set(name, key, value);
    }

    // Leaves a value a test already set.
    TransformerConfig setIfAbsent(String key, String value) {
        return null == System.getProperty(name + "-" + key) ? set(key, value) : this;
    }

    TransformerConfig set(String transformerName, String key, String value) {
        String property = transformerName + "-" + key;
        System.setProperty(property, value);
        keys.add(property);
        return this;
    }
}
