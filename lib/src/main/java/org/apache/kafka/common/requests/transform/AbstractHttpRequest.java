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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractHttpRequest {
    private final String uri;

    // Kept so the request body can be logged on error; the underlying clients don't expose it after building.
    protected ByteBuffer body;

    public AbstractHttpRequest(String uri) throws Exception {
        this.uri = uri;
    }

    public String uri() {
        return uri;
    }

    @Override
    public String toString() {
        String bodyString = null == body ? "" : StandardCharsets.UTF_8.decode(body.duplicate()).toString();
        return "POST " + uri + "\n" + formatHeaders(headers()) + "\n\n" + bodyString;
    }

    static String formatHeaders(Map<String, List<String>> headers) {
        return headers.entrySet().stream()
            .map(entry -> entry.getKey() + ": " + String.join(", ", entry.getValue()))
            .collect(Collectors.joining("\n"));
    }

    public abstract Map<String, List<String>> headers();
    public abstract AbstractHttpRequest header(String key, String value);
    public abstract AbstractHttpRequest body(String key, ByteBuffer byteBuffer);
}
