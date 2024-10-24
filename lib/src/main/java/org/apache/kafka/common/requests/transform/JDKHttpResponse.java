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

import java.net.URI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDKHttpResponse implements HttpResponse {
    public static final Logger log = LoggerFactory.getLogger(JDKHttpResponse.class);

    private JDKHttpRequest httpRequest;
    private java.net.http.HttpResponse<byte[]> httpResponse;

    public JDKHttpResponse(JDKHttpRequest httpRequest, java.net.http.HttpResponse<byte[]> httpResponse) {
        this.httpRequest = httpRequest;
        this.httpResponse = httpResponse;
    }

    public AbstractHttpRequest request() {
        return httpRequest;
    }

    public int statusCode() {
        return httpResponse.statusCode();
    }

    public Map<String, List<String>> headers() {
        Map<String, List<String>> headersMap = new HashMap<>();
        headersMap.putAll(httpResponse.headers().map());
        return headersMap;
    }

    public byte[] body() {
        return httpResponse.body();
    }
}

