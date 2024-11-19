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

import org.apache.hc.client5.http.classic.methods.HttpPost;

import org.apache.hc.core5.http.io.entity.ByteBufferEntity;
import org.apache.hc.core5.http.ContentType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AHC5HttpRequest extends AbstractHttpRequest {
    private static final Logger log = LoggerFactory.getLogger(AHC5HttpRequest.class);

    private AHC5HttpClient httpClient;
    private String headerPrefix;
    private HttpPost httpRequest;

    public AHC5HttpRequest(AHC5HttpClient httpClient, String uri) throws Exception {
        super(uri);
        this.httpClient = httpClient;
        headerPrefix = httpClient.httpProduceRequestDataTransformer.headerPrefix;
        httpRequest = new HttpPost(uri);
    }

    public AbstractHttpRequest header(String key, String value) {
        httpRequest.setHeader(key, value);
        return this;
    }

    public AbstractHttpRequest body(String key, ByteBuffer byteBuffer) {
        if(!org.apache.kafka.common.utils.Utils.isBlank(key)) {
            header(headerPrefix+"message-key", key);
        }

        httpRequest.setEntity(new ByteBufferEntity(byteBuffer, ContentType.DEFAULT_BINARY));
        return this;
    }

    public HttpPost httpRequest() {
        return httpRequest;
    }
}

