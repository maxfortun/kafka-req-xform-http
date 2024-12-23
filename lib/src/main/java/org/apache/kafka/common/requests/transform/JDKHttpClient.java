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

import java.net.URI;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDKHttpClient extends AbstractHttpClient {
    private static final Logger log = LoggerFactory.getLogger(JDKHttpClient.class);

    private final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
        .version(java.net.http.HttpClient.Version.HTTP_2)
        .build();

    private final Duration requestTimeout;

    public JDKHttpClient(HttpProduceRequestDataTransformer httpProduceRequestDataTransformer) {
        super(httpProduceRequestDataTransformer);

        String requestTimeoutString = httpProduceRequestDataTransformer.appConfig("httpClient.socketTimeout");
        if(null != requestTimeoutString) {
            requestTimeout = Duration.parse(requestTimeoutString);
        } else {
            requestTimeout = null;
        }
    }

    public AbstractHttpRequest newHttpRequest(String uri) throws Exception {
        return new JDKHttpRequest(this, uri);
    }

    public HttpResponse send(AbstractHttpRequest httpRequest) throws Exception {
        java.net.http.HttpResponse<byte[]> httpResponse = httpClient.send(((JDKHttpRequest)httpRequest).httpRequest(), java.net.http.HttpResponse.BodyHandlers.ofByteArray());
        return new JDKHttpResponse((JDKHttpRequest)httpRequest, httpResponse);
    }

}

