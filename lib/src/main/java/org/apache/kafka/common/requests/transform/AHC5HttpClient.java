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

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;

import org.apache.hc.core5.http.io.entity.ByteBufferEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.Timeout;
import org.apache.hc.core5.util.TimeValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AHC5HttpClient extends HttpClient {
    public static final Logger log = LoggerFactory.getLogger(AHC5HttpClient.class);

    private final CloseableHttpClient httpClient;

    public AHC5HttpClient(HttpProduceRequestDataTransformer httpProduceRequestDataTransformer) {
        super(httpProduceRequestDataTransformer);

        PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
            .setDefaultSocketConfig(
                SocketConfig.custom()
                .setSoTimeout(appTimeout("soTimeout"))
                .build()
            )
            .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
            .setConnPoolPolicy(PoolReusePolicy.LIFO)
            .setDefaultConnectionConfig(
                ConnectionConfig.custom()
                .setSocketTimeout(appTimeout("socketTimeout"))
                .setConnectTimeout(appTimeout("connectTimeout"))
                .setTimeToLive(TimeValue.ofMinutes(10))
                .build()
            )
            .build();


        httpClient = HttpClients.custom()
            .setConnectionManager(connectionManager)
            .build();

    }

	private Timeout appTimeout(String key) {
        String string = httpProduceRequestDataTransformer.appConfig("httpClient."+key);
        if(null == string) {
            return Timeout.INFINITE;
        }
        Timeout timeout = Timeout.ofSeconds(Long.parseLong(string));
		log.debug("{}: {}={}", httpProduceRequestDataTransformer.transformerName, key, timeout);
		return timeout;
	}

    public AbstractHttpRequest newHttpRequest(String uri) throws Exception {
		return new AHC5HttpRequest(this, uri);
	}

    public HttpResponse send(AbstractHttpRequest httpRequest) throws Exception {
		ClassicHttpResponse httpResponse = httpClient.execute(((AHC5HttpRequest)httpRequest).httpRequest(), response -> { 
			HttpEntity httpEntity = response.getEntity();
			EntityUtils.consume(httpEntity);
			return response;
		});

		return new AHC5HttpResponse((AHC5HttpRequest)httpRequest, httpResponse);
	}

}

