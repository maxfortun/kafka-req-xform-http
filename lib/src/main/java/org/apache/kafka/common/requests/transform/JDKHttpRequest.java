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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDKHttpRequest extends AbstractHttpRequest {
    public static final Logger log = LoggerFactory.getLogger(JDKHttpRequest.class);

	private java.net.http.HttpRequest.Builder httpRequestBuilder = java.net.http.HttpRequest.newBuilder();
	private java.net.http.HttpRequest httpRequest = null;

    public JDKHttpRequest(String uri) throws Exception {
		super(uri);
		httpRequestBuilder.uri(new URI(uri));
	}

    public AbstractHttpRequest header(String key, String value) {
		httpRequestBuilder.header(key, value);
		return this;
	}

    public AbstractHttpRequest body(String key, ByteBuffer byteBuffer) {
        int position = byteBuffer.position();
        int arrayOffset = byteBuffer.arrayOffset();

        byte[] array = new byte[byteBuffer.remaining()];
        byteBuffer.get(array, 0, array.length);

        java.net.http.HttpRequest.BodyPublisher bodyPublisher = java.net.http.HttpRequest.BodyPublishers.ofByteArray(array);
        httpRequestBuilder.POST(bodyPublisher);

		return this;
	}

	public java.net.http.HttpRequest httpRequest() {
		return httpRequest;
	}
}

