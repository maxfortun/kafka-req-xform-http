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
package org.apache.kafka.common.requests;

import java.util.ResourceBundle;

import java.net.URI;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.message.ProduceRequestData;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPProduceRequestParser extends ProduceRequestParser {
	public static final Logger log = LoggerFactory.getLogger(HTTPProduceRequestParser.class);
	private ResourceBundle resources = ResourceBundle.getBundle("HTTPProduceRequestParser");

	private HttpClient httpClient = HttpClient.newHttpClient();
	private URI uri = URI.create(resources.getString("uri"));

	public HTTPProduceRequestParser() {
	}

    public ProduceRequest parse(ByteBuffer buffer, short version) {
		try {
			HttpRequest httpRequest = HttpRequest.newBuilder()
				.uri(uri)
				.POST(HttpRequest.BodyPublishers.noBody())
				.build();
			HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        	return new ProduceRequest(new ProduceRequestData(new ByteBufferAccessor(buffer), version), version);
		} catch(Exception e) {
			String message = "Failed to parse request "+StandardCharsets.UTF_8.decode(buffer).toString();
			throw new InvalidRequestException(message, e);
		}
    }
}
