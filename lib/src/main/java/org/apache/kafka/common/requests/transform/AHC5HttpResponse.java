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

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AHC5HttpResponse implements HttpResponse {
    public static final Logger log = LoggerFactory.getLogger(AHC5HttpResponse.class);

	private final AHC5HttpRequest httpRequest;
	private final ClassicHttpResponse httpResponse;
	private final StatusLine statusLine;
	private final byte[] body;

	public AHC5HttpResponse(AHC5HttpRequest httpRequest, ClassicHttpResponse httpResponse) throws IOException {
		this.httpRequest = httpRequest;
		this.httpResponse = httpResponse;
		statusLine = new StatusLine(httpResponse);
		body = EntityUtils.toByteArray(httpResponse.getEntity());
	}

    public AbstractHttpRequest request() {
		return httpRequest;
	}

    public int statusCode() {
		return statusLine.getStatusCode();
	}

    public Map<String, List<String>> headers() {
		Map<String, List<String>> headersMap = new HashMap<>();
        for(org.apache.hc.core5.http.Header header : httpResponse.getHeaders()) {
            List<String> values = headersMap.get(header.getName());
            if(null == values) {
                values = new ArrayList<String>();
                headersMap.put(header.getName(), values);
            }
            values.add(header.getValue());
        }

		return headersMap;
	}

    public byte[] body() {
		return body;
	}
}

