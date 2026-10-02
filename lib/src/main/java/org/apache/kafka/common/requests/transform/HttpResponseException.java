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

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpResponseException extends Exception {
    private static final Logger log = LoggerFactory.getLogger(HttpResponseException.class);

    private HttpResponse httpResponse;

    public HttpResponseException(HttpResponse httpResponse) {
        super(httpResponse.request().uri()+" returned "+httpResponse.statusCode()+"\n"+describe(httpResponse));
        this.httpResponse = httpResponse;
    }

    public HttpResponse httpResponse() {
        return httpResponse;
    }

    public static String describe(HttpResponse httpResponse) {
        AbstractHttpRequest request = httpResponse.request();
        return "--- request ---\n"
            + (null == request ? "(unavailable)" : request.toString()) + "\n"
            + "--- response ---\n"
            + httpResponse.statusCode() + "\n"
            + AbstractHttpRequest.formatHeaders(httpResponse.headers()) + "\n\n"
            + new String(httpResponse.body(), StandardCharsets.UTF_8);
    }
}
