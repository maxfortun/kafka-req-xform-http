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

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HttpClient {
    public static final Logger log = LoggerFactory.getLogger(HttpClient.class);

	private final static Class[] httpClientConstructorParameterTypes = new Class[] {HttpProduceRequestDataTransformer.class};

	protected HttpProduceRequestDataTransformer httpProduceRequestDataTransformer;
	protected HttpClient(HttpProduceRequestDataTransformer httpProduceRequestDataTransformer) {
		this.httpProduceRequestDataTransformer = httpProduceRequestDataTransformer;
	}

    public static HttpClient newHttpClient(HttpProduceRequestDataTransformer httpProduceRequestDataTransformer) throws Exception {
		String httpClientClassName = httpProduceRequestDataTransformer.appConfig("httpClientClassName");

		if(null == httpClientClassName) {
			log.debug("Defaulting to {}.", JDKHttpClient.class);
			return new JDKHttpClient(httpProduceRequestDataTransformer);
		}

		Class<?> httpClientClass = Class.forName(httpClientClassName);
        Constructor<?> httpClientConstructor = httpClientClass.getConstructor(httpClientConstructorParameterTypes);
        HttpClient httpClient = (HttpClient)httpClientConstructor.newInstance(new Object[] {httpProduceRequestDataTransformer});
		log.debug("Using {}.", httpClient.getClass());
		return httpClient;
	}

	public abstract HttpRequest newHttpRequest();
	public abstract HttpResponse send(HttpRequest httpRequest);
}
