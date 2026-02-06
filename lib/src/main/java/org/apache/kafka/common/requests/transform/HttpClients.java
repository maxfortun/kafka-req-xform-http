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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.internals.RecordHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClients {
    private static final Logger log = LoggerFactory.getLogger(HttpClients.class);

    private final static Class[] httpClientConstructorParameterTypes = new Class[] {AbstractTransformer.class};

    private static Map<String, AbstractHttpClient> httpClients = new HashMap<String, AbstractHttpClient>();

    private static AbstractHttpClient newHttpClient(String httpClientClassName, AbstractTransformer transformer) throws Exception {
        Class<?> httpClientClass = Class.forName(httpClientClassName);
        Constructor<?> httpClientConstructor = httpClientClass.getConstructor(httpClientConstructorParameterTypes);
        AbstractHttpClient httpClient = (AbstractHttpClient)httpClientConstructor.newInstance(new Object[] {transformer});
        log.info("Created {}.", httpClient.getClass());
        return httpClient;
    }

    public static AbstractHttpClient getHttpClient(RecordHeaders recordHeaders, HttpProduceRequestDataTransformer httpProduceRequestDataTransformer) throws Exception {
        return getHttpClient(recordHeaders, (AbstractTransformer) httpProduceRequestDataTransformer);
    }

    public static AbstractHttpClient getHttpClient(RecordHeaders recordHeaders, AbstractTransformer transformer) throws Exception {
        String httpClientClassName = transformer.reqConfig(recordHeaders, "httpClient.class");
        AbstractHttpClient httpClient = httpClients.get(httpClientClassName);
        if(null != httpClient) {
            return httpClient;
        }

        synchronized(HttpClients.class) {
            httpClient = httpClients.get(httpClientClassName);
            if(null != httpClient) {
                return httpClient;
            }

            httpClient = newHttpClient(httpClientClassName, transformer);
            httpClients.put(httpClientClassName, httpClient);
            return httpClient;
        }
    }
}

