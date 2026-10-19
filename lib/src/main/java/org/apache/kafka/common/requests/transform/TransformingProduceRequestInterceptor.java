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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InterceptResult;
import org.apache.kafka.common.requests.ProduceRequestInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interceptor that transforms produce requests and can return per-partition errors
 * without closing the client connection. Errors are returned as protocol-level
 * responses that the producer can handle (retry or fail gracefully).
 */
public class TransformingProduceRequestInterceptor implements ProduceRequestInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TransformingProduceRequestInterceptor.class);

    private static Class[] transformerConstructorParameterTypes = new Class[] {String.class};

    private ResourceBundle resources = ResourceBundle.getBundle("TransformingProduceRequestInterceptor");
    private Collection<ProduceRequestDataInterceptor> interceptors = new ArrayList<>();

    public TransformingProduceRequestInterceptor() {
        try {
            try {
                String[] interceptorNames = resources.getString("interceptors").split("[\\s,;]+");
                for (String interceptorName : interceptorNames) {
                    interceptors.add((ProduceRequestDataInterceptor) getInterceptor(interceptorName));
                }
            } catch(MissingResourceException mre) {
                log.debug("No interceptors configured in TransformingProduceRequestInterceptor.properties");
            }
        } catch (Exception e) {
            String message = "Failed to initialize TransformingProduceRequestInterceptor";
            log.error(message, e);
            throw new InvalidConfigurationException(message, e);
        }
    }

    private Object getInterceptor(String interceptorName) throws Exception {
        String interceptorClassName = resources.getString(interceptorName + ".class");

        Class<?> interceptorClass = Class.forName(interceptorClassName);
        Constructor<?> interceptorConstructor = interceptorClass.getConstructor(transformerConstructorParameterTypes);
        return interceptorConstructor.newInstance(new Object[] {interceptorName});
    }

    @Override
    public InterceptResult intercept(ProduceRequestData data, short version) {
        log.trace("intercept in: {}", data);

        Map<TopicPartition, Errors> allErrors = new HashMap<>();
        ProduceRequestData currentData = data;

        for (ProduceRequestDataInterceptor interceptor : interceptors) {
            InterceptResult result = interceptor.intercept(currentData, version);
            currentData = result.data();

            // Accumulate errors from all interceptors
            if (result.hasErrors()) {
                allErrors.putAll(result.partitionErrors());
            }
        }

        log.trace("intercept out: {}, errors: {}", currentData, allErrors);

        return new InterceptResult(currentData, allErrors);
    }
}
