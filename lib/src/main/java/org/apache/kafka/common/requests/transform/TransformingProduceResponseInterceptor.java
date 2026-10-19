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
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.requests.ProduceResponseInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interceptor that chains multiple response interceptors.
 * Calls each interceptor's onAppend method after records are written.
 */
public class TransformingProduceResponseInterceptor implements ProduceResponseInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TransformingProduceResponseInterceptor.class);

    private static Class[] interceptorConstructorParameterTypes = new Class[] {String.class};

    private ResourceBundle resources = ResourceBundle.getBundle("TransformingProduceResponseInterceptor");
    private Collection<ProduceResponseDataInterceptor> interceptors = new ArrayList<>();

    public TransformingProduceResponseInterceptor() {
        try {
            try {
                String[] interceptorNames = resources.getString("interceptors").split("[\\s,;]+");
                for (String interceptorName : interceptorNames) {
                    interceptors.add((ProduceResponseDataInterceptor) getInterceptor(interceptorName));
                }
            } catch (MissingResourceException mre) {
                log.debug("No interceptors configured in TransformingProduceResponseInterceptor.properties");
            }
        } catch (Exception e) {
            String message = "Failed to initialize TransformingProduceResponseInterceptor";
            log.error(message, e);
            throw new InvalidConfigurationException(message, e);
        }
    }

    private Object getInterceptor(String interceptorName) throws Exception {
        String interceptorClassName = resources.getString(interceptorName + ".class");

        Class<?> interceptorClass = Class.forName(interceptorClassName);
        Constructor<?> interceptorConstructor = interceptorClass.getConstructor(interceptorConstructorParameterTypes);
        return interceptorConstructor.newInstance(new Object[] {interceptorName});
    }

    @Override
    public void onAppend(ProduceRequestData requestData,
                         Map<TopicPartition, PartitionResponse> responses,
                         short version) {
        log.trace("onAppend with {} partitions", responses.size());

        for (ProduceResponseDataInterceptor interceptor : interceptors) {
            try {
                interceptor.onAppend(requestData, responses, version);
            } catch (Exception e) {
                log.error("Interceptor {} failed", interceptor.getClass().getName(), e);
            }
        }
    }
}
