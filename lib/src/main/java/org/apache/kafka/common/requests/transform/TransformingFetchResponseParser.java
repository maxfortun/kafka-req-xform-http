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

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponseParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformingFetchResponseParser implements FetchResponseParser {
    private static final Logger log = LoggerFactory.getLogger(TransformingFetchResponseParser.class);

    private static Class[] transformerConstructorParameterTypes = new Class[] {String.class};

    private ResourceBundle resources = ResourceBundle.getBundle("TransformingFetchResponseParser");
    private Collection<ByteBufferTransformer> byteBufferTransformers = new ArrayList<>();
    private Collection<FetchResponseDataTransformer> fetchResponseDataTransformers = new ArrayList<>();

    public TransformingFetchResponseParser() {
        try {
            try {
                String[] byteBufferTransformerNames = resources.getString("byteBufferTransformers").split("[\\s,;]+");
                for (String byteBufferTransformerName : byteBufferTransformerNames) {
                    byteBufferTransformers.add((ByteBufferTransformer) getTransformer(byteBufferTransformerName));
                }
            } catch(MissingResourceException mre) {}

            try {
                String[] fetchResponseDataTransformerNames = resources.getString("fetchResponseDataTransformers").split("[\\s,;]+");
                for (String fetchResponseDataTransformerName : fetchResponseDataTransformerNames) {
                    fetchResponseDataTransformers.add((FetchResponseDataTransformer) getTransformer(fetchResponseDataTransformerName));
                }
            } catch(MissingResourceException mre) {}
        } catch (Exception e) {
            String message = "Failed to initialize";
            log.error(message, e);
            throw new InvalidConfigurationException(message, e);
        }
    }

    private Object getTransformer(String transformerName) throws Exception {
        String transformerClassName = resources.getString(transformerName + ".class");

        Class<?> transformerClass = Class.forName(transformerClassName);
        Constructor<?> transformerConstructor = transformerClass.getConstructor(transformerConstructorParameterTypes);
        return transformerConstructor.newInstance(new Object[] {transformerName});
    }

    public FetchResponse parse(ByteBuffer byteBuffer, short version) {
        log.trace("byteBuffer in: {}", byteBuffer);
        for (ByteBufferTransformer byteBufferTransformer : byteBufferTransformers) {
            byteBuffer = byteBufferTransformer.transform(byteBuffer, version);
        }
        log.trace("byteBuffer out: {}", byteBuffer);

        FetchResponseData fetchResponseData = new FetchResponseData(new ByteBufferAccessor(byteBuffer), version);
        log.trace("fetchResponseData in: {}", fetchResponseData);

        for (FetchResponseDataTransformer fetchResponseDataTransformer : fetchResponseDataTransformers) {
            fetchResponseData = fetchResponseDataTransformer.transform(fetchResponseData, version);
        }

        log.trace("fetchResponseData out: {}", fetchResponseData);

        return new FetchResponse(fetchResponseData);
    }
}
