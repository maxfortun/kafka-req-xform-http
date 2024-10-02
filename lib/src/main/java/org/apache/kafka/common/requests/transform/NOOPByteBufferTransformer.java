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
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NOOPByteBufferTransformer implements ByteBufferTransformer {
    public static final Logger log = LoggerFactory.getLogger(NOOPByteBufferTransformer.class);

    private String transformerName;

    public NOOPByteBufferTransformer(String transformerName) {
        this.transformerName = transformerName;
    }

    public ByteBuffer transform(ByteBuffer byteBuffer, short version) {
        int prevPosition = byteBuffer.position();

        if (log.isTraceEnabled()) {
            log.trace("{}: Returning buffer as-is {} {}", transformerName, Utils.utf8(byteBuffer));
        } else {
            log.debug("{}: Returning buffer as-is {}", transformerName, byteBuffer);
        }

        byteBuffer.position(prevPosition);
        return byteBuffer;
    }
}
