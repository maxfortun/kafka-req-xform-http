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

import org.apache.kafka.common.header.Header;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static String utf8(byte[] bytes) {
        if(null == bytes) {
            return null;
        }
        return org.apache.kafka.common.utils.Utils.utf8(bytes);
    }

    public static String utf8(ByteBuffer byteBuffer) {
        if(null == byteBuffer) {
            return null;
        }
        return org.apache.kafka.common.utils.Utils.utf8(byteBuffer);
    }

    public static String toString(Header[] headers) {
        StringBuffer stringBuffer = new StringBuffer();
        for(Header header : headers) {
            stringBuffer.append("  H:");
            stringBuffer.append(header.key());
            stringBuffer.append("=");
            stringBuffer.append(new String(header.value(), StandardCharsets.UTF_8));
            stringBuffer.append("\n");
        }
        return stringBuffer.toString();
    }
}
