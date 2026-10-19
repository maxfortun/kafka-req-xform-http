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

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.requests.InterceptResult;

/**
 * Interface for interceptors that transform produce request data and can signal
 * per-partition errors without throwing exceptions.
 */
public interface ProduceRequestDataInterceptor {
    /**
     * Intercept and optionally transform produce request data.
     *
     * @param data The produce request data to intercept
     * @param version The API version
     * @return An InterceptResult containing the (possibly transformed) data and any partition errors.
     *         Partitions with errors will not be written to the log; instead, the error code will be
     *         returned to the producer in the response.
     */
    InterceptResult intercept(ProduceRequestData data, short version);
}
