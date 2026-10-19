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

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;

/**
 * Interface for interceptors that process produce responses after records are written.
 * Used to notify external systems of assigned offsets.
 */
public interface ProduceResponseDataInterceptor {
    /**
     * Called after records have been appended to the log.
     *
     * @param requestData The produce request data that was written
     * @param responses Map of topic-partition to response with assigned offsets
     * @param version The API version
     */
    void onAppend(ProduceRequestData requestData,
                  Map<TopicPartition, PartitionResponse> responses,
                  short version);
}
