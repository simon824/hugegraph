/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.kafka.topic;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;

import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.StoreSerializer;
import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.util.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;


public class HugeGraphSyncTopicBuilder {

    private static final Logger LOG = Log.logger(HugeGraphSyncTopicBuilder.class);
    private static final int PARTITION_COUNT = BrokerConfig.getInstance().getPartitionCount();
    private static final String DELIM = "/";
    private String graphName;
    private String graphSpace;
    private BackendMutation mutation;

    public HugeGraphSyncTopicBuilder() {

    }

    /**
     * Extract graphSpace and graphName
     *
     * @param record
     * @return [{graphSpace}, {graphName}]
     */
    public static String[] extractGraphs(ConsumerRecord<String, ByteBuffer> record) {
        String[] keys = record.key().split(DELIM);
        if (keys.length != 2) {
            throw new InvalidParameterException("invalid record key: " + record.key());
        }
        return keys;
    }

    private String makeKey() {
        // {graphSpace}/{graphName}
        return String.join(DELIM, this.graphSpace, this.graphName);
    }

    /**
     * 使用graph的hashCode来计算partition，确保一个graph总在同一个partition内
     *
     * @return
     */
    private int calcPartition() {
        if (PARTITION_COUNT <= 1) {
            return 0;
        }
        int code = this.graphName.hashCode() % (PARTITION_COUNT - 1);
        if (code < 0) {
            code = -code;
        }
        return code + 1;
    }

    public HugeGraphSyncTopicBuilder setMutation(BackendMutation mutation) {
        this.mutation = mutation;
        return this;
    }

    public HugeGraphSyncTopicBuilder setGraphName(String graphName) {
        this.graphName = graphName;
        return this;
    }

    public HugeGraphSyncTopicBuilder setGraphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
        return this;
    }

    public HugeGraphSyncTopic build() {

        String key = this.makeKey();

        byte[] value = StoreSerializer.writeMutation(mutation);
        ByteBuffer buffer = ByteBuffer.wrap(value);
        HugeGraphSyncTopic topic = new HugeGraphSyncTopic(key, buffer, this.calcPartition());

        return topic;
    }
}
