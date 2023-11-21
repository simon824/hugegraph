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

package org.apache.hugegraph.kafka;

import java.util.Properties;

import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.kafka.consumer.StandardConsumerBuilder;
import org.apache.hugegraph.kafka.topic.HugeGraphMutateTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author Scorpiour
 * @since 2022-01-22
 */
public class SyncMutateConsumerBuilder extends StandardConsumerBuilder {

    private static GraphManager graphManager;

    public SyncMutateConsumerBuilder() {
        super();
        this.topic = HugeGraphMutateTopic.TOPIC;
        this.groupId = BrokerConfig.getInstance().getMutateGroupId();
        this.groupInstanceId = BrokerConfig.getInstance().getMutateGroupInstanceId();
    }

    public static void setGraphManager(GraphManager manager) {
        SyncMutateConsumerBuilder.graphManager = manager;
    }

    @Override
    @Deprecated
    public SyncMutateConsumerBuilder setTopic(String topic) {
        return this;
    }

    @Override
    public SyncMutateConsumer build() {
        Properties props = new Properties();

        String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        //props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, this.groupInstanceId);
        props.put("topic", topic);

        SyncMutateConsumer consumer = new SyncMutateConsumer(props);
        consumer.setGraphManager(SyncMutateConsumerBuilder.graphManager);
        return consumer;

    }
}
