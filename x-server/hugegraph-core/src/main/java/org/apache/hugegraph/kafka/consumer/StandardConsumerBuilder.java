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

package org.apache.hugegraph.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.kafka.topic.HugeGraphSyncTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StandardConsumerBuilder extends ConsumerBuilder<String, ByteBuffer> {

    private final static Object MTX = new Object();
    private static StandardConsumer consumer;

    public StandardConsumerBuilder() {

        super();

        this.topic = HugeGraphSyncTopic.TOPIC;
        this.groupId = BrokerConfig.getInstance().getSyncGroupId();
        this.groupInstanceId = BrokerConfig.getInstance().getSyncGroupInstanceId();
        this.kafkaHost = BrokerConfig.getInstance().getKafkaHost();
        this.kafkaPort = BrokerConfig.getInstance().getKafkaPort();
        this.keyDeserializer = StringDeserializer.class;
        this.valueDeserializer = ByteBufferDeserializer.class;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String, ByteBuffer> setKeyDeserializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String, ByteBuffer> setValueDeserializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String, ByteBuffer> setKafkaHost(String host) {
        return this;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String, ByteBuffer> setKafkaPort(String port) {
        return this;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String, ByteBuffer> setGroupId(String groupId) {
        return this;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String, ByteBuffer> setGroupInstanceId(String groupInstanceId) {
        return this;
    }

    @Override
    public StandardConsumer build() {

        synchronized (StandardConsumerBuilder.MTX) {
            if (StandardConsumerBuilder.consumer == null) {
                Properties props = new Properties();

                String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                          this.valueDeserializer.getName());
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                          this.keyDeserializer.getName());
                props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
                // props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, this.groupInstanceId);
                props.put("topic", topic);

                StandardConsumerBuilder.consumer = new StandardConsumer(props);
            }
        }

        return consumer;
    }

}
