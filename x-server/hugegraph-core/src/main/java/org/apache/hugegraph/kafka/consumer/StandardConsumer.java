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
import org.apache.hugegraph.meta.MetaManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Used to consume HugeGraphSyncTopic then send the data to slave cluster
 *
 * @author Scorpiour
 * @since 2022-01-25
 */
public class StandardConsumer extends ConsumerClient<String, ByteBuffer> {

    MetaManager manager = MetaManager.instance();

    protected StandardConsumer(Properties props) {
        super(props);
    }

    @Override
    protected boolean handleRecord(ConsumerRecord<String, ByteBuffer> record) {
        if (BrokerConfig.getInstance().needKafkaSyncStorage()) {
            return true;
        }
        return false;
    }

}
