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

import java.util.List;
import java.util.Properties;

import org.apache.hugegraph.kafka.consumer.ConsumerClient;
import org.apache.hugegraph.kafka.topic.SyncConfTopicBuilder;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.util.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

/**
 * @author Scorpiour
 * @since 2022-02-02
 */
public class SyncConfConsumer extends ConsumerClient<String, String> {

    private static final Logger LOG = Log.logger(SyncConfConsumer.class);
    protected final BrokerConfig config = BrokerConfig.getInstance();
    private final MetaManager manager = MetaManager.instance();

    protected SyncConfConsumer(Properties props) {
        super(props);
    }

    @Override
    protected boolean handleRecord(ConsumerRecord<String, String> record) {
        if (BrokerConfig.getInstance().needKafkaSyncStorage()) {
            List<String> etcdKV = SyncConfTopicBuilder.extractKeyValue(record);
            String etcdKey = etcdKV.get(0);
            String etcdVal = etcdKV.get(1);

            if (config.isMaster()) {

            } else if (config.isSlave()) {
                manager.kafkaPutOrDeleteRaw(etcdKey, etcdVal);
            }
            return true;
        }
        return false;
    }

}
