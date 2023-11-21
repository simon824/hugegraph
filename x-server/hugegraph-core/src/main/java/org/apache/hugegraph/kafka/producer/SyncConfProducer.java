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

package org.apache.hugegraph.kafka.producer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.kafka.topic.SyncConfTopic;
import org.apache.hugegraph.kafka.topic.SyncConfTopicBuilder;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.base.Strings;

/**
 * Sync etcd service conf from master to slave
 */
public class SyncConfProducer extends ProducerClient<String, String> {

    private static final Logger LOG = Log.logger(ProducerClient.class);

    private final MetaManager manager = MetaManager.instance();

    protected SyncConfProducer(Properties props) {
        super(props);

        if (BrokerConfig.getInstance().isMaster()) {
            manager.listenAll(this::listenEtcdChanged);
        }
    }

    private <T> void listenEtcdChanged(T response) {
        Map<String, String> map = manager.extractKVFromResponse(response);
        map.entrySet().forEach((entry) -> {
            String key = entry.getKey();
            String value = entry.getValue();

            String[] keyParts = key.split(MetaManager.META_PATH_DELIMITER);
            Boolean isGraphPath = false;
            Boolean isGraphspacePath = false;
            Boolean isGraphConfPath = false;
            for (int i = keyParts.length - 1; i > 1; i--) {
                String part = keyParts[i];
                switch (part) {
                    case MetaManager.META_PATH_TASK:
                    case MetaManager.META_PATH_SERVICE_CONF:
                    case MetaManager.META_PATH_SERVICE:
                    case MetaManager.META_PATH_TASK_LOCK:
                    case MetaManager.META_PATH_KAFKA:
                    case MetaManager.META_PATH_DDS:
                        // filter specified keys only appears after index 2
                        return;
                    case MetaManager.META_PATH_GRAPH_CONF:
                        isGraphConfPath = true;
                        break;
                    case MetaManager.META_PATH_GRAPH:
                        isGraphPath = true;
                        break;
                    case MetaManager.META_PATH_GRAPHSPACE:
                        isGraphspacePath = true;
                        break;
                }
            }

            try {
                String graphSpace = null;

                // Check if graph is filtered, If it is true skip
                if (isGraphConfPath) {
                    List<String> info = manager.extractGraphFromKey(key);
                    if (info.size() >= 2) {
                        graphSpace = info.get(0);
                        String graph = info.get(1);
                        if (BrokerConfig.getInstance().graphFiltered(graphSpace, graph)) {
                            return;
                        }
                    }
                } else if (isGraphPath) {
                    String[] parts = entry.getValue().split("-");
                    if (parts.length == 2 &&
                        BrokerConfig.getInstance().graphFiltered(parts[0], parts[1])) {
                        return;
                    }
                } else if (isGraphspacePath) {
                    if (Strings.isNullOrEmpty(graphSpace)) {
                        graphSpace = manager.extractGraphSpaceFromKey(key);
                    }
                    if (BrokerConfig.getInstance().graphSpaceFiltered(graphSpace)) {
                        return;
                    }
                }

                SyncConfTopic topic = new SyncConfTopicBuilder()
                        .setKey(key)
                        .setValue(value)
                        .build();
                this.produce(topic);
            } catch (Throwable t) {

            }
        });
    }
}
