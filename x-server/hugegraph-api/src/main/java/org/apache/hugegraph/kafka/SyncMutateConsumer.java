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

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.kafka.consumer.StandardConsumer;
import org.apache.hugegraph.kafka.topic.HugeGraphMutateTopicBuilder;
import org.apache.hugegraph.util.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

/**
 * Used to consume HugeGraphMutateTopic, that is used to apply mutate to storage
 * This consumer is used in Slave cluster only
 *
 * @author Scorpiour
 * @since 2022-01-22
 */
public class SyncMutateConsumer extends StandardConsumer {

    private static final Logger LOG = Log.logger(SyncMutateConsumer.class);

    private GraphManager manager;
    private HugeGraph graph;

    protected SyncMutateConsumer(Properties props) {
        super(props);
    }

    protected void setGraphManager(GraphManager manager) {
        this.manager = manager;
    }

    public void consume(HugeGraph graph) {
        this.graph = graph;
        this.consume();
    }

    @Override
    protected boolean handleRecord(ConsumerRecord<String, ByteBuffer> record) {

        if (BrokerConfig.getInstance().needKafkaSyncStorage()) {

            if (null == this.manager && null == this.graph) {
                return true;
            }
            String[] graphInfo = HugeGraphMutateTopicBuilder.extractGraphs(record);
            String graphSpace = graphInfo[0];
            String graphName = graphInfo[1];


            HugeGraph graph = manager.graph(graphSpace, graphName);
            if (null == graph) {
                return false;
            }
            BackendMutation mutation = HugeGraphMutateTopicBuilder.buildMutation(record.value());
            graph.applyMutation(mutation);
            graph.tx().commit();
            return true;
        }
        return false;

    }

    @Override
    public void close() {
        super.close();
    }
}
