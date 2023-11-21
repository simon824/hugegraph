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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.kafka.producer.ProducerClient;
import org.apache.hugegraph.kafka.producer.SyncConfProducer;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Used to wrap the mutation sync related modules of slave
 *
 * @author Scorpiour
 * @since 2022-03-03
 */
public class SlaveServerWrapper {

    private static final Logger LOGGER = Log.logger(SlaveServerWrapper.class);
    private final SyncMutateConsumer consumer = new SyncMutateConsumerBuilder().build();
    private final ProducerClient<String, ByteBuffer> standardProducer =
            ClientFactory.getInstance().getStandardProducer();
    private final SyncConfProducer syncConfProducer =
            ClientFactory.getInstance().getSyncConfProducer();
    private volatile boolean closing = false;

    public static SlaveServerWrapper getInstance() {
        return InstanceHolder.instance;
    }

    private void initConsumer(GraphManager manager) {
        SyncMutateConsumerBuilder.setGraphManager(manager);
        consumer.setGraphManager(manager);
        consumer.consume();
    }

    private void initServer() throws IOException {
        if (BrokerConfig.getInstance().isSlave()) {
            // ProducerClient<String, ByteBuffer> producer = ClientFactory.getInstance()
            // .getStandardProducer();
            String CONF_PREFIX = BrokerConfig.getInstance().getConfPrefix();
        }
    }

    public void init(GraphManager manager) {

        // Init consumer first
        this.initConsumer(manager);

        // At last, init server wait for grpc
        try {
            initServer();
        } catch (IOException ioe) {
            LOGGER.error("Init Slave cluster Kafka-sync-server failed!", ioe);
        }

    }

    /**
     * Close resource, be aware of the order
     */
    public synchronized void close() {
        if (closing) {
            return;
        }
        closing = true;
        // close consumer
        try {
            if (null != consumer) {
                consumer.close();
            }
        } catch (Exception e) {

        }

    }

    private static class InstanceHolder {
        private static final SlaveServerWrapper instance = new SlaveServerWrapper();
    }

}
