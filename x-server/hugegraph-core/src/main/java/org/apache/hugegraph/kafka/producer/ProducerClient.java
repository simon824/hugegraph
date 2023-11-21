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

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.kafka.topic.TopicBase;
import org.apache.hugegraph.util.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

// import org.slf4j.Logger;

/**
 * Kafka producer encapsulation
 *
 * @author Scorpiour
 * @since 2022-01-18
 */
public class ProducerClient<K, V> {

    private static final Logger LOG = Log.logger(ProducerClient.class);

    private final KafkaProducer<K, V> producer;
    private final ExecutorService asyncExecutor;
    private volatile boolean closing = false;

    protected ProducerClient(Properties props) {
        asyncExecutor = Executors.newSingleThreadExecutor();
        producer = new KafkaProducer<>(props);
    }

    /**
     * Produce by custom key-value
     *
     * @param key
     * @param value
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public Future<?> produce(String topic, int partition, K key, V value) throws
                                                                          InterruptedException,
                                                                          ExecutionException {
        if (closing) {
            throw new IllegalStateException("Cannot produce when producer is closing");
        }
        return asyncExecutor.submit(() -> {
            try {
                ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, key, value);
                producer.send(record);
            } catch (Exception e) {

            }
            // producer.flush();
        });
    }

    /**
     * Produce by encapsulated topic
     *
     * @param topic
     * @return
     */
    public Future<?> produce(TopicBase<K, V> topic) {
        if (closing) {
            throw new IllegalStateException("Cannot produce when producer is closing");
        }
        boolean needSync = BrokerConfig.getInstance().needKafkaSyncBroker();
        if (!needSync) {
            return null;
        }

        return asyncExecutor.submit(() -> {
            try {
                ProducerRecord<K, V> record = new ProducerRecord<>(
                        topic.getTopic(),
                        topic.getPartition(),
                        topic.getKey(),
                        topic.getValue());
                producer.send(record); //.get();
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
            } finally {
                producer.flush();
            }
        });
    }

    public void close(long ttl) {
        this.closing = true;
        asyncExecutor.submit(new Runnable() {
            @Override
            public void run() {
                if (null != producer) {
                    Duration duration = Duration.ofSeconds(ttl);
                    producer.close(duration);
                }
            }
        });
    }

    public void close() {
        close(30);
    }

}
