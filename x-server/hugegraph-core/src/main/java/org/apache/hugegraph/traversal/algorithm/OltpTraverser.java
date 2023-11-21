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

package org.apache.hugegraph.traversal.algorithm;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.collection.JniIdSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.google.common.base.Objects;

public abstract class OltpTraverser extends HugeTraverser
        implements AutoCloseable {

    private static final String EXECUTOR_NAME = "oltp";
    private static Consumers.ExecutorPool executors;

    protected OltpTraverser(HugeGraph graph) {
        super(graph);
        if (executors != null) {
            return;
        }
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                return;
            }
            int workers = this.graph().option(CoreOptions.OLTP_CONCURRENT_THREADS);
            if (workers > 0) {
                executors = new Consumers.ExecutorPool(EXECUTOR_NAME, workers);
            }
        }
    }

    public static void destroy() {
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                executors.destroy();
                executors = null;
            }
        }
    }

    @Override
    public void close() {
        // pass
    }

    protected long traversePairs(Iterator<Pair<Id, Id>> pairs,
                                 Consumer<Pair<Id, Id>> consumer) {
        return this.traverse(pairs, consumer, "traverse-pairs");
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer) {
        return this.traverse(ids, consumer, "traverse-ids");
    }

    protected <K> long traverse(Iterator<K> iterator, Consumer<K> consumer,
                                String name) {
        if (!iterator.hasNext()) {
            return 0L;
        }
        long total = 0L;
        AtomicBoolean done = new AtomicBoolean(false);
        Consumers<K> consumers = null;
        try {
            consumers = new Consumers<>(executors.getExecutor(),
                                        consumer, () -> {
                done.set(true);
            });
            consumers.start(name);
            while (iterator.hasNext() && !done.get()) {
                total++;
                K v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            this.edgeIterCounter.addAndGet(total);
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    protected <K> long traverseBatchCurrentThread(Iterator<CIter<K>> iterator,
                                                  Consumer<CIter<K>> consumer,
                                                  String name,
                                                  int queueWorkerSize) {
        if (!iterator.hasNext()) {
            return 0L;
        }
        AtomicBoolean done = new AtomicBoolean(false);
        Consumers<CIter<K>> consumers = null;
        try {
            consumers = getConsumers(consumer, queueWorkerSize, done,
                                     null);
            return consumersStart(iterator, name, done, consumers);
        } catch (Exception e) {
            throw e;
        } finally {
            executors.returnExecutor(consumers.executor());
        }
    }

    protected <K> long traverseBatch(Iterator<CIter<K>> iterator,
                                     Consumer<CIter<K>> consumer,
                                     String name, int queueWorkerSize) {
        if (!iterator.hasNext()) {
            return 0L;
        }
        AtomicBoolean done = new AtomicBoolean(false);
        Consumers<CIter<K>> consumers = null;
        try {
            consumers = getConsumers(consumer, queueWorkerSize, done,
                                     executors.getExecutor());
            return consumersStart(iterator, name, done, consumers);
        } catch (Exception e) {
            throw e;
        } finally {
            executors.returnExecutor(consumers.executor());
        }
    }

    private <K> long consumersStart(Iterator<CIter<K>> iterator, String name,
                                    AtomicBoolean done,
                                    Consumers<CIter<K>> consumers) {
        long total = 0L;
        try {
            consumers.start(name);
            while (iterator.hasNext() && !done.get()) {
                total++;
                CIter<K> v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    private <K> Consumers<CIter<K>> getConsumers(
            Consumer<CIter<K>> consumer, int queueWorkerSize,
            AtomicBoolean done, ExecutorService executor) {
        Consumers<CIter<K>> consumers;
        consumers = new Consumers<>(executor,
                                    consumer, null,
                                    e -> {
                                        done.set(true);
                                    }, queueWorkerSize);
        return consumers;
    }

    protected Iterator<Vertex> filter(Iterator<Vertex> vertices,
                                      String key, Object value) {
        return new FilterIterator<>(vertices, vertex -> match(vertex, key, value));
    }

    protected boolean match(Element elem, String key, Object value) {
        // check property key exists
        this.graph().propertyKey(key);
        // return true if property value exists & equals to specified value
        Property<Object> p = elem.property(key);
        return p.isPresent() && Objects.equal(p.value(), value);
    }

    protected void bfsQuery(Iterator<Id> vertices, Directions dir,
                            Id label,
                            long degree, long skipDegree,
                            long capacity,
                            Consumer<EdgeId> parseConsumer,
                            Query.OrderType orderType) {
        List<Id> labels = label == null ? null : Collections.singletonList(label);
        CapacityConsumer consumer = new CapacityConsumer(parseConsumer, capacity);

        EdgeIdsIterator edgeIts =
                edgeIdsOfVertices(vertices, dir, labels, degree, skipDegree, orderType);
        // 并行乱序处理
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edgeid", 1);
    }

    protected void bfsQueryForPersonalRank(Iterator<Id> vertices,
                                           Directions dir,
                                           List<Id> labels,
                                           long degree, long skipDegree,
                                           long capacity,
                                           Consumer<EdgeId> parseConsumer,
                                           Query.OrderType orderType) {
        CapacityConsumer consumer = new CapacityConsumer(parseConsumer, capacity);

        EdgeIdsIterator edgeIts =
                edgeIdsOfVertices(vertices, dir, labels, degree, skipDegree, orderType);
        // 并行乱序处理
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edgeid", 1);
    }

    protected void bfsQuery(Iterator<Id> vertices, Directions dir,
                            List<Id> labels,
                            long degree, long skipDegree,
                            long capacity,
                            Consumer<EdgeId> parseConsumer,
                            Query.OrderType orderType) {
        CapacityConsumer consumer = new CapacityConsumer(parseConsumer, capacity);
        EdgeIdsIterator edgeIts =
                edgeIdsOfVertices(vertices, dir, labels, degree, skipDegree, orderType);
        // 并行乱序处理
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edgeid", 1);
    }

    protected void bfsQuery(Iterator<Id> vertices,
                            Steps steps,
                            long capacity,
                            Consumer<EdgeId> parseConsumer,
                            Query.OrderType orderType) {
        CapacityConsumerWithStep consumer =
                new CapacityConsumerWithStep(parseConsumer, capacity, steps);

        boolean withEdgeStepProperties = !steps.isEdgeStepPropertiesEmpty();
        EdgesQueryIterator queryIterator =
                new EdgesQueryIterator(vertices, steps.direction(), steps.edgeLabels(),
                                       steps.degree(), steps.skipDegree(),
                                       withEdgeStepProperties,
                                       orderType);
        if (withEdgeStepProperties || !steps.isVertexEmpty()) {
            queryIterator.setCondition(
                    getConditionWithStepsForQuery(steps.edgeSteps(), steps.vertexSteps()));
        }

        // 这里获取边数据，以便支持 step
        EdgesIterator edgeIts = new EdgesIterator(queryIterator);
        // consumer.steps = steps;

        // 并行乱序处理
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
    }

    protected <E> void bfsQuery(Iterator<Id> vertices,
                                EdgeStep edgeStep,
                                long capacity,
                                Consumer<E> parseConsumer,
                                boolean isEdgeConsumer,
                                Query.OrderType orderType) {
        bfsQuery(vertices, edgeStep, capacity, parseConsumer, isEdgeConsumer, orderType, false);
    }

    protected <E> void bfsQuery(Iterator<Id> vertices,
                                EdgeStep edgeStep,
                                long capacity,
                                Consumer<E> parseConsumer,
                                boolean isEdgeConsumer,
                                Query.OrderType orderType,
                                boolean withEdgeProperties) {
        CapacityEdgeConsumer consumer =
                new CapacityEdgeConsumer<>(parseConsumer, capacity, isEdgeConsumer);

        boolean withEdgeStepProperties =
                (edgeStep.properties() != null && !edgeStep.properties().isEmpty());
        EdgesQueryIterator queryIterator = new EdgesQueryIterator(vertices, edgeStep.direction(),
                                                                  List.of(edgeStep.edgeLabels()),
                                                                  edgeStep.degree(),
                                                                  edgeStep.skipDegree(),
                                                                  withEdgeStepProperties ||
                                                                  withEdgeProperties,
                                                                  orderType);
        if (withEdgeStepProperties) {
            queryIterator.setCondition(
                    TraversalUtil.conditionEdgeStep(edgeStep.properties(), graph()));
        }

        // 这里获取边数据，以便支持 step
        EdgesIterator edgeIts = new EdgesIterator(queryIterator);
        // consumer.steps = steps;

        // 并行乱序处理
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
    }

    protected <E> Set<E> subset(Set<E> set, long limit) {
        if (set instanceof JniIdSet) {
            ((JniIdSet) set).setLimit(limit);
        } else {
            int redundantNeighborsCount = (int) (set.size() - limit);
            if (redundantNeighborsCount > 0) {
                Set<E> redundantNeighbors =
                        new HashSet<>(redundantNeighborsCount);

                for (E vId : set) {
                    redundantNeighbors.add(vId);
                    if (redundantNeighbors.size() >= redundantNeighborsCount) {
                        break;
                    }
                }
                return redundantNeighbors;
            }
        }

        return set;
    }

    static class ConcurrentVerticesConsumer implements Consumer<EdgeId> {
        // 要求 Set 支持并发
        protected Id sourceV;
        protected Set<Id> excluded;
        protected Set<Id> neighbors;
        // 考虑到性能，这里为非精准的限制
        protected long limit;
        protected AtomicInteger count = new AtomicInteger(0);
        protected boolean useJniAddExclusive;

        public ConcurrentVerticesConsumer(Id sourceV, Set<Id> excluded,
                                          long limit, Set<Id> neighbors) {
            this.sourceV = sourceV;
            this.excluded = excluded;
            this.limit = limit;
            this.neighbors = neighbors;
            this.useJniAddExclusive = neighbors instanceof JniIdSet && excluded instanceof JniIdSet;
        }

        @Override
        public void accept(EdgeId edgeId) {
            if (limit != NO_LIMIT && count.get() >= limit) {
                throw new Consumers.StopExecution("reach limit");
                // return;
            }
            Id target = edgeId.otherVertexId();
            if (sourceV.equals(target)) {
                return;
            }

            if (useJniAddExclusive) {
                if (((JniIdSet) neighbors).addExclusive(target, (JniIdSet) excluded)) {
                    if (limit != NO_LIMIT) {
                        count.getAndIncrement();
                    }
                }
            } else {
                if (excluded != null && excluded.contains(target)) {
                    return;
                }
                if (neighbors.add(target)) {
                    if (limit != NO_LIMIT) {
                        count.getAndIncrement();
                    }
                }
            }
        }
    }

    protected abstract class EdgeItConsumer<T, E> implements Consumer<CIter<T>> {
        private final Consumer<E> parseConsumer;
        private final long capacity;

        public EdgeItConsumer(Consumer<E> parseConsumer, long capacity) {
            this.parseConsumer = parseConsumer;
            this.capacity = capacity;
        }

        protected abstract CIter<E> prepare(CIter<T> it);

        @Override
        public void accept(CIter<T> edges) {
            CIter<E> ids = prepare(edges);
            try {
                long counter = 0;
                while (ids.hasNext()) {
                    if (Thread.currentThread().isInterrupted()) {
                        LOG.warn("Consumer isInterrupted");
                        break;
                    }
                    counter++;
                    parseConsumer.accept(ids.next());
                }
                long total = edgeIterCounter.addAndGet(counter);
                // 按批次检测 capacity，以提高性能
                if (this.capacity != NO_LIMIT && total >= capacity) {
                    // todo： 放弃迭代
                    throw new Consumers.StopExecution("reach capacity");
                }
            } finally {
                try {
                    ids.close();
                } catch (Exception ex) {
                    LOG.warn("Exception when closing CIter", ex);
                }
            }
        }
    }

    protected class CapacityConsumerWithStep extends EdgeItConsumer<Edge, EdgeId> {
        public Steps steps;

        public CapacityConsumerWithStep(Consumer<EdgeId> parseConsumer,
                                        long capacity,
                                        Steps steps) {
            super(parseConsumer, capacity);
            this.steps = steps;
        }

        @Override
        protected CIter<EdgeId> prepare(CIter<Edge> edges) {
            Iterator<Edge> it = edgesOfVertexStep(edges, steps);
            return new MapperIterator<>(it, (e) -> ((HugeEdge) e).id());
        }
    }

    protected class CapacityConsumer extends EdgeItConsumer<EdgeId, EdgeId> {
        public CapacityConsumer(Consumer<EdgeId> parseConsumer, long capacity) {
            super(parseConsumer, capacity);
        }

        @Override
        protected CIter<EdgeId> prepare(CIter<EdgeId> edges) {
            return edges;
        }
    }

    protected class CapacityEdgeConsumer<E> extends EdgeItConsumer<Edge, E> {
        private final boolean isEdge;

        public CapacityEdgeConsumer(Consumer<E> parseConsumer, long capacity, boolean isEdge) {
            super(parseConsumer, capacity);
            this.isEdge = isEdge;
        }

        @Override
        protected CIter<E> prepare(CIter<Edge> edges) {
            if (isEdge) {
                return (CIter<E>) edges;
            } else {
                return new MapperIterator<>(edges, (e) -> (E) ((HugeEdge) e).id());
            }
        }
    }
}
