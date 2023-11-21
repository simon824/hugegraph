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

package org.apache.hugegraph.traversal.optimize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

public final class HugeGraphStep<S, E extends Element>
        extends GraphStep<S, E> implements QueryHolder {

    private static final long serialVersionUID = -679873894532085972L;

    private static final Logger LOG = Log.logger(HugeGraphStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    // Store limit/order-by
    private final Query queryInfo = new Query(HugeType.UNKNOWN);

    private Iterator<E> lastTimeResults = QueryResults.emptyIterator();

    public HugeGraphStep(final GraphStep<S, E> originGraphStep) {
        super(originGraphStep.getTraversal(),
              originGraphStep.getReturnClass(),
              originGraphStep.isStartStep(),
              originGraphStep.getIds());

        originGraphStep.getLabels().forEach(this::addLabel);

        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        this.setIteratorSupplier(() -> {
            Iterator<E> results = queryVertex ? this.vertices() : this.edges();
            this.lastTimeResults = results;
            return results;
        });
    }

    protected long count() {
        if (this.returnsVertex()) {
            return this.verticesCount();
        } else {
            assert this.returnsEdge();
            return this.edgesCount();
        }
    }

    private long verticesCount() {
        Iterator<E> result;
        if (this.hasIds()) {
            result = this.vertices();
            try {
                return IteratorUtils.count(result);
            } finally {
                CloseableIterator.closeIterator(result);
            }
        }
        HugeGraph graph = TraversalUtil.getGraph(this);
        Query query = this.makeQuery(graph, HugeType.VERTEX);
        return graph.queryNumber(query).longValue();
    }

    private long edgesCount() {
        Iterator<E> result;
        if (this.hasIds()) {
            result = this.edges();
            try {
                return IteratorUtils.count(result);
            } finally {
                CloseableIterator.closeIterator(result);
            }
        }
        HugeGraph graph = TraversalUtil.getGraph(this);
        Query query = this.makeQuery(graph, HugeType.EDGE);
        return graph.queryNumber(query).longValue();
    }

    @SuppressWarnings("unchecked")
    private Iterator<E> vertices() {
        LOG.debug("HugeGraphStep.vertices(): {}", this);

        HugeGraph graph = TraversalUtil.getGraph(this);
        // g.V().hasId(EMPTY_LIST) will set ids to null
        if (this.ids == null) {
            return QueryResults.emptyIterator();
        }

        if (this.hasIds()) {
            return TraversalUtil.filterResult(this.hasContainers,
                                              graph.vertices(this.ids));
        }

        Query query = this.makeQuery(graph, HugeType.VERTEX);
        Iterator<E> result;
        return (Iterator<E>) graph.vertices(query);
    }

    @SuppressWarnings("unchecked")
    private Iterator<E> edges() {
        LOG.debug("HugeGraphStep.edges(): {}", this);

        HugeGraph graph = TraversalUtil.getGraph(this);

        // g.E().hasId(EMPTY_LIST) will set ids to null
        if (this.ids == null) {
            return QueryResults.emptyIterator();
        }

        if (this.hasIds()) {
            return TraversalUtil.filterResult(this.hasContainers,
                                              graph.edges(this.ids));
        }

        Query query = this.makeQuery(graph, HugeType.EDGE);

        return (Iterator<E>) graph.edges(query);
    }

    private boolean hasIds() {
        return this.ids != null && this.ids.length > 0;
    }

    private Query makeQuery(HugeGraph graph, HugeType type) {
        Query query = null;
        if (this.hasContainers.isEmpty()) {
            // Query all
            query = new Query(type);
        } else {
            ConditionQuery q = new ConditionQuery(type);
            query = TraversalUtil.fillConditionQuery(q, this.hasContainers, graph);
        }

        query = this.injectQueryInfo(query);
        return query;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }

        return this.ids.length == 0 ?
               StringFactory.stepString(this,
                                        this.returnClass.getSimpleName(),
                                        this.hasContainers) :
               StringFactory.stepString(this,
                                        this.returnClass.getSimpleName(),
                                        Arrays.toString(this.ids),
                                        this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer has) {
        if (SYSPROP_PAGE.equals(has.getKey())) {
            this.setPage((String) has.getValue());
            return;
        }
        this.hasContainers.add(has);
    }

    @Override
    public Query queryInfo() {
        return this.queryInfo;
    }

    @Override
    public Iterator<?> lastTimeResults() {
        return this.lastTimeResults;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^
               this.queryInfo.hashCode() ^
               this.hasContainers.hashCode();
    }
}
