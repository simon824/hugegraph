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

package org.apache.hugegraph.backend.cache;

import static org.apache.hugegraph.type.define.Directions.BOTH;
import static org.apache.hugegraph.type.define.Directions.OUT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.vgraph.VirtualEdgeStatus;
import org.apache.hugegraph.vgraph.VirtualGraph;
import org.apache.hugegraph.vgraph.VirtualVertexStatus;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public final class VirtualGraphTransaction extends GraphTransaction {

    private final VirtualGraph vGraph;

    public VirtualGraphTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        this.vGraph = graph.vGraph();
    }

    @Override
    public Iterator<Vertex> adjacentVertexWithProp(Object... ids) {
        return this.queryVerticesByIds(ids, true,
                                       this.checkAdjacentVertexExist,
                                       query -> this.queryVerticesFromBackend(query,
                                                                              VirtualVertexStatus.Property),
                                       HugeType.VERTEX);
    }

    @Override
    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        return this.queryVerticesFromBackend(query, VirtualVertexStatus.Id);
    }

    private Iterator<HugeVertex> queryVerticesFromBackend(Query query, VirtualVertexStatus status) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVerticesByIds((IdQuery) query, status);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    private Iterator<HugeVertex> queryVerticesByIds(IdQuery query, VirtualVertexStatus status) {
        List<Id> vIds = new ArrayList<>(query.ids());
        return this.vGraph.queryHugeVertexByIds(vIds, status);
    }

    @Override
    @Watched
    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        return this.queryEdgesFromBackend(query, VirtualEdgeStatus.Id);
    }

    private Iterator<HugeEdge> queryEdgesFromBackend(Query query, VirtualEdgeStatus status) {
        if (query.empty() || query.paging() || query.bigCapacity()) {
            // Query all edges or query edges in paging, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        List<HugeEdge> edges = new ArrayList<>();
        Query newQuery = query;
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            // Query from vGraph
            newQuery = queryEdgesFromVirtualGraphByEIds(query, edges, status);
        } else if (!query.conditions().isEmpty()) {
            newQuery = queryEdgesFromVirtualGraph(query, edges);
        }

        if (newQuery == null) {
            return edges.iterator();
        }

        ExtendableIterator<HugeEdge> results = new ExtendableIterator<>();
        if (!edges.isEmpty()) {
            results.extend(edges.iterator());
        }

        if (!newQuery.empty()) {

            Iterator<HugeEdge> rs = super.queryEdgesFromBackend(newQuery);

            List<HugeEdge> edgesRS = new ArrayList<>();
            rs.forEachRemaining(edgesRS::add);
            if (!edgesRS.isEmpty()) {
                putEdgesToVirtualGraph(newQuery, edgesRS.listIterator());
                results.extend(edgesRS.listIterator());
            }
        }
        return results;
    }

    private void putEdgesToVirtualGraph(Query query, Iterator<HugeEdge> edges) {
        if (!edges.hasNext()) {
            return;
        }
        if (query instanceof ConditionQuery) {
            ConditionQuery conditionQuery = (ConditionQuery) query;
            Id vId = conditionQuery.condition(HugeKeys.OWNER_VERTEX);
            if (vId != null) {
                HugeVertex vertex = null;
                List<HugeEdge> outEdges = new ArrayList<>();
                List<HugeEdge> inEdges = new ArrayList<>();
                while (edges.hasNext()) {
                    HugeEdge e = edges.next();
                    if (e.direction().equals(OUT)) {
                        if (vertex == null) {
                            vertex = e.ownerVertex();
                        }
                        outEdges.add(e);
                    } else {
                        inEdges.add(e);
                    }
                }
                if (vertex == null) {
                    // vertex has no out-edge
                    vertex = inEdges.get(0).targetVertex();
                    // vertex.resetEdges();
                }
                this.vGraph.putVertex(vertex, outEdges, inEdges.iterator());
            }
        } else {
            this.vGraph.putEdges(edges);
        }
    }

    private Query queryEdgesFromVirtualGraphByEIds(Query query, List<HugeEdge> edges,
                                                   VirtualEdgeStatus status) {
        List<Id> eIds = new ArrayList<>(query.ids());
        Iterator<HugeEdge> edgesFromVGraph = this.vGraph.queryEdgeByIds(eIds, status);
        edgesFromVGraph.forEachRemaining(edges::add);
        return null;
    }

    private Query queryEdgesFromVirtualGraph(Query query, List<HugeEdge> results) {
        if (query instanceof ConditionQuery) {
            ConditionQuery conditionQuery = (ConditionQuery) query;
            Id vId = conditionQuery.condition(HugeKeys.OWNER_VERTEX);
            if (vId != null) {
                this.getQueryEdgesFromVirtualGraph(vId, conditionQuery, results);
                if (results.size() <= 0) {
                    // query all edges of this vertex from backend
                    ConditionQuery vertexAllEdgeQuery = constructEdgesQuery(vId, BOTH);
                    vertexAllEdgeQuery.capacity(query.capacity());
                    vertexAllEdgeQuery.limit(query.limit());
                    Iterator<HugeEdge> allEdges = super.queryEdgesFromBackend(vertexAllEdgeQuery);
                    List<HugeEdge> allEdgeList = new ArrayList<>();
                    allEdges.forEachRemaining(e -> {
                        allEdgeList.add(e);
                        if (query.test(e)) {
                            results.add(e);
                        }
                    });
                    if (query.limit() == Query.NO_LIMIT || allEdgeList.size() < query.limit()) {
                        // got all edges of this vertex
                        putEdgesToVirtualGraph(vertexAllEdgeQuery, allEdgeList.listIterator());
                    }
                }
                return null;
            }
        }

        return query;
    }

    private void getQueryEdgesFromVirtualGraph(Id vId, ConditionQuery query,
                                               List<HugeEdge> results) {
        Iterator<HugeEdge> edges =
                this.vGraph.queryEdgesByVertexId(vId, getVVStatusFromQuery(query));
        if (edges != null) {
            edges.forEachRemaining(e -> {
                if (query.test(e)) {
                    results.add(e);
                }
            });
        }
    }

    private VirtualVertexStatus getVVStatusFromQuery(ConditionQuery query) {
        Directions direction = query.condition(HugeKeys.DIRECTION);
        if (direction != null) {
            switch (direction) {
                case OUT:
                    return VirtualVertexStatus.OutEdge;
                case IN:
                    return VirtualVertexStatus.InEdge;
                case BOTH:
                    return VirtualVertexStatus.AllEdge;
                default:
                    throw new IllegalStateException("Unexpected value: " + direction);
            }
        }
        return VirtualVertexStatus.AllEdge;
    }

    @Override
    protected void commitMutation2Backend(BackendMutation... mutations) {
        // Collect changes before commit
        Collection<HugeVertex> updates = this.verticesInTxUpdated();
        Collection<HugeVertex> deletions = this.verticesInTxRemoved();
        Collection<HugeEdge> updatesE = this.edgesInTxUpdated();
        Collection<HugeEdge> deletionsE = this.edgesInTxRemoved();

        try {
            super.commitMutation2Backend(mutations);

            // Update vertex cache
            for (HugeVertex vertex : updates) {
                this.vGraph.updateIfPresentVertex(vertex, null);
            }
            // Update edge cache
            this.vGraph.updateIfPresentEdge(updatesE.iterator());

        } finally {
            // invalidate removed vertex in cache whatever success or fail
            int vertexOffset = 0;
            int edgeOffset = 0;
            Id[] vertexIdsDeleted = new Id[deletions.size()];
            Id[] edgeIdsDeleted = new Id[deletionsE.size()];

            for (HugeVertex vertex : deletions) {
                vertexIdsDeleted[vertexOffset++] = vertex.id();
            }
            for (HugeEdge edge : deletionsE) {
                edgeIdsDeleted[edgeOffset++] = edge.id();
            }
            if (vertexOffset > 0) {
                this.vGraph.notifyChanges(Cache.ACTION_INVALID,
                                          HugeType.VERTEX, vertexIdsDeleted);
            }
            if (edgeOffset > 0) {
                this.vGraph.notifyChanges(Cache.ACTION_INVALID,
                                          HugeType.EDGE, edgeIdsDeleted);
            }
        }
    }
}
