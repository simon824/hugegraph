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

package org.apache.hugegraph.backend.tx;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.SplicingIdGenerator;
import org.apache.hugegraph.backend.page.IdHolderList;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.page.QueryList;
import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.ConditionQueryFlatten;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.MatchedIndex;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.ErrorCode;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.iterator.BatchMapperIterator;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.iterator.FlatMapperIterator;
import org.apache.hugegraph.iterator.LimitIterator;
import org.apache.hugegraph.iterator.ListIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.job.system.DeleteExpiredJob;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeFeatures.HugeVertexFeatures;
import org.apache.hugegraph.structure.HugeIndex;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Action;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.LockUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import jakarta.ws.rs.ForbiddenException;

public class GraphTransaction extends IndexableTransaction implements AutoCloseable {

    public static final int COMMIT_BATCH = (int) Query.COMMIT_BATCH;
    protected final boolean checkAdjacentVertexExist;
    private final GraphIndexTransaction indexTx;
    private final LockUtil.LocksTable locksTable;
    private final boolean checkCustomVertexExist;
    private final boolean lazyLoadAdjacentVertex;
    private final boolean removeLeftIndexOnOverwrite;
    private final boolean ignoreInvalidEntry;
    private final boolean optimizeAggrByIndex;
    private final int commitPartOfAdjacentEdges;
    private final int batchSize;
    private final int pageSize;
    private final int verticesCapacity;
    private final int edgesCapacity;
    private Map<Id, HugeVertex> addedVertices;
    private Map<Id, HugeVertex> removedVertices;
    private Map<Id, HugeEdge> addedEdges;
    private Map<Id, HugeEdge> removedEdges;
    private Set<HugeProperty<?>> addedProps;
    private Set<HugeProperty<?>> removedProps;
    // These are used to rollback state
    private Map<Id, HugeVertex> updatedVertices;
    private Map<Id, HugeEdge> updatedEdges;
    private Set<HugeProperty<?>> updatedOldestProps; // Oldest props

    public GraphTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        this.indexTx = new GraphIndexTransaction(graph, store);
        assert !this.indexTx.autoCommit();
        String spaceGraph = graph.graph().spaceGraphName();
        this.locksTable = new LockUtil.LocksTable(spaceGraph);

        final HugeConfig conf = graph.configuration();
        this.checkCustomVertexExist =
                conf.get(CoreOptions.VERTEX_CHECK_CUSTOMIZED_ID_EXIST);
        this.checkAdjacentVertexExist =
                conf.get(CoreOptions.VERTEX_ADJACENT_VERTEX_EXIST);
        this.lazyLoadAdjacentVertex =
                conf.get(CoreOptions.VERTEX_ADJACENT_VERTEX_LAZY);
        this.removeLeftIndexOnOverwrite =
                conf.get(CoreOptions.VERTEX_REMOVE_LEFT_INDEX);
        this.commitPartOfAdjacentEdges =
                conf.get(CoreOptions.VERTEX_PART_EDGE_COMMIT_SIZE);
        this.ignoreInvalidEntry =
                conf.get(CoreOptions.QUERY_IGNORE_INVALID_DATA);
        this.optimizeAggrByIndex =
                conf.get(CoreOptions.QUERY_OPTIMIZE_AGGR_BY_INDEX);
        this.batchSize = conf.get(CoreOptions.QUERY_BATCH_SIZE);
        this.pageSize = conf.get(CoreOptions.QUERY_PAGE_SIZE);

        this.verticesCapacity = conf.get(CoreOptions.VERTEX_TX_CAPACITY);
        this.edgesCapacity = conf.get(CoreOptions.EDGE_TX_CAPACITY);

        E.checkArgument(this.commitPartOfAdjacentEdges < this.edgesCapacity,
                        "Option value of %s(%s) must be < %s(%s)",
                        CoreOptions.VERTEX_PART_EDGE_COMMIT_SIZE.name(),
                        this.commitPartOfAdjacentEdges,
                        CoreOptions.EDGE_TX_CAPACITY.name(),
                        this.edgesCapacity);
    }

    /**
     * Construct one edge condition query based on source vertex, direction and
     * edge labels
     *
     * @param sourceVertex source vertex of edge
     * @param direction    only be "IN", "OUT" or "BOTH"
     * @param edgeLabels   edge labels of queried edges
     * @return constructed condition query
     */
    @Watched
    public static ConditionQuery constructEdgesQuery(Id sourceVertex,
                                                     Directions direction,
                                                     Id... edgeLabels) {
        E.checkState(direction != null,
                     "The edge query must contain direction");

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // Edge source vertex
        if (sourceVertex != null) {
            query.eq(HugeKeys.OWNER_VERTEX, sourceVertex);
        }
        // Edge direction
        if (direction == Directions.BOTH) {
            query.query(Condition.or(
                    Condition.eq(HugeKeys.DIRECTION, Directions.OUT),
                    Condition.eq(HugeKeys.DIRECTION, Directions.IN)));
        } else {
            assert direction == Directions.OUT || direction == Directions.IN;
            query.eq(HugeKeys.DIRECTION, direction);
        }

        if (edgeLabels != null && edgeLabels.length > 0) {
            // Edge labels
            if (edgeLabels.length == 1 && edgeLabels[0] != null) {
                query.eq(HugeKeys.LABEL, edgeLabels[0]);
            } else if (edgeLabels.length > 1) {
                query.query(Condition.in(HugeKeys.LABEL,
                                         Arrays.asList(edgeLabels)));
            }
        }

        return query;
    }

    /*
     * replace old function
     * should remove
     *  public static ConditionQuery constructEdgesQuery(Id sourceVertex,
     *                                                   Directions direction,
     *                                                   Id... edgeLabels)
     * after test
     * */
    public static ConditionQuery constructEdgesQuery(Id sourceVertex,
                                                     Directions direction,
                                                     List<Id> edgeLabels) {
        E.checkState(direction != null,
                     "The edge query must contain direction");

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // Edge source vertex
        if (sourceVertex != null) {
            query.eq(HugeKeys.OWNER_VERTEX, sourceVertex);
        }
        // Edge direction
        if (direction == Directions.BOTH) {
            query.query(Condition.or(
                    Condition.eq(HugeKeys.DIRECTION, Directions.OUT),
                    Condition.eq(HugeKeys.DIRECTION, Directions.IN)));
        } else {
            assert direction == Directions.OUT || direction == Directions.IN;
            query.eq(HugeKeys.DIRECTION, direction);
        }

        if (edgeLabels != null && edgeLabels.size() > 0) {
            // Edge labels
            if (edgeLabels.size() == 1) {
                query.eq(HugeKeys.LABEL, edgeLabels.get(0));
            } else if (edgeLabels.size() > 1) {
                query.query(Condition.in(HugeKeys.LABEL, edgeLabels));
            }
        }

        return query;
    }

    /**
     * Construct one edge condition query based on source vertex, direction and
     * edge labels
     * constructEdgesQuery2 is to construct Query based on the complete
     * Edgelabel, because the parent-child Edgelabel function requires
     * complete Edgelabel information
     *
     * @param sourceVertex source vertex of edge
     * @param direction    only be "IN", "OUT" or "BOTH"
     * @param edgeLabels   edge labels of queried edges
     * @return constructed condition query
     */
    @Watched
    public static ConditionQuery constructEdgesQuery2(Id sourceVertex,
                                                      Directions direction,
                                                      List<Id> edgeLabels,
                                                      HugeGraph graph) {
        E.checkState(direction != null,
                     "The edge query must contain direction");

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // Edge source vertex
        if (sourceVertex != null) {
            query.eq(HugeKeys.OWNER_VERTEX, sourceVertex);
        }
        // Edge direction
        if (direction == Directions.BOTH) {
            query.query(Condition.or(
                    Condition.eq(HugeKeys.DIRECTION, Directions.OUT),
                    Condition.eq(HugeKeys.DIRECTION, Directions.IN)));
        } else {
            assert direction == Directions.OUT || direction == Directions.IN;
            query.eq(HugeKeys.DIRECTION, direction);
        }

        // Edge labels
        if (edgeLabels == null) {
            return query;
        }
        if (edgeLabels.size() == 1 && graph != null) {
            // If it is a subquery, encapsulate the parent EdgeLabelId, SourceVertexLabelID
            // and TargetVertexLabelID into the query
            EdgeLabel edgeLabel = graph.schemaTransaction()
                                       .getEdgeLabel(edgeLabels.get(0));
            if (edgeLabel.hasFather()) {
                query.eq(HugeKeys.LABEL, edgeLabel.fatherId());
                query.eq(HugeKeys.SUB_LABEL, edgeLabel.id());
            } else {
                query.eq(HugeKeys.LABEL, edgeLabel.id());
                // query.eq(HugeKeys.SUB_LABEL, edgeLabels[0].id());
            }
        } else if (edgeLabels.size() >= 1) {
            query.query(Condition.in(HugeKeys.LABEL, edgeLabels));
        }
        return query;
    }

    public static boolean matchFullEdgeSortKeys(ConditionQuery query,
                                                HugeGraph graph) {
        // All queryKeys in sortKeys
        return matchEdgeSortKeys(query, true, graph);
    }

    public static boolean matchPartialEdgeSortKeys(ConditionQuery query,
                                                   HugeGraph graph) {
        // Partial queryKeys in sortKeys
        return matchEdgeSortKeys(query, false, graph);
    }

    private static boolean matchEdgeSortKeys(ConditionQuery query,
                                             boolean matchAll,
                                             HugeGraph graph) {
        assert query.resultType().isEdge();
        Id label = query.condition(HugeKeys.LABEL);
        if (label == null) {
            return false;
        }
        List<Id> sortKeys = graph.edgeLabel(label).sortKeys();
        if (sortKeys.isEmpty()) {
            return false;
        }
        Set<Id> queryKeys = query.userpropKeys();
        for (int i = sortKeys.size(); i > 0; i--) {
            List<Id> subFields = sortKeys.subList(0, i);
            if (queryKeys.containsAll(subFields)) {
                if (queryKeys.size() == subFields.size() || !matchAll) {
                    /*
                     * Return true if:
                     * matchAll=true and all queryKeys are in sortKeys
                     *  or
                     * partial queryKeys are in sortKeys
                     */
                    return true;
                }
            }
        }
        return false;
    }

    private static void verifyVerticesConditionQuery(ConditionQuery query) {
        assert query.resultType().isVertex();

        int total = query.conditions().size();
        if (total == 1) {
            /*
             * Supported query:
             *  1.query just by vertex label
             *  2.query just by PROPERTIES (like containsKey,containsValue)
             *  3.query with scan
             */
            if (query.containsCondition(HugeKeys.LABEL) ||
                query.containsCondition(HugeKeys.PROPERTIES) ||
                query.containsScanCondition()) {
                return;
            }
        }

        int matched = 0;
        if (query.containsCondition(HugeKeys.PROPERTIES)) {
            matched++;
            if (query.containsCondition(HugeKeys.LABEL)) {
                matched++;
            }
        }

        if (matched != total) {
            throw new HugeException("Not supported querying vertices by %s",
                                    query.conditions());
        }
    }

    private static void verifyEdgesConditionQuery(ConditionQuery query) {
        assert query.resultType().isEdge();

        int total = query.conditions().size();
        if (total == 1) {
            /*
             * Supported query:
             *  1.query just by edge label
             *  2.query just by PROPERTIES (like containsKey,containsValue)
             *  3.query with scan
             */
            if (query.containsCondition(HugeKeys.LABEL) ||
                query.containsCondition(HugeKeys.PROPERTIES) ||
                query.containsScanCondition()) {
                return;
            }
        }

        int matched = 0;
        for (HugeKeys key : EdgeId.KEYS) {
            Object value = query.condition(key);
            if (value == null) {
                break;
            }
            matched++;
        }
        int count = matched;

        if (query.containsCondition(HugeKeys.PROPERTIES)) {
            matched++;
            if (count < 3 && query.containsCondition(HugeKeys.LABEL)) {
                matched++;
            }
        }

        if (matched != total) {
            throw new HugeException(
                    "Not supported querying edges by %s, expect %s",
                    query.conditions(), EdgeId.KEYS[count]);
        }
    }

    @Override
    public boolean hasUpdate() {
        return this.mutationSize() > 0 || super.hasUpdate();
    }

    @Override
    public boolean hasUpdate(HugeType type, Action action) {
        if (type.isVertex()) {
            if (action == Action.DELETE) {
                if (this.removedVertices.size() > 0) {
                    return true;
                }
            } else {
                if (this.addedVertices.size() > 0 ||
                    this.updatedVertices.size() > 0) {
                    return true;
                }
            }
        } else if (type.isEdge()) {
            if (action == Action.DELETE) {
                if (this.removedEdges.size() > 0) {
                    return true;
                }
            } else {
                if (this.addedEdges.size() > 0 ||
                    this.updatedEdges.size() > 0) {
                    return true;
                }
            }
        }
        return super.hasUpdate(type, action);
    }

    @Override
    public int mutationSize() {
        return this.verticesInTxSize() + this.edgesInTxSize();
    }

    public boolean checkAdjacentVertexExist() {
        return this.checkAdjacentVertexExist;
    }

    @Override
    protected void reset() {
        super.reset();

        // Clear mutation
        if (this.addedVertices == null || !this.addedVertices.isEmpty()) {
            this.addedVertices = InsertionOrderUtil.newMap();
        }
        if (this.removedVertices == null || !this.removedVertices.isEmpty()) {
            this.removedVertices = InsertionOrderUtil.newMap();
        }
        if (this.updatedVertices == null || !this.updatedVertices.isEmpty()) {
            this.updatedVertices = InsertionOrderUtil.newMap();
        }

        if (this.addedEdges == null || !this.addedEdges.isEmpty()) {
            this.addedEdges = InsertionOrderUtil.newMap();
        }
        if (this.removedEdges == null || !this.removedEdges.isEmpty()) {
            this.removedEdges = InsertionOrderUtil.newMap();
        }
        if (this.updatedEdges == null || !this.updatedEdges.isEmpty()) {
            this.updatedEdges = InsertionOrderUtil.newMap();
        }

        if (this.addedProps == null || !this.addedProps.isEmpty()) {
            this.addedProps = InsertionOrderUtil.newSet();
        }
        if (this.removedProps == null || !this.removedProps.isEmpty()) {
            this.removedProps = InsertionOrderUtil.newSet();
        }
        if (this.updatedOldestProps == null ||
            !this.updatedOldestProps.isEmpty()) {
            this.updatedOldestProps = InsertionOrderUtil.newSet();
        }
    }

    @Override
    protected GraphIndexTransaction indexTransaction() {
        return this.indexTx;
    }

    @Override
    protected void beforeWrite() {
        this.checkTxVerticesCapacity();
        this.checkTxEdgesCapacity();

        super.beforeWrite();
    }

    protected final int verticesInTxSize() {
        return this.addedVertices.size() +
               this.removedVertices.size() +
               this.updatedVertices.size();
    }

    protected final int edgesInTxSize() {
        return this.addedEdges.size() +
               this.removedEdges.size() +
               this.updatedEdges.size();
    }

    protected final Collection<HugeVertex> verticesInTxUpdated() {
        int size = this.addedVertices.size() + this.updatedVertices.size();
        List<HugeVertex> vertices = new ArrayList<>(size);
        vertices.addAll(this.addedVertices.values());
        vertices.addAll(this.updatedVertices.values());
        return vertices;
    }

    protected final Collection<HugeVertex> verticesInTxRemoved() {
        return new ArrayList<>(this.removedVertices.values());
    }

    protected final Collection<HugeEdge> edgesInTxUpdated() {
        int size = this.addedEdges.size() + this.updatedEdges.size();
        List<HugeEdge> edges = new ArrayList<>(size);
        edges.addAll(this.addedEdges.values());
        edges.addAll(this.updatedEdges.values());
        return edges;
    }

    protected final Collection<HugeEdge> edgesInTxRemoved() {
        return new ArrayList<>(this.removedEdges.values());
    }

    protected final boolean removingEdgeOwner(HugeEdge edge) {
        for (HugeVertex vertex : this.removedVertices.values()) {
            if (edge.belongToVertex(vertex)) {
                return true;
            }
        }
        return false;
    }

    @Watched(prefix = "tx")
    @Override
    protected BackendMutation prepareCommit() {
        // Serialize and add updates into super.deletions
        if (this.removedVertices.size() > 0 || this.removedEdges.size() > 0) {
            this.prepareDeletions(this.removedVertices, this.removedEdges);
        }

        if (this.addedProps.size() > 0 || this.removedProps.size() > 0) {
            this.prepareUpdates(this.addedProps, this.removedProps);
        }

        // Serialize and add updates into super.additions
        if (this.addedVertices.size() > 0 || this.addedEdges.size() > 0) {
            this.prepareAdditions(this.addedVertices, this.addedEdges);
        }

        return this.mutation();
    }

    protected void prepareAdditions(Map<Id, HugeVertex> addedVertices,
                                    Map<Id, HugeEdge> addedEdges) {
        if (this.checkCustomVertexExist) {
            this.checkVertexExistIfCustomizedId(addedVertices);
        }

        if (this.removeLeftIndexOnOverwrite) {
            this.removeLeftIndexIfNeeded(addedVertices);
        }

        // Do vertex update
        for (HugeVertex v : addedVertices.values()) {
            assert !v.removed();
            v.committed();
            this.checkAggregateProperty(v);
            // Check whether passed all non-null properties
            if (!this.graphMode().loading()) {
                this.checkNonnullProperty(v);
            }

            // Add vertex entry
            this.doInsert(this.serializer.writeVertex(v));
            // Update index of vertex(only include props)
            this.indexTx.updateVertexIndex(v, false);
            this.indexTx.updateLabelIndex(v, false);
        }

        // Do edge update
        for (HugeEdge e : addedEdges.values()) {
            assert !e.removed();
            e.committed();
            // Skip edge if its owner has been removed
            if (this.removingEdgeOwner(e)) {
                continue;
            }
            this.checkAggregateProperty(e);
            // Add edge entry of OUT and IN
            this.doInsert(this.serializer.writeEdge(e));
            this.doInsert(this.serializer.writeEdge(e.switchOwner()));
            // Update index of edge
            this.indexTx.updateEdgeIndex(e, false);
            this.indexTx.updateLabelIndex(e, false);
        }
    }

    protected void prepareDeletions(Map<Id, HugeVertex> removedVertices,
                                    Map<Id, HugeEdge> removedEdges) {
        // Remove related edges of each vertex
        for (HugeVertex v : removedVertices.values()) {
            if (!v.schemaLabel().existsLinkLabel()) {
                continue;
            }
            // Query all edges of the vertex and remove them
            Query query = constructEdgesQuery(v.id(), Directions.BOTH);
            Iterator<HugeEdge> vedges = this.queryEdgesFromBackend(query);
            try {
                while (vedges.hasNext()) {
                    this.checkTxEdgesCapacity();
                    HugeEdge edge = vedges.next();
                    // NOTE: will change the input parameter
                    removedEdges.put(edge.id(), edge);
                    // Commit first if enabled commit-part mode
                    if (this.commitPartOfAdjacentEdges > 0 &&
                        removedEdges.size() >= this.commitPartOfAdjacentEdges) {
                        this.commitPartOfEdgeDeletions(removedEdges);
                    }
                }
            } finally {
                CloseableIterator.closeIterator(vedges);
            }
        }

        // Remove vertices
        for (HugeVertex v : removedVertices.values()) {
            this.checkAggregateProperty(v);
            /*
             * If the backend stores vertex together with edges, it's edges
             * would be removed after removing vertex. Otherwise, if the
             * backend stores vertex which is separated from edges, it's
             * edges should be removed manually when removing vertex.
             */
            this.doRemove(this.serializer.writeVertex(v.prepareRemoved()));
            this.indexTx.updateVertexIndex(v, true);
            this.indexTx.updateLabelIndex(v, true);
        }

        // Remove edges
        this.prepareDeletions(removedEdges);
    }

    protected void prepareDeletions(Map<Id, HugeEdge> removedEdges) {
        // Remove edges
        for (HugeEdge e : removedEdges.values()) {
            this.checkAggregateProperty(e);
            // Update edge index
            this.indexTx.updateEdgeIndex(e, true);
            this.indexTx.updateLabelIndex(e, true);
            // Remove edge of OUT and IN
            e = e.prepareRemoved();
            this.doRemove(this.serializer.writeEdge(e));
            this.doRemove(this.serializer.writeEdge(e.switchOwner()));
        }
    }

    protected void prepareUpdates(Set<HugeProperty<?>> addedProps,
                                  Set<HugeProperty<?>> removedProps) {
        for (HugeProperty<?> p : removedProps) {
            this.checkAggregateProperty(p);
            if (p.element().type().isVertex()) {
                HugeVertexProperty<?> prop = (HugeVertexProperty<?>) p;
                if (this.store().features().supportsUpdateVertexProperty()) {
                    // Update vertex index without removed property
                    this.indexTx.updateVertexIndex(prop.element(), false);
                    // Eliminate the property(OUT and IN owner edge)
                    this.doEliminate(this.serializer.writeVertexProperty(prop));
                } else {
                    // Override vertex
                    this.addVertex(prop.element());
                }
            } else {
                assert p.element().type().isEdge();
                HugeEdgeProperty<?> prop = (HugeEdgeProperty<?>) p;
                if (this.store().features().supportsUpdateEdgeProperty()) {
                    // Update edge index without removed property
                    this.indexTx.updateEdgeIndex(prop.element(), false);
                    // Eliminate the property(OUT and IN owner edge)
                    this.doEliminate(this.serializer.writeEdgeProperty(prop));
                    this.doEliminate(this.serializer.writeEdgeProperty(
                            prop.switchEdgeOwner()));
                } else {
                    // Override edge(it will be in addedEdges & updatedEdges)
                    this.addEdge(prop.element());
                }
            }
        }
        for (HugeProperty<?> p : addedProps) {
            this.checkAggregateProperty(p);
            if (p.element().type().isVertex()) {
                HugeVertexProperty<?> prop = (HugeVertexProperty<?>) p;
                if (this.store().features().supportsUpdateVertexProperty()) {
                    // Update vertex index with new added property
                    this.indexTx.updateVertexIndex(prop.element(), false);
                    // Append new property(OUT and IN owner edge)
                    this.doAppend(this.serializer.writeVertexProperty(prop));
                } else {
                    // Override vertex
                    this.addVertex(prop.element());
                }
            } else {
                assert p.element().type().isEdge();
                HugeEdgeProperty<?> prop = (HugeEdgeProperty<?>) p;
                if (this.store().features().supportsUpdateEdgeProperty()) {
                    // Update edge index with new added property
                    this.indexTx.updateEdgeIndex(prop.element(), false);
                    // Append new property(OUT and IN owner edge)
                    this.doAppend(this.serializer.writeEdgeProperty(prop));
                    this.doAppend(this.serializer.writeEdgeProperty(
                            prop.switchEdgeOwner()));
                } else {
                    // Override edge (it will be in addedEdges & updatedEdges)
                    this.addEdge(prop.element());
                }
            }
        }
    }

    private void commitPartOfEdgeDeletions(Map<Id, HugeEdge> removedEdges) {
        assert this.commitPartOfAdjacentEdges > 0;

        this.prepareDeletions(removedEdges);

        BackendMutation mutation = this.mutation();
        BackendMutation idxMutation = this.indexTransaction().mutation();

        try {
            this.commitMutation2Backend(mutation, idxMutation);
        } catch (Throwable e) {
            this.rollbackBackend();
        } finally {
            mutation.clear();
            idxMutation.clear();
        }

        removedEdges.clear();
    }

    @Override
    public void commit() throws BackendException {
        try {
            super.commit();
        } finally {
            this.locksTable.unlock();
        }
    }

    @Override
    public void rollback() throws BackendException {
        // Rollback properties changes
        for (HugeProperty<?> prop : this.updatedOldestProps) {
            prop.element().setProperty(prop);
        }
        try {
            super.rollback();
        } finally {
            this.locksTable.unlock();
        }
    }

    @Override
    protected QueryResults<BackendEntry> query(Query query) {
        if (!(query instanceof ConditionQuery)) {
            LOG.debug("Query{final:{}}", query);
            return super.query(query);
        }
        QueryList<BackendEntry> queries = this.optimizeQueries(query,
                                                               super::query);
        LOG.debug("{}", queries);
        return queries.empty() ? QueryResults.empty() :
               queries.fetch(this.pageSize);
    }

    @Override
    public Number queryNumber(Query query) {
        E.checkArgument(!this.hasUpdate(),
                        "It's not allowed to query number when " +
                        "there are uncommitted records.");

        if (!(query instanceof ConditionQuery)) {
            return super.queryNumber(query);
        }

        Aggregate aggregate = query.aggregateNotNull();

        QueryList<Number> queries = this.optimizeQueries(query, q -> {
            boolean indexQuery = q.getClass() == IdQuery.class;
            OptimizedType optimized = ((ConditionQuery) query).optimized();
            Number result;
            if (!indexQuery) {
                result = super.queryNumber(q);
            } else {
                E.checkArgument(aggregate.func() == AggregateFunc.COUNT,
                                "The %s operator on index is not supported now",
                                aggregate.func().string());
                if (this.optimizeAggrByIndex &&
                    optimized == OptimizedType.INDEX) {
                    // The ids size means results count (assume no left index)
                    result = q.ids().size();
                } else {
                    assert optimized == OptimizedType.INDEX_FILTER ||
                           optimized == OptimizedType.INDEX;
                    assert q.resultType().isVertex() || q.resultType().isEdge();
                    Iterator<? extends Element> queryResult =
                            q.resultType().isVertex() ? this.queryVertices(q) :
                            this.queryEdges(q);
                    try {
                        result = IteratorUtils.count(queryResult);
                    } finally {
                        CloseableIterator.closeIterator(queryResult);
                    }
                }
            }
            return new QueryResults<>(IteratorUtils.of(result), q);
        });

        QueryResults<Number> results = queries.empty() ?
                                       QueryResults.empty() :
                                       queries.fetch(this.pageSize);
        return aggregate.reduce(results.iterator());
    }

    @Watched(prefix = "graph")
    public HugeVertex addVertex(Object... keyValues) {
        return this.addVertex(this.constructVertex(true, keyValues));
    }

    @Watched("graph.addVertex-instance")
    public HugeVertex addVertex(HugeVertex vertex) {
        this.checkOwnerThread();
        assert !vertex.removed();

        // Override vertices in local `removedVertices`
        this.removedVertices.remove(vertex.id());
        try {
            this.locksTable.lockReads(LockUtil.VERTEX_LABEL_DELETE,
                                      vertex.schemaLabel().id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE,
                                      vertex.schemaLabel().indexLabels());
            // Ensure vertex label still exists from vertex-construct to lock
            this.graph().vertexLabel(vertex.schemaLabel().id());
            /*
             * No need to lock VERTEX_LABEL_ADD_UPDATE, because vertex label
             * update only can add nullable properties and user data, which is
             * unconcerned with add vertex
             */
            this.beforeWrite();
            this.addedVertices.put(vertex.id(), vertex);
            this.afterWrite();
        } catch (Throwable e) {
            this.locksTable.unlock();
            throw e;
        }
        return vertex;
    }

    @Watched(prefix = "graph")
    public HugeVertex constructVertex(boolean verifyVL, Object... keyValues) {
        HugeElement.ElementKeys elemKeys = HugeElement.classifyKeys(keyValues);
        if (possibleOlapVertex(elemKeys)) {
            Id id = HugeVertex.getIdValue(elemKeys.id());
            HugeVertex vertex = HugeVertex.create(this, id,
                                                  VertexLabel.OLAP_VL);
            ElementHelper.attachProperties(vertex, keyValues);
            Iterator<HugeProperty<?>> iterator = vertex.getProperties().values()
                                                       .iterator();
            assert iterator.hasNext();
            if (iterator.next().propertyKey().olap()) {
                return vertex;
            }
        }
        VertexLabel vertexLabel = this.checkVertexLabel(elemKeys.label(),
                                                        verifyVL);
        Id id = HugeVertex.getIdValue(elemKeys.id());
        List<Id> keys = this.graph().mapPkName2Id(elemKeys.keys());

        // Check whether id match with id strategy
        this.checkId(id, keys, vertexLabel);

        // Create HugeVertex
        HugeVertex vertex = HugeVertex.create(this, null, vertexLabel);

        // Set properties
        ElementHelper.attachProperties(vertex, keyValues);

        // Assign vertex id
        if (this.params().mode().maintaining() &&
            vertexLabel.idStrategy() == IdStrategy.AUTOMATIC) {
            // Resume id for AUTOMATIC id strategy in restoring mode
            vertex.assignId(id, true);
        } else {
            vertex.assignId(id);
        }

        return vertex;
    }

    private boolean possibleOlapVertex(HugeElement.ElementKeys elemKeys) {
        return elemKeys.id() != null && elemKeys.label() == null &&
               elemKeys.keys().size() == 1;
    }

    @Watched(prefix = "graph")
    public void removeVertex(HugeVertex vertex) {
        this.checkOwnerThread();

        this.beforeWrite();

        // Override vertices in local `addedVertices`
        this.addedVertices.remove(vertex.id());

        // Collect the removed vertex
        this.removedVertices.put(vertex.id(), vertex);

        this.afterWrite();
    }

    public Iterator<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        if (this.lazyLoadAdjacentVertex) {
            return new MapperIterator<>(edges, edge -> ((HugeEdge) edge).otherVertex());
        }
        return new BatchMapperIterator<>(this.batchSize, edges, batchEdges -> {
            List<Id> vertexIds = new ArrayList<>();
            for (Edge edge : batchEdges) {
                vertexIds.add(((HugeEdge) edge).otherVertex().id());
            }
            assert vertexIds.size() > 0;
            return this.queryAdjacentVertices(vertexIds.toArray());
        });
    }

    public Iterator<Vertex> queryAdjacentVertices(Object... vertexIds) {
        return this.queryVerticesByIds(vertexIds, true,
                                       this.checkAdjacentVertexExist);
    }

    public Iterator<Vertex> adjacentVertexWithProp(Object... ids) {
        return this.queryVerticesByIds(ids, true,
                                       this.checkAdjacentVertexExist);
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        return this.queryVerticesByIds(vertexIds, false, false);
    }

    /**
     * 查询task信息,指定类型为TASK,是为了访问存储的TASK_INFO表
     *
     * @param vertexIds
     * @return
     */
    public Iterator<Vertex> queryTaskInfos(Object... vertexIds) {
        return this.queryVerticesByIds(vertexIds, false, false,
                                       this::queryVerticesFromBackend, HugeType.TASK);
    }

    /**
     * 根据条件查询task信息
     *
     * @param query
     * @return
     */
    public Iterator<Vertex> queryTaskInfos(Query query) {
        // 设置为task 查询
        query.setTaskQuery(true);
        return this.queryVertices(query);
    }

    public Vertex queryVertex(Object vertexId) {
        Iterator<Vertex> iter = this.queryVerticesByIds(new Object[]{vertexId},
                                                        false, true);
        Vertex vertex = QueryResults.one(iter);
        if (vertex == null) {
            throw new NotFoundException("Vertex '%s' does not exist", vertexId);
        }
        return vertex;
    }

    protected Iterator<Vertex> queryVerticesByIds(Object[] vertexIds,
                                                  boolean adjacentVertex,
                                                  boolean checkMustExist) {
        return this.queryVerticesByIds(vertexIds, adjacentVertex, checkMustExist,
                                       this::queryVerticesFromBackend, HugeType.VERTEX);
    }

    protected Iterator<Vertex> queryVerticesByIds(Object[] vertexIds,
                                                  boolean adjacentVertex,
                                                  boolean checkMustExist,
                                                  Function<Query, Iterator<HugeVertex>> queryBackendFunction,
                                                  HugeType type) {
        Query.checkForceCapacity(vertexIds.length);

        // NOTE: allowed duplicated vertices if query by duplicated ids
        List<Id> ids = InsertionOrderUtil.newList();
        Map<Id, HugeVertex> vertices = new HashMap<>(vertexIds.length);

        IdQuery query = new IdQuery(type);
        // 当为任务类型查询时,需要对query isTaskQuery设置为true
        if (type == HugeType.TASK) {
            query.setTaskQuery(true);
        }
        for (Object vertexId : vertexIds) {
            HugeVertex vertex;
            Id id = HugeVertex.getIdValue(vertexId);
            if (id == null || this.removedVertices.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((vertex = this.addedVertices.get(id)) != null ||
                       (vertex = this.updatedVertices.get(id)) != null) {
                if (vertex.expired()) {
                    continue;
                }
                // Found from local tx
                vertices.put(vertex.id(), vertex);
            } else {
                // Prepare to query from backend store
                query.query(id);
            }
            ids.add(id);
        }

        if (!query.empty()) {
            // Query from backend store
            query.mustSortByInput(false);
            Iterator<HugeVertex> it = queryBackendFunction.apply(query);
            QueryResults.fillMap(it, vertices);
        }

        return new MapperIterator<>(ids.iterator(), id -> {
            HugeVertex vertex = vertices.get(id);
            if (vertex == null) {
                if (checkMustExist) {
                    String message = ErrorCode.VERTEX_NOT_EXIST.format(id);
                    throw new NotFoundException(message);
                } else if (adjacentVertex) {
                    // Return undefined if adjacentVertex but !checkMustExist
                    vertex = HugeVertex.undefined(this.graph(), id);
                    vertex.resetProperties();
                } else {
                    // Return null
                    assert vertex == null;
                }
            }
            return vertex;
        });
    }

    public Iterator<Vertex> queryVertices() {
        Query q = new Query(HugeType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterator<Vertex> queryVertices(Query query) {
        E.checkArgument(this.removedVertices.isEmpty() || query.noLimit(),
                        "It's not allowed to query with limit when " +
                        "there are uncommitted delete records.");

        query.resetActualOffset();

        Iterator<HugeVertex> results = this.queryVerticesFromBackend(query);
        results = this.filterUnmatchedRecords(results, query);

        @SuppressWarnings("unchecked")
        Iterator<Vertex> r = (Iterator<Vertex>) joinTxVertices(query, results);
        return this.skipOffsetOrStopLimit(r, query);
    }

    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        assert query.resultType().isVertex();

        QueryResults<BackendEntry> results = this.query(query);
        Iterator<BackendEntry> entries = results.iterator();

        Iterator<HugeVertex> vertices = new MapperIterator<>(entries,
                                                             this::parseEntry);
        vertices = this.filterExpiredResultFromFromBackend(query, vertices);

        if (!this.store().features().supportsQuerySortByInputIds()) {
            // There is no id in BackendEntry, so sort after deserialization
            vertices = results.keepInputOrderIfNeeded(vertices);
        }
        return vertices;
    }

    @Watched(prefix = "graph")
    public HugeEdge addEdge(HugeEdge edge) {
        this.checkOwnerThread();
        assert !edge.removed();

        // Override edges in local `removedEdges`
        this.removedEdges.remove(edge.id());
        try {
            this.locksTable.lockReads(LockUtil.EDGE_LABEL_DELETE,
                                      edge.schemaLabel().id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE,
                                      edge.schemaLabel().indexLabels());
            // Ensure edge label still exists from edge-construct to lock
            this.graph().edgeLabel(edge.schemaLabel().id());
            /*
             * No need to lock EDGE_LABEL_ADD_UPDATE, because edge label
             * update only can add nullable properties and user data, which is
             * unconcerned with add edge
             */
            this.beforeWrite();
            this.addedEdges.put(edge.id(), edge);
            this.afterWrite();
        } catch (Throwable e) {
            this.locksTable.unlock();
            throw e;
        }
        return edge;
    }

    @Watched(prefix = "graph")
    public void removeEdge(HugeEdge edge) {
        this.checkOwnerThread();

        this.beforeWrite();

        // Override edges in local `addedEdges`
        this.addedEdges.remove(edge.id());

        // Collect the removed edge
        this.removedEdges.put(edge.id(), edge);

        this.afterWrite();
    }

    public Iterator<Edge> queryEdgesByVertex(Id id) {
        return this.queryEdges(constructEdgesQuery(id, Directions.BOTH));
    }

    public Iterator<Edge> queryEdges(Object... edgeIds) {
        return this.queryEdgesByIds(edgeIds, false);
    }

    public Edge queryEdge(Object edgeId) {
        Iterator<Edge> iter = this.queryEdgesByIds(new Object[]{edgeId}, true);
        Edge edge = QueryResults.one(iter);
        if (edge == null) {
            throw new NotFoundException("Edge '%s' does not exist", edgeId);
        }
        return edge;
    }

    protected Iterator<Edge> queryEdgesByIds(Object[] edgeIds,
                                             boolean verifyId) {
        return this.queryEdgesByIds(edgeIds, verifyId, this::queryEdgesFromBackend);
    }

    protected Iterator<Edge> queryEdgesByIds(Object[] edgeIds,
                                             boolean verifyId,
                                             Function<Query, Iterator<HugeEdge>> queryBackendFunction) {
        Query.checkForceCapacity(edgeIds.length);

        // NOTE: allowed duplicated edges if query by duplicated ids
        List<Id> ids = InsertionOrderUtil.newList();
        Map<Id, HugeEdge> edges = new HashMap<>(edgeIds.length);

        IdQuery query = new IdQuery(HugeType.EDGE);
        for (Object edgeId : edgeIds) {
            HugeEdge edge;
            EdgeId id = HugeEdge.getIdValue(edgeId, !verifyId);
            if (id == null) {
                continue;
            }
            if (id.direction() == Directions.IN) {
                id = id.switchDirection();
            }
            if (this.removedEdges.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((edge = this.addedEdges.get(id)) != null ||
                       (edge = this.updatedEdges.get(id)) != null) {
                if (edge.expired()) {
                    continue;
                }
                // Found from local tx
                edges.put(edge.id(), edge);
            } else {
                // Prepare to query from backend store
                query.query(id);
            }
            ids.add(id);
        }

        if (!query.empty()) {
            // Query from backend store
            if (edges.isEmpty() && query.ids().size() == ids.size()) {
                /*
                 * Sort at the lower layer and return directly if there is no
                 * local vertex and duplicated id.
                 */
                Iterator<HugeEdge> it = queryBackendFunction.apply(query);
                @SuppressWarnings({"unchecked", "rawtypes"})
                Iterator<Edge> r = (Iterator) it;
                return r;
            }

            query.mustSortByInput(false);
            Iterator<HugeEdge> it = this.queryEdgesFromBackend(query);
            QueryResults.fillMap(it, edges);
        }

        return new MapperIterator<>(ids.iterator(), edges::get);
    }

    public Iterator<Edge> queryEdges() {
        Query q = new Query(HugeType.EDGE);
        return this.queryEdges(q);
    }

    @Watched
    public Iterator<Edge> queryEdges(Query query) {
        E.checkArgument(this.removedEdges.isEmpty() || query.noLimit(),
                        "It's not allowed to query with limit when " +
                        "there are uncommitted delete records.");

        query.resetActualOffset();

        Iterator<HugeEdge> results = this.queryEdgesFromBackend(query);
        results = this.filterUnmatchedRecords(results, query);

        /*
         * Without repeated edges if not querying by BOTH all edges
         * TODO: any unconsidered case, maybe the query with OR condition?
         */
        boolean dedupEdge = false;
        if (dedupEdge) {
            Set<Id> returnedEdges = new HashSet<>();
            results = new FilterIterator<>(results, edge -> {
                // Filter duplicated edges (edge may be repeated query both)
                if (!returnedEdges.contains(edge.id())) {
                    /*
                     * NOTE: Maybe some edges are IN and others are OUT
                     * if querying edges both directions, perhaps it would look
                     * better if we convert all edges in results to OUT, but
                     * that would break the logic when querying IN edges.
                     */
                    returnedEdges.add(edge.id());
                    return true;
                } else {
                    LOG.debug("Result contains duplicated edge: {}", edge);
                    return false;
                }
            });
        }

        @SuppressWarnings("unchecked")
        Iterator<Edge> r = (Iterator<Edge>) joinTxEdges(query, results,
                                                        this.removedVertices);
        return this.skipOffsetOrStopLimit(r, query);
    }

    @Watched
    public Iterator<CIter<Edge>> queryEdges(Iterator<Query> queryList) {
        if (queryList == null || !queryList.hasNext()) {
            return QueryResults.emptyIterator();
        }
        final boolean withProperties = (queryList instanceof EdgesQueryIterator
                                        && ((EdgesQueryIterator) queryList).isWithEdgeProperties());

//        final boolean lightWeitht = (queryList instanceof EdgesQueryIterator);
        // 当前都使用 EdgesQueryIterator.  todo: 应当把解析参数加入到 EdgesQueryIterator或者Query中
        final boolean lightWeitht = false;
        return this.queryEdgesFromBackend(queryList, (entry) ->
                serializer.readEdges(this.graph(), entry, withProperties, lightWeitht));
        /* 这里根据需要补完ConditionQuery部分的逻辑,暂时无应用场景 */
    }

    @Watched
    public CIter<EdgeId> queryEdgeIds(Query query) {
        ArrayList<Query> list = new ArrayList<>();
        list.add(query);
        Iterator<CIter<EdgeId>> its = this.queryEdgeIds(list.iterator());
        // 合并多个迭代器为一个
        return new FlatMapperIterator<>(its, (it) -> it);
    }

    @Watched
    public Iterator<CIter<EdgeId>> queryEdgeIds(Iterator<Query> queryList) {
        if (queryList == null || !queryList.hasNext()) {
            return QueryResults.emptyIterator();
        }
        return this.queryEdgesFromBackend(queryList, (entry) ->
                serializer.readEdgeIds(this.graph(), entry));
    }

    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        assert query.resultType().isEdge();
        if (query instanceof ConditionQuery) {
            /*如果查询的是父类型+ sortKeys, 进行单独处理
             * g.V("V.id").outE("parentLabel").has("sortKey","value")
             * cq.condition(HugeKeys.OWNER_LABEL) == null为核心判断条件
             * 如果查询的是子类型边+sortKeys,(HugeKeys.OWNER_LABEL)在
             * 前一步的constructQuery的时候就进行了注入
             * */
            ConditionQuery cq = (ConditionQuery) query;
            Id label = cq.condition(HugeKeys.LABEL);
            if (label != null &&
                graph().edgeLabel(label).isFather() &&
                cq.condition(HugeKeys.SUB_LABEL) == null &&
                cq.condition(HugeKeys.OWNER_VERTEX) != null &&
                cq.condition(HugeKeys.DIRECTION) != null &&
                matchEdgeSortKeys(cq, false, this.graph())) {
                return parentElQueryWithSortKeys(graph().edgeLabel(label), graph().edgeLabels(),
                                                 cq);
            }
        }

        QueryResults<BackendEntry> results = this.query(query);
        Iterator<BackendEntry> entries = results.iterator();

        Iterator<HugeEdge> edges = new FlatMapperIterator<>(entries, entry -> {
            // Edges are in a vertex
            HugeVertex vertex = this.parseEntry(entry, query.withProperties());
            if (vertex == null) {
                return null;
            }
            if (query.ids().size() == 1) {
                assert vertex.getEdges().size() == 1;
            }
            /*
             * Copy to avoid ConcurrentModificationException when removing edge
             * because HugeEdge.remove() will update edges in owner vertex
             */
            return new ListIterator<>(ImmutableList.copyOf(vertex.getEdges()));
        });

        edges = this.filterExpiredResultFromFromBackend(query, edges);

        if (!this.store().features().supportsQuerySortByInputIds()) {
            // There is no id in BackendEntry, so sort after deserialization
            edges = results.keepInputOrderIfNeeded(edges);
        }
        return edges;
    }

    private Iterator<HugeEdge> parentElQueryWithSortKeys(EdgeLabel label,
                                                         Collection<EdgeLabel> allEls,
                                                         ConditionQuery cq) {
        Iterator<HugeEdge> edges = null;
        int count = 0;

        /* g.V(id1).hasLabel("父类型").has("sortKeys",value) 将会转成子类型的并集
         * g.V(id1).hasLabel("子类型1").has("sortKeys",value) +
         * g.V(id1).hasLabel("子类型2").has("sortKeys",value) + ...
         * */
        for (EdgeLabel el : allEls) {
            if (el.edgeLabelType().sub() && el.fatherId().equals(label.id())) {
                ConditionQuery tempQuery = cq.copy();
                tempQuery.eq(HugeKeys.SUB_LABEL, el.id());
                count++;
                if (count == 1) {
                    edges = this.queryEdgesFromBackend(tempQuery);
                } else {
                    edges = Iterators.concat(edges,
                                             this.queryEdgesFromBackend(tempQuery));
                }
            }
        }
        return edges;
    }

    protected <K> Iterator<CIter<K>> queryEdgesFromBackend(Iterator<Query> queryList,
                                                           Function<BackendEntry, Iterator<K>> mapper) {
        if (!queryList.hasNext()) {
            return QueryResults.emptyIterator();
        }

        Iterator<Iterator<BackendEntry>> queryResults = this.query(queryList);
        return new OuterIterator<>(queryResults, mapper);
    }

    @Watched(prefix = "graph")
    public <V> void addVertexProperty(HugeVertexProperty<V> prop) {
        // NOTE: this method can also be used to update property

        HugeVertex vertex = prop.element();
        E.checkState(vertex != null,
                     "No owner for updating property '%s'", prop.key());

        // Add property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.setProperty(prop);
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertices.containsKey(vertex.id()) ||
                        this.updatedVertices.containsKey(vertex.id()),
                        "Can't update property '%s' for adding-state vertex",
                        prop.key());
        E.checkArgument(!vertex.removed() &&
                        !this.removedVertices.containsKey(vertex.id()),
                        "Can't update property '%s' for removing-state vertex",
                        prop.key());
        // Check is updating primary key
        List<Id> primaryKeyIds = vertex.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeyIds.contains(prop.propertyKey().id()),
                        "Can't update primary key: '%s'", prop.key());

        // Do property update
        this.lockForUpdateProperty(vertex.schemaLabel(), prop, () -> {
            // Update old vertex to remove index (without new property)
            this.indexTx.updateVertexIndex(vertex, true);
            // Update(add) vertex property
            this.propertyUpdated(vertex, prop, vertex.setProperty(prop));
        });
    }

    @Watched(prefix = "graph")
    public <V> void removeVertexProperty(HugeVertexProperty<V> prop) {
        HugeVertex vertex = prop.element();
        PropertyKey propKey = prop.propertyKey();
        E.checkState(vertex != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed (compatible with tinkerpop)
        if (!vertex.hasProperty(propKey.id())) {
            // PropertyTest shouldAllowRemovalFromVertexWhenAlreadyRemoved()
            return;
        }
        // Check is removing primary key
        List<Id> primaryKeyIds = vertex.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeyIds.contains(propKey.id()),
                        "Can't remove primary key '%s'", prop.key());
        // Remove property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.removeProperty(propKey.id());
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertices.containsKey(vertex.id()) ||
                        this.updatedVertices.containsKey(vertex.id()),
                        "Can't remove property '%s' for adding-state vertex",
                        prop.key());
        E.checkArgument(!this.removedVertices.containsKey(vertex.id()),
                        "Can't remove property '%s' for removing-state vertex",
                        prop.key());

        // Do property update
        this.lockForUpdateProperty(vertex.schemaLabel(), prop, () -> {
            // Update old vertex to remove index (with the property)
            this.indexTx.updateVertexIndex(vertex, true);
            // Update(remove) vertex property
            HugeProperty<?> removed = vertex.removeProperty(propKey.id());
            this.propertyUpdated(vertex, null, removed);
        });
    }

    @Watched(prefix = "graph")
    public <V> void addEdgeProperty(HugeEdgeProperty<V> prop) {
        // NOTE: this method can also be used to update property

        HugeEdge edge = prop.element();
        E.checkState(edge != null,
                     "No owner for updating property '%s'", prop.key());

        // Add property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.setProperty(prop);
            return;
        }
        // Check is updating property of added/removed edge
        E.checkArgument(!this.addedEdges.containsKey(edge.id()) ||
                        this.updatedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!edge.removed() &&
                        !this.removedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for removing-state edge",
                        prop.key());
        // Check is updating sort key
        List<Id> sortKeys = edge.schemaLabel().sortKeys();
        E.checkArgument(!sortKeys.contains(prop.propertyKey().id()),
                        "Can't update sort key '%s'", prop.key());

        // Do property update
        this.lockForUpdateProperty(edge.schemaLabel(), prop, () -> {
            // Update old edge to remove index (without new property)
            this.indexTx.updateEdgeIndex(edge, true);
            // Update(add) edge property
            this.propertyUpdated(edge, prop, edge.setProperty(prop));
        });
    }

    @Watched(prefix = "graph")
    public <V> void removeEdgeProperty(HugeEdgeProperty<V> prop) {
        HugeEdge edge = prop.element();
        PropertyKey propKey = prop.propertyKey();
        E.checkState(edge != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed
        if (!edge.hasProperty(propKey.id())) {
            return;
        }
        // Check is removing sort key
        List<Id> sortKeyIds = edge.schemaLabel().sortKeys();
        E.checkArgument(!sortKeyIds.contains(prop.propertyKey().id()),
                        "Can't remove sort key '%s'", prop.key());
        // Remove property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.removeProperty(propKey.id());
            return;
        }
        // Check is updating property of added/removed edge
        E.checkArgument(!this.addedEdges.containsKey(edge.id()) ||
                        this.updatedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!this.removedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for removing-state edge",
                        prop.key());

        // Do property update
        this.lockForUpdateProperty(edge.schemaLabel(), prop, () -> {
            // Update old edge to remove index (with the property)
            this.indexTx.updateEdgeIndex(edge, true);
            // Update(remove) edge property
            this.propertyUpdated(edge, null,
                                 edge.removeProperty(propKey.id()));
        });
    }

    private <R> QueryList<R> optimizeQueries(Query query,
                                             QueryResults.Fetcher<R> fetcher) {

        if (GraphReadMode.OLTP_ONLY.equals(this.graph().readMode())) {
            allowedOlapQuery((ConditionQuery) query);
        }

        QueryList<R> queries = new QueryList<>(query, fetcher);


        for (ConditionQuery cq : ConditionQueryFlatten.flatten(
                (ConditionQuery) query)) {
            // Optimize by sysprop
            Query q = this.optimizeQuery(cq);
            /*
             * NOTE: There are two possibilities for this query:
             * 1.sysprop-query, which would not be empty.
             * 2.index-query result(ids after optimization), which may be empty.
             *
             * NOTE: There will be a case here where there is UserpropRelation
             * and with no index, using hstore for Predicate in StoreNode
             */
            if (q == null) {
                Id label = cq.condition(HugeKeys.LABEL);
                if (label != null && cq.conditions().size() == 1) {
                    // 仅指定了Label并且enableLabelIndex
                    if ((cq.resultType().isVertex() &&
                         this.graph().vertexLabel(label).enableLabelIndex()) ||
                        (cq.resultType().isEdge() &&
                         this.graph().edgeLabel(label).enableLabelIndex())) {

                        QUERYLOGGER.info("Query with LabelIndex: {}, origin: " +
                                         "{}", cq, cq.originQuery());
                        // 指定了Label，且enableLabelIndex
                        queries.add(this.indexQuery(cq), this.batchSize);

                        continue;
                    }
                }

                // 判断是否命中索引
                Set<MatchedIndex> matchedIndexes = this.indexTx
                        .collectMatchedIndexes(cq);

                // 是否存在部分数据未创建索引
                boolean existNotHitIndex = false;
                for (MatchedIndex matchedIndex : matchedIndexes) {
                    if (CollectionUtils.isEmpty(matchedIndex.indexLabels())) {
                        existNotHitIndex = true;
                    }
                }

                if (existNotHitIndex) {
                    if (!this.hasOlapCondition(cq)) {
                        // 存在部分数据未命中索引，且不存在olap属性过滤
                        // 全部算子下沉，全局过滤
                        E.checkState(this.indexTx.store().features()
                                                 .supportsFilterInStore(),
                                     "Must support filter in store");
                        QUERYLOGGER.info("Use operator sinking for {}, " +
                                         "MatchedIndexes: {}, origin: {}",
                                         cq, matchedIndexes, cq.originQuery());
                        queries.add(cq);
                    } else {
                        // 存在部分数据未命中索引，且存在olap属性过滤
                        // 需要移除OLAP相关conditon后走算子下沉的逻辑
                        // 全部算子下沉，全局过滤
                        ConditionQuery newQuery = cq.copy();
                        removeOlapCondtions(newQuery);

                        E.checkState(this.indexTx.store().features()
                                                 .supportsFilterInStore(),
                                     "Must support filter in store");
                        QUERYLOGGER.info("Use operator sinking with olap " +
                                         "query {}, origin: {}", newQuery,
                                         cq.originQuery());
                        queries.add(newQuery);
                    }
                } else {
                    for (MatchedIndex matchedIndex : matchedIndexes) {
                        ConditionQuery newQuery = cq.copy();
                        newQuery.matchedIndex(matchedIndex);

                        QUERYLOGGER.info("Use index query for {}, " +
                                         "MatchedIndex: {}, origin: {}", cq,
                                         matchedIndex, cq.originQuery());
                        queries.add(this.indexQuery(newQuery),
                                    this.batchSize);
                    }
                }
            } else if (!q.empty()) {
                queries.add(q);
            }
        }
        return queries;
    }

    private boolean hasOlapCondition(ConditionQuery query) {
        for (Id pk : query.userpropKeys()) {
            if (this.graph().propertyKey(pk).olap()) {
                return true;
            }
        }

        return false;
    }

    private void removeOlapCondtions(ConditionQuery query) {
        for (Id pk : query.userpropKeys()) {
            if (this.graph().propertyKey(pk).olap()) {
                query.userpropConditions(pk).forEach(
                        c -> query.removeCondition(c)
                );
            }
        }
    }

    private Query optimizeQuery(ConditionQuery query) {
        if (!query.ids().isEmpty()) {
            throw new HugeException(
                    "Not supported querying by id and conditions: %s", query);
        }

        Id label = query.condition(HugeKeys.LABEL);

        // Optimize vertex query
        if (label != null && query.resultType().isVertex()) {
            VertexLabel vertexLabel = this.graph().vertexLabel(label);
            if (vertexLabel.idStrategy() == IdStrategy.PRIMARY_KEY) {
                List<Id> keys = vertexLabel.primaryKeys();
                E.checkState(!keys.isEmpty(),
                             "The primary keys can't be empty when using " +
                             "'%s' id strategy for vertex label '%s'",
                             IdStrategy.PRIMARY_KEY, vertexLabel.name());
                if (query.matchUserpropKeys(keys)) {
                    // Query vertex by label + primary-values
                    query.optimized(OptimizedType.PRIMARY_KEY);
                    String primaryValues = query.userpropValuesString(keys);
                    LOG.debug("Query vertices by primaryKeys: {}", query);
                    // Convert {vertex-label + primary-key} to vertex-id
                    Id id = SplicingIdGenerator.splicing(label.asString(),
                                                         primaryValues);
                    /*
                     * Just query by primary-key(id), ignore other userprop(if
                     * exists) that it will be filtered by queryVertices(Query)
                     */
                    return new IdQuery(query, id);
                }
            }
        }

        // Optimize edge query
        if (query.resultType().isEdge() && label != null &&
            query.condition(HugeKeys.OWNER_VERTEX) != null &&
            query.condition(HugeKeys.DIRECTION) != null &&
            matchEdgeSortKeys(query, false, this.graph())) {
            // Query edge by sourceVertex + direction + label + sort-values
            query.optimized(OptimizedType.SORT_KEYS);
            query = query.copy();
            if (query.hasRangeCondition()) {
                query.shard(true);
            }
            // Serialize sort-values
            EdgeLabel el = this.graph().edgeLabel(label);
            List<Id> keys = el.sortKeys();
            List<Condition> conditions =
                    GraphIndexTransaction.constructShardConditions(
                            query, keys, HugeKeys.SORT_VALUES);

            query.query(conditions);

            // 如果优化策略是sortKeys类型，需要注入 subLabelId
            // 因为EdgeId = sourceVertex + direction + label +subLabelId +
            // sortKeys
            if (query.condition(HugeKeys.SUB_LABEL) == null) {
                query.eq(HugeKeys.SUB_LABEL, el.id());
            }
            LOG.debug("Query edges by sortKeys: {}", query);
            return query;
        }

        // Optimize edge query
        if (query.resultType().isEdge() &&
            query.condition(HugeKeys.OWNER_VERTEX) != null &&
            query.condition(HugeKeys.DIRECTION) != null) {
            // Query edge by sourceVertex + direction + props
            return query;
        }

        /*
         * Query only by sysprops, like: by vertex label, by edge label.
         * NOTE: we assume sysprops would be indexed by backend store
         * but we don't support query edges only by direction/target-vertex.
         */
        if (query.allSysprop()) {
            if (query.resultType().isVertex()) {
                verifyVerticesConditionQuery(query);
            } else if (query.resultType().isEdge()) {
                verifyEdgesConditionQuery(query);
            }
            /*
             * Just support:
             *  1.not query by label
             *  2.or query by label and store supports this feature
             */
            boolean byLabel = (label != null && query.conditions().size() == 1);
            if (!byLabel || this.store().features().supportsQueryByLabel()) {
                return query;
            }
        }

        return null;
    }

    private IdHolderList indexQuery(ConditionQuery query) {
        /*
         * Optimize by index-query
         * It will return a list of id (maybe empty) if success,
         * or throw exception if there is no any index for query properties.
         */
        this.beforeRead();
        try {
            return this.indexTx.queryIndex(query);
        } finally {
            this.afterRead();
        }
    }

    private VertexLabel checkVertexLabel(Object label, boolean verifyLabel) {
        HugeVertexFeatures features = graph().features().vertex();

        // Check Vertex label
        if (label == null && features.supportsDefaultLabel()) {
            label = features.defaultLabel();
        }

        if (label == null) {
            throw Element.Exceptions.labelCanNotBeNull();
        }

        E.checkArgument(label instanceof String || label instanceof VertexLabel,
                        "Expect a string or a VertexLabel object " +
                        "as the vertex label argument, but got: '%s'", label);
        // The label must be an instance of String or VertexLabel
        if (label instanceof String) {
            if (verifyLabel) {
                ElementHelper.validateLabel((String) label);
            }
            label = graph().vertexLabel((String) label);
        }

        assert (label != null);
        return (VertexLabel) label;
    }

    private void checkId(Id id, List<Id> keys, VertexLabel vertexLabel) {
        // Check whether id match with id strategy
        IdStrategy strategy = vertexLabel.idStrategy();
        switch (strategy) {
            case PRIMARY_KEY:
                E.checkArgument(id == null,
                                "Can't customize vertex id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                // Check whether primaryKey exists
                List<Id> primaryKeys = vertexLabel.primaryKeys();
                E.checkArgument(keys.containsAll(primaryKeys),
                                "The primary keys: %s of vertex label '%s' " +
                                "must be set when using '%s' id strategy",
                                this.graph().mapPkId2Name(primaryKeys),
                                vertexLabel.name(), strategy);
                break;
            case AUTOMATIC:
                if (this.params().mode().maintaining()) {
                    E.checkArgument(id != null && id.number(),
                                    "Must customize vertex number id when " +
                                    "id strategy is '%s' for vertex label " +
                                    "'%s' in restoring mode",
                                    strategy, vertexLabel.name());
                } else {
                    E.checkArgument(id == null,
                                    "Can't customize vertex id when " +
                                    "id strategy is '%s' for vertex label '%s'",
                                    strategy, vertexLabel.name());
                }
                break;
            case CUSTOMIZE_STRING:
            case CUSTOMIZE_UUID:
                E.checkArgument(id != null && !id.number(),
                                "Must customize vertex string id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                break;
            case CUSTOMIZE_NUMBER:
                E.checkArgument(id != null && id.number(),
                                "Must customize vertex number id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                break;
            default:
                throw new AssertionError("Unknown id strategy: " + strategy);
        }
    }

    private void checkAggregateProperty(HugeElement element) {
        E.checkArgument(element.getAggregateProperties().isEmpty() ||
                        this.store().features().supportsAggregateProperty(),
                        "The %s store does not support aggregate property",
                        this.store().provider().type());
    }

    private void checkAggregateProperty(HugeProperty<?> property) {
        E.checkArgument(!property.isAggregateType() ||
                        this.store().features().supportsAggregateProperty(),
                        "The %s store does not support aggregate property",
                        this.store().provider().type());
    }

    private void checkNonnullProperty(HugeVertex vertex) {
        Set<Id> keys = vertex.getProperties().keySet();
        VertexLabel vertexLabel = vertex.schemaLabel();
        // Check whether passed all non-null property
        @SuppressWarnings("unchecked")
        Collection<Id> nonNullKeys = CollectionUtils.subtract(
                vertexLabel.properties(),
                vertexLabel.nullableKeys());
        if (!keys.containsAll(nonNullKeys)) {
            @SuppressWarnings("unchecked")
            Collection<Id> missed = CollectionUtils.subtract(nonNullKeys, keys);
            HugeGraph graph = this.graph();
            E.checkArgument(false, "All non-null property keys %s of " +
                                   "vertex label '%s' must be setted, missed keys %s",
                            graph.mapPkId2Name(nonNullKeys), vertexLabel.name(),
                            graph.mapPkId2Name(missed));
        }
    }

    private void checkVertexExistIfCustomizedId(Map<Id, HugeVertex> vertices) {
        Set<Id> ids = new HashSet<>();
        for (HugeVertex vertex : vertices.values()) {
            VertexLabel vl = vertex.schemaLabel();
            if (!vl.hidden() && vl.idStrategy().isCustomized()) {
                ids.add(vertex.id());
            }
        }
        if (ids.isEmpty()) {
            return;
        }
        IdQuery idQuery = new IdQuery(HugeType.VERTEX, ids);
        Iterator<HugeVertex> results = this.queryVerticesFromBackend(idQuery);
        try {
            if (!results.hasNext()) {
                return;
            }
            HugeVertex existedVertex = results.next();
            HugeVertex newVertex = vertices.get(existedVertex.id());
            if (!existedVertex.label().equals(newVertex.label())) {
                throw new HugeException(
                        "The newly added vertex with id:'%s' label:'%s' " +
                        "is not allowed to insert, because already exist " +
                        "a vertex with same id and different label:'%s'",
                        newVertex.id(), newVertex.label(),
                        existedVertex.label());
            }
        } finally {
            CloseableIterator.closeIterator(results);
        }
    }

    private void lockForUpdateProperty(SchemaLabel schemaLabel,
                                       HugeProperty<?> prop,
                                       Runnable callback) {
        this.checkOwnerThread();

        Id pkey = prop.propertyKey().id();
        Set<Id> indexIds = new HashSet<>();
        for (Id il : schemaLabel.indexLabels()) {
            if (graph().indexLabel(il).indexFields().contains(pkey)) {
                indexIds.add(il);
            }
        }
        String group = schemaLabel.type() == HugeType.VERTEX_LABEL ?
                       LockUtil.VERTEX_LABEL_DELETE :
                       LockUtil.EDGE_LABEL_DELETE;
        try {
            this.locksTable.lockReads(group, schemaLabel.id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE, indexIds);
            // Ensure schema label still exists
            if (schemaLabel.type() == HugeType.VERTEX_LABEL) {
                this.graph().vertexLabel(schemaLabel.id());
            } else {
                assert schemaLabel.type() == HugeType.EDGE_LABEL;
                this.graph().edgeLabel(schemaLabel.id());
            }
            /*
             * No need to lock INDEX_LABEL_ADD_UPDATE, because index label
             * update only can add  user data, which is unconcerned with
             * update property
             */
            this.beforeWrite();
            callback.run();
            this.afterWrite();
        } catch (Throwable e) {
            this.locksTable.unlock();
            throw e;
        }
    }

    private void removeLeftIndexIfNeeded(Map<Id, HugeVertex> vertices) {
        Set<Id> ids = vertices.keySet();
        if (ids.isEmpty()) {
            return;
        }
        IdQuery idQuery = new IdQuery(HugeType.VERTEX, ids);
        Iterator<HugeVertex> results = this.queryVerticesFromBackend(idQuery);
        try {
            while (results.hasNext()) {
                HugeVertex existedVertex = results.next();
                this.indexTx.updateVertexIndex(existedVertex, true);
            }
        } finally {
            CloseableIterator.closeIterator(results);
        }
    }

    private <T extends HugeElement> Iterator<T> filterUnmatchedRecords(
            Iterator<T> results,
            Query query) {
        // Filter unused or incorrect records
        return new FilterIterator<>(results, elem -> {
            // TODO: Left vertex/edge should to be auto removed via async task
            return filterElement(query, elem);
        });
    }

    private <T extends HugeElement> Boolean filterElement(Query query, T elem) {
        if (elem.schemaLabel().undefined()) {
            LOG.warn("Left record is found: id={}, label={}, properties={}",
                     elem.id(), elem.schemaLabel().id(),
                     elem.getPropertiesMap());
        }
        // Filter hidden results
        if (!query.showHidden() && Graph.Hidden.isHidden(elem.label())) {
            return false;
        }
        // Filter vertices/edges of deleting label
        if (elem.schemaLabel().status().deleting() &&
            !query.showDeleting()) {
            return false;
        }
        // Process results that query from left index or primary-key
        if (query.resultType().isVertex() == elem.type().isVertex() &&
            !rightResultFromIndexQuery(query, elem)) {
            // Only index query will come here
            return false;
        }
        return true;
    }

    private boolean rightResultFromIndexQuery(Query query, HugeElement elem) {
        /*
         * If query is ConditionQuery or query.originQuery() is ConditionQuery
         * means it's index query
         */
        if (!(query instanceof ConditionQuery)) {
            if (query.originQuery() instanceof ConditionQuery) {
                query = query.originQuery();
            } else {
                return true;
            }
        }

        ConditionQuery cq = (ConditionQuery) query;

        if (cq.conditions().size() == 1 &&
            // 当只是g.E().haslabel()查询时，不用进行条件过滤
            cq.condition(HugeKeys.LABEL) != null && cq.resultType().isEdge()) {
            return true;
        }

        if (cq.optimized() == OptimizedType.NONE || cq.test(elem)) {
            if (cq.existLeftIndex(elem.id())) {
                /*
                 * Both have correct and left index, wo should return true
                 * but also needs to cleaned up left index
                 */
                this.indexTx.asyncRemoveIndexLeft(cq, elem);
            }

            /* Return true if:
             * 1.not query by index or by primary-key/sort-key
             *   (cq.optimized() == 0 means query just by sysprop)
             * 2.the result match all conditions
             */
            return true;
        }

        if (cq.optimized() == OptimizedType.INDEX) {
            this.indexTx.asyncRemoveIndexLeft(cq, elem);
        }
        return false;
    }

    private <T extends HugeElement> Iterator<T>
    filterExpiredResultFromFromBackend(
            Query query, Iterator<T> results) {
        if (this.store().features().supportsTtl() || query.showExpired()) {
            return results;
        }
        // Filter expired vertices/edges with TTL
        return new FilterIterator<>(results, elem -> {
            if (elem.expired()) {
                DeleteExpiredJob.asyncDeleteExpiredObject(this.graph(), elem);
                return false;
            }
            return true;
        });
    }

    private <T> Iterator<T> skipOffsetOrStopLimit(Iterator<T> results,
                                                  Query query) {
        if (query.noLimitAndOffset()) {
            return results;
        }
        // Skip offset
        long offset = query.offset();
        if (offset > 0L && results.hasNext()) {
            /*
             * Must call results.hasNext() before query.actualOffset() due to
             * some backends will go offset and update query.actualOffset
             */
            long current = query.actualOffset();
            for (; current < offset && results.hasNext(); current++) {
                results.next();
                query.goOffset(1L);
            }
        }
        // Stop if reach limit
        return new LimitIterator<>(results, elem -> {
            long count = query.goOffset(1L);
            if (query.reachLimit(count - 1L)) {
                try {
                    if (results instanceof CIter) {
                        ((CIter<?>) results).close();
                    } else {
                        LOG.warn("Failed to close iterator {}({})", results,
                                 results.getClass());
                    }
                } catch (Exception e) {
                    throw new HugeException("Failed to close iterator", e);
                }
                return true;
            }
            return false;
        });
    }

    private Iterator<?> joinTxVertices(Query query,
                                       Iterator<HugeVertex> vertices) {
        assert query.resultType().isVertex();
        BiFunction<Query, HugeVertex, HugeVertex> matchTxFunc = (q, v) -> {
            if (v.expired() && !q.showExpired()) {
                // Filter expired vertices with TTL
                return null;
            }
            // Filter vertices matched conditions
            return q.test(v) ? v : null;
        };
        vertices = this.joinTxRecords(query, vertices, matchTxFunc,
                                      this.addedVertices, this.removedVertices,
                                      this.updatedVertices);
        return vertices;
    }

    private Iterator<?> joinTxEdges(Query query, Iterator<HugeEdge> edges,
                                    Map<Id, HugeVertex> removingVertices) {
        assert query.resultType().isEdge();
        BiFunction<Query, HugeEdge, HugeEdge> matchTxFunc = (q, e) -> {
            assert q.resultType() == HugeType.EDGE;
            if (e.expired() && !q.showExpired()) {
                // Filter expired edges with TTL
                return null;
            }
            // Filter edges matched conditions
            return q.test(e) ? e : q.test(e = e.switchOwner()) ? e : null;
        };
        edges = this.joinTxRecords(query, edges, matchTxFunc,
                                   this.addedEdges, this.removedEdges,
                                   this.updatedEdges);
        if (removingVertices.isEmpty()) {
            return edges;
        }
        // Filter edges that belong to deleted vertex
        return new FilterIterator<>(edges, edge -> {
            for (HugeVertex v : removingVertices.values()) {
                if (edge.belongToVertex(v)) {
                    return false;
                }
            }
            return true;
        });
    }

    private <V extends HugeElement> Iterator<V> joinTxRecords(
            Query query,
            Iterator<V> records,
            BiFunction<Query, V, V> matchFunc,
            Map<Id, V> addedTxRecords,
            Map<Id, V> removedTxRecords,
            Map<Id, V> updatedTxRecords) {
        this.checkOwnerThread();
        // Return the origin results if there is no change in tx
        if (addedTxRecords.isEmpty() &&
            removedTxRecords.isEmpty() &&
            updatedTxRecords.isEmpty()) {
            return records;
        }

        Set<V> txResults = InsertionOrderUtil.newSet();

        /*
         * Collect added/updated records
         * Records in memory have higher priority than query from backend store
         */
        for (V elem : addedTxRecords.values()) {
            if (query.reachLimit(txResults.size())) {
                break;
            }
            if ((elem = matchFunc.apply(query, elem)) != null) {
                txResults.add(elem);
            }
        }
        for (V elem : updatedTxRecords.values()) {
            if (query.reachLimit(txResults.size())) {
                break;
            }
            if ((elem = matchFunc.apply(query, elem)) != null) {
                txResults.add(elem);
            }
        }

        // Filter backend record if it's updated in memory
        Iterator<V> backendResults = new FilterIterator<>(records, elem -> {
            Id id = elem.id();
            return !addedTxRecords.containsKey(id) &&
                   !updatedTxRecords.containsKey(id) &&
                   !removedTxRecords.containsKey(id);
        });

        return new ExtendableIterator<>(txResults.iterator(), backendResults);
    }

    private void checkTxVerticesCapacity() throws LimitExceedException {
        if (this.verticesInTxSize() >= this.verticesCapacity) {
            throw new LimitExceedException(
                    "Vertices size has reached tx capacity %d",
                    this.verticesCapacity);
        }
    }

    private void checkTxEdgesCapacity() throws LimitExceedException {
        if (this.edgesInTxSize() >= this.edgesCapacity) {
            throw new LimitExceedException(
                    "Edges size has reached tx capacity %d",
                    this.edgesCapacity);
        }
    }

    private void propertyUpdated(HugeElement element, HugeProperty<?> property,
                                 HugeProperty<?> oldProperty) {
        if (element.type().isVertex()) {
            this.updatedVertices.put(element.id(), (HugeVertex) element);
        } else {
            assert element.type().isEdge();
            this.updatedEdges.put(element.id(), (HugeEdge) element);
        }

        if (oldProperty != null) {
            this.updatedOldestProps.add(oldProperty);
        }
        if (property == null) {
            this.removedProps.add(oldProperty);
        } else {
            this.addedProps.remove(property);
            this.addedProps.add(property);
        }
    }

    private HugeVertex parseEntry(BackendEntry entry) {
        return this.parseEntry(entry, true);
    }

    private HugeVertex parseEntry(BackendEntry entry, boolean withProperty) {
        try {
            HugeVertex vertex = this.serializer.readVertex(graph(), entry, withProperty);
            assert vertex != null;
            return vertex;
        } catch (ForbiddenException | SecurityException e) {
            /*
             * Can't ignore permission exception here, otherwise users will
             * be confused to treat as the record does not exist.
             */
            throw e;
        } catch (Throwable e) {
            LOG.error("Failed to parse entry: {}", entry, e);
            if (this.ignoreInvalidEntry) {
                return null;
            }
            throw e;
        }
    }

    /*
     * TODO: set these methods to protected
     */
    public void removeIndex(IndexLabel indexLabel) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.beforeWrite();
        this.indexTx.removeIndex(indexLabel);
        this.afterWrite();
    }

    public void updateIndex(Id ilId, HugeElement element, boolean removed) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.indexTx.updateIndex(ilId, element, removed);
    }

    public void removeIndex(HugeIndex index) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.beforeWrite();
        this.indexTx.doEliminate(this.serializer.writeIndex(index));
        this.afterWrite();
    }

    public void removeVertices(VertexLabel vertexLabel) {
        if (this.hasUpdate()) {
            throw new HugeException("There are still changes to commit");
        }

        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        // Commit data already in tx firstly
        this.commit();
        try {
            this.traverseVerticesByLabel(vertexLabel, vertex -> {
                this.removeVertex((HugeVertex) vertex);
                this.commitIfGtSize(COMMIT_BATCH);
            }, true);
            this.commit();
        } catch (Exception e) {
            LOG.error("Failed to remove vertices", e);
            throw new HugeException("Failed to remove vertices", e);
        } finally {
            this.autoCommit(autoCommit);
        }
    }

    public void removeEdges(EdgeLabel edgeLabel) {
        if (this.hasUpdate()) {
            throw new HugeException("There are still changes to commit");
        }

        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        // Commit data already in tx firstly
        this.commit();
        try {
            if (this.store().features().supportsDeleteEdgeByLabel()) {
                // TODO: Need to change to writeQuery!
                this.doRemove(this.serializer.writeId(HugeType.EDGE_OUT,
                                                      edgeLabel.id()));
                this.doRemove(this.serializer.writeId(HugeType.EDGE_IN,
                                                      edgeLabel.id()));
            } else {
                this.traverseEdgesByLabel(edgeLabel, edge -> {
                    this.removeEdge((HugeEdge) edge);
                    this.commitIfGtSize(COMMIT_BATCH);
                }, true);
            }
            this.commit();
        } catch (Exception e) {
            LOG.error("Failed to remove edges", e);
            throw new HugeException("Failed to remove edges", e);
        } finally {
            this.autoCommit(autoCommit);
        }
    }

    public void traverseVerticesByLabel(VertexLabel label,
                                        Consumer<Vertex> consumer,
                                        boolean deleting) {
        this.traverseByLabel(label, this::queryVertices, consumer, deleting);
    }

    public void traverseEdgesByLabel(EdgeLabel label, Consumer<Edge> consumer,
                                     boolean deleting) {
        this.traverseByLabel(label, this::queryEdges, consumer, deleting);
    }

    private <T> void traverseByLabel(SchemaLabel label,
                                     Function<Query, Iterator<T>> fetcher,
                                     Consumer<T> consumer, boolean deleting) {
        HugeType type = label.type() == HugeType.VERTEX_LABEL ?
                        HugeType.VERTEX : HugeType.EDGE;
        Query query = label.enableLabelIndex() ? new ConditionQuery(type) :
                      new Query(type);
        query.capacity(Query.NO_CAPACITY);
        query.limit(Query.NO_LIMIT);
        if (this.store().features().supportsQueryByPage()) {
            query.page(PageInfo.PAGE_NONE);
        }
        if (label.hidden()) {
            query.showHidden(true);
        }
        query.showDeleting(deleting);
        query.showExpired(deleting);

        if (label.enableLabelIndex()) {
            // Support label index, query by label index by paging
            ((ConditionQuery) query).eq(HugeKeys.LABEL, label.id());
            Iterator<T> iter = fetcher.apply(query);
            try {
                // Fetch by paging automatically
                while (iter.hasNext()) {
                    consumer.accept(iter.next());
                    /*
                     * Commit per batch to avoid too much data in single commit,
                     * especially for Cassandra backend
                     */
                    this.commitIfGtSize(GraphTransaction.COMMIT_BATCH);
                }
                // Commit changes if exists
                this.commit();
            } finally {
                CloseableIterator.closeIterator(iter);
            }
        } else {
            // Not support label index, query all and filter by label

            // 2023-03-21
            // 不适用分页功能，直接全量查找。
            Iterator<T> iter = fetcher.apply(query);
            try {
                while (iter.hasNext()) {
                    T e = iter.next();
                    SchemaLabel elemLabel = ((HugeElement) e).schemaLabel();
                    if (label.equals(elemLabel)) {
                        consumer.accept(e);
                        /*
                         * Commit per batch to avoid too much data in single
                         * commit, especially for Cassandra backend
                         */
                        this.commitIfGtSize(GraphTransaction.COMMIT_BATCH);
                    }
                }
                // Commit changes of every page before next page query
                this.commit();
            } finally {
                CloseableIterator.closeIterator(iter);
            }
        }
    }

    static class OuterIterator<K> implements Iterator<CIter<K>>, Closeable {
        Iterator<Iterator<BackendEntry>> obj;
        Function<BackendEntry, Iterator<K>> mapper;

        public OuterIterator(Iterator<Iterator<BackendEntry>> obj,
                             Function<BackendEntry, Iterator<K>> mapper) {
            this.obj = obj;
            this.mapper = mapper;
        }

        @Override
        public boolean hasNext() {
            return obj.hasNext();
        }

        @Override
        public CIter<K> next() {
            return new FlatMapperIterator<>(obj.next(), mapper);
        }

        @Override
        public void close() throws IOException {
            CloseableIterator.closeIterator(obj);
        }
    }
}