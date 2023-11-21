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

package org.apache.hugegraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStoreSystemInfo;
import org.apache.hugegraph.backend.tx.SchemaTransaction;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeFeatures;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.traversal.optimize.HugeCountStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugePrimaryKeyStrategy;
import org.apache.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Graph interface for Gremlin operations
 */
public interface HugeGraph extends Graph {

    public static void registerTraversalStrategies(Class<?> clazz) {
        TraversalStrategies strategies = null;
        strategies = TraversalStrategies.GlobalCache
                .getStrategies(Graph.class)
                .clone();
        strategies.addStrategies(HugeVertexStepStrategy.instance(),
                                 HugeGraphStepStrategy.instance(),
                                 HugeCountStepStrategy.instance(),
                                 HugePrimaryKeyStrategy.instance());

        TraversalStrategies.GlobalCache.registerStrategies(clazz, strategies);
    }

    public HugeGraph hugegraph();

    public HugeGraph hugegraph(boolean required);

    public SchemaManager schema();

    public SchemaTransaction schemaTransaction();

    public BackendStoreProvider storeProvider();

    public Id getNextId(HugeType type);

    public Id addPropertyKey(PropertyKey key);

    public void updatePropertyKey(PropertyKey old, PropertyKey upadte);

    public void updatePropertyKey(PropertyKey key);

    public Id removePropertyKey(Id key);

    public Id clearPropertyKey(PropertyKey propertyKey);

    public Collection<PropertyKey> propertyKeys();

    public PropertyKey propertyKey(String key);

    public PropertyKey propertyKey(Id key);

    public boolean existsPropertyKey(String key);

    public boolean existsOlapTable(PropertyKey key);

    public void addVertexLabel(VertexLabel vertexLabel);

    public void updateVertexLabel(VertexLabel label);

    public Id removeVertexLabel(Id label);

    public Collection<VertexLabel> vertexLabels();

    public VertexLabel vertexLabel(String label);

    public VertexLabel vertexLabel(Id label);

    public VertexLabel vertexLabelOrNone(Id id);

    public boolean existsVertexLabel(String label);

    public boolean existsLinkLabel(Id vertexLabel);

    public void addEdgeLabel(EdgeLabel edgeLabel);

    public void updateEdgeLabel(EdgeLabel label);

    public Id removeEdgeLabel(Id label);

    public Collection<EdgeLabel> edgeLabels();

    public EdgeLabel edgeLabel(String label);

    public EdgeLabel edgeLabel(Id label);

    public EdgeLabel edgeLabelOrNone(Id label);

    public boolean existsEdgeLabel(String label);

    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel);

    public void updateIndexLabel(IndexLabel label);

    public Id removeIndexLabel(Id label);

    public Id rebuildIndex(SchemaElement schema);

    public Collection<IndexLabel> indexLabels();

    public IndexLabel indexLabel(String label);

    public IndexLabel indexLabel(Id id);

    public boolean existsIndexLabel(String label);

    @Override
    public Vertex addVertex(Object... keyValues);

    public void removeVertex(Vertex vertex);

    public void removeVertex(String label, Object id);

    public <V> void addVertexProperty(VertexProperty<V> property);

    public <V> void removeVertexProperty(VertexProperty<V> property);

    public Edge addEdge(Edge edge);

    public void canAddEdge(Edge edge);

    public void removeEdge(Edge edge);

    public void removeEdge(String label, Object id);

    public <V> void addEdgeProperty(Property<V> property);

    public <V> void removeEdgeProperty(Property<V> property);

    public Vertex vertex(Object id);

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds);

    public Iterator<Vertex> vertices(Query query);

    public Iterator<Vertex> adjacentVertex(Object id);

    public Iterator<Vertex> adjacentVertexWithProp(Object... ids);

    public boolean checkAdjacentVertexExist();

    public Edge edge(Object id);

    @Override
    public Iterator<Edge> edges(Object... edgeIds);

    public Iterator<Edge> edges(Query query);

    Iterator<CIter<Edge>> edges(Iterator<Query> queryList);

    CIter<EdgeId> edgeIds(Query query);

    Iterator<CIter<EdgeId>> edgeIds(Iterator<Query> queryList);

    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges);

    public Iterator<Edge> adjacentEdges(Id vertexId);

    public Number queryNumber(Query query);

    public String graphSpace();

    public void graphSpace(String graphSpace);

    public String name();

    public String spaceGraphName();

    public String backend();

    public String backendVersion();

    public BackendStoreSystemInfo backendStoreSystemInfo();

    public BackendFeatures backendStoreFeatures();

    public GraphMode mode();

    public void mode(GraphMode mode);

    public GraphReadMode readMode();

    public void readMode(GraphReadMode readMode);

    public String nickname();

    public void nickname(String nickname);

    public String creator();

    public void creator(String creator);

    public Date createTime();

    public void createTime(Date createTime);

    public Date updateTime();

    public void updateTime(Date updateTime);

    public void refreshUpdateTime();

    public void waitStarted();

    public void serverStarted();

    public boolean started();

    public void started(boolean started);

    public boolean closed();

    public void closeTx();

    public Vertex addVertex(Vertex vertex);

    public <T> T metadata(HugeType type, String meta, Object... args);

    public void initBackend();

    public void clearBackend();

    public void truncateBackend();

    public void truncateGraph();

    @Override
    public HugeFeatures features();

    public AuthManager authManager();

    public void switchAuthManager(AuthManager authManager);

    public TaskScheduler taskScheduler();

    public void proxy(HugeGraph graph);

    public boolean sameAs(HugeGraph graph);

    public long now();

    public <K, V> V option(TypedOption<K, V> option);

    public default List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapVlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.vertexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<Id> mapPkName2Id(Collection<String> pkeys) {
        List<Id> ids = new ArrayList<>(pkeys.size());
        for (String pkey : pkeys) {
            PropertyKey propertyKey = this.propertyKey(pkey);
            ids.add(propertyKey.id());
        }
        return ids;
    }

    public default Id[] mapElName2Id(String[] edgeLabels) {
        Id[] ids = new Id[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            EdgeLabel edgeLabel = this.edgeLabel(edgeLabels[i]);
            if (edgeLabel.hasFather()) {
                ids[i] = edgeLabel.fatherId();
            } else {
                ids[i] = edgeLabel.id();
            }
        }
        return ids;
    }

    public default EdgeLabel[] mapElName2El(String[] edgeLabels) {
        EdgeLabel[] els = new EdgeLabel[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            els[i] = this.edgeLabel(edgeLabels[i]);
        }
        return els;
    }

    public default Id[] mapVlName2Id(String[] vertexLabels) {
        Id[] ids = new Id[vertexLabels.length];
        for (int i = 0; i < vertexLabels.length; i++) {
            VertexLabel vertexLabel = this.vertexLabel(vertexLabels[i]);
            ids[i] = vertexLabel.id();
        }
        return ids;
    }

    public default Set<Pair<String, String>> mapPairId2Name(
            Set<Pair<Id, Id>> pairs) {
        Set<Pair<String, String>> results = new HashSet<>(pairs.size());
        for (Pair<Id, Id> pair : pairs) {
            results.add(Pair.of(this.vertexLabel(pair.getLeft()).name(),
                                this.vertexLabel(pair.getRight()).name()));
        }
        return results;
    }

    public void applyMutation(BackendMutation mutation);
}
