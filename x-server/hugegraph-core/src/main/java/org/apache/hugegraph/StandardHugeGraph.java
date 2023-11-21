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

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.analyzer.Analyzer;
import org.apache.hugegraph.analyzer.AnalyzerFactory;
import org.apache.hugegraph.auth.AuthContext;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.cache.CachedGraphTransaction;
import org.apache.hugegraph.backend.cache.CachedSchemaTransaction;
import org.apache.hugegraph.backend.cache.VirtualGraphTransaction;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.SnowflakeIdGenerator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.serializer.SerializerFactory;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendProviderFactory;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStoreSystemInfo;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.SchemaTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.io.HugeGraphIoRegistry;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeFeatures;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.variables.HugeVariables;
import org.apache.hugegraph.vgraph.VirtualGraph;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

/**
 * StandardHugeGraph is the entrance of the graph system, you can modify or
 * query the schema/vertex/edge data through this class.
 */
public class StandardHugeGraph implements HugeGraph {

    public static final Class<?>[] PROTECT_CLASSES = {
            StandardHugeGraph.class,
            StandardHugeGraph.StandardHugeGraphParams.class,
            TinkerPopTransaction.class,
            StandardHugeGraph.Txs.class,
            };

    public static final Set<TypedOption<?, ?>> ALLOWED_CONFIGS = ImmutableSet.of(
            CoreOptions.TASK_WAIT_TIMEOUT,
            CoreOptions.TASK_SYNC_DELETION,
            CoreOptions.TASK_TTL_DELETE_BATCH,
            CoreOptions.TASK_INPUT_SIZE_LIMIT,
            CoreOptions.TASK_RESULT_SIZE_LIMIT,
            CoreOptions.TASK_RETRY,
            CoreOptions.OLTP_CONCURRENT_THREADS,
            CoreOptions.OLTP_CONCURRENT_DEPTH,
            CoreOptions.OLTP_COLLECTION_TYPE,
            CoreOptions.OLTP_QUERY_BATCH_SIZE,
            CoreOptions.OLTP_QUERY_BATCH_AVG_DEGREE_RATIO,
            CoreOptions.OLTP_QUERY_BATCH_EXPECT_DEGREE,
            CoreOptions.VERTEX_DEFAULT_LABEL,
            CoreOptions.STORE_GRAPH,
            CoreOptions.STORE
    );

    private static final Logger LOG = Log.logger(HugeGraph.class);
    private final String name;
    private final StandardHugeGraphParams params;
    private final HugeConfig configuration;
    private final EventHub schemaEventHub;
    private final EventHub graphEventHub;
    private final EventHub indexEventHub;
    private final RateLimiter writeRateLimiter;
    private final RateLimiter readRateLimiter;
    private final TaskManager taskManager;
    private final HugeFeatures features;
    private final BackendStoreProvider storeProvider;
    private final TinkerPopTransaction tx;
    private final String schedulerType;
    private final boolean virtualGraphEnable;
    private final VirtualGraph vGraph;
    private final MetaManager metaManager = MetaManager.instance();
    private volatile boolean started;
    private volatile boolean closed;
    private volatile GraphMode mode;
    private volatile GraphReadMode readMode;
    private volatile HugeVariables variables;
    private String graphSpace;
    private String nickname;
    private String creator;
    private Date createTime;
    private Date updateTime;
    private Boolean checkAdjacentVertexExist = null;

    public StandardHugeGraph(HugeConfig config) {

        /**
         * pd.peers passed from config is required for the case of isolated instantiate
         */
        if (config.containsKey("pd.peers")) {
            String pdPeers = config.getString("pd.peers");
            BrokerConfig.setPdPeers(pdPeers);
        }

        this.params = new StandardHugeGraphParams();
        this.configuration = config;
        this.graphSpace = config.get(CoreOptions.GRAPH_SPACE);

        this.schemaEventHub = new EventHub("schema");
        this.graphEventHub = new EventHub("graph");
        this.indexEventHub = new EventHub("index");

        final int writeLimit = config.get(CoreOptions.RATE_LIMIT_WRITE);
        this.writeRateLimiter = writeLimit > 0 ?
                                RateLimiter.create(writeLimit) : null;
        final int readLimit = config.get(CoreOptions.RATE_LIMIT_READ);
        this.readRateLimiter = readLimit > 0 ?
                               RateLimiter.create(readLimit) : null;

        String graphSpace = config.getString("graphSpace");
        if (!StringUtils.isEmpty(graphSpace) && StringUtils.isEmpty(this.graphSpace())) {
            this.graphSpace(graphSpace);
        }

        this.taskManager = TaskManager.instance();

        this.features = new HugeFeatures(this, true);

        this.name = config.get(CoreOptions.STORE);
        this.started = false;
        this.closed = false;
        this.mode = GraphMode.NONE;
        this.readMode = GraphReadMode.valueOf(
                config.get(CoreOptions.GRAPH_READ_MODE));
        this.schedulerType = config.get(CoreOptions.SCHEDULER_TYPE);

        LockUtil.init(this.spaceGraphName());

        try {
            this.storeProvider = this.loadStoreProvider();
        } catch (Exception e) {
            LockUtil.destroy(this.spaceGraphName());
            String message = "Failed to load backend store provider";
            LOG.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }

        try {
            this.tx = new TinkerPopTransaction(this);

            SnowflakeIdGenerator.init(this.params);
            this.taskManager.addScheduler(this.params);
            this.variables = null;
        } catch (Exception e) {
            this.storeProvider.close();
            LockUtil.destroy(this.spaceGraphName());
            throw e;
        }

        virtualGraphEnable = config.get(CoreOptions.VIRTUAL_GRAPH_ENABLE);
        if (virtualGraphEnable) {
            this.vGraph = new VirtualGraph(this.params);
        } else {
            this.vGraph = null;
        }
    }

    @Override
    public BackendStoreProvider storeProvider() {
        return this.storeProvider;
    }

    @Override
    public String graphSpace() {
        return this.graphSpace;
    }

    @Override
    public void graphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String spaceGraphName() {
        return this.graphSpace + "-" + this.name;
    }

    @Override
    public HugeGraph hugegraph() {
        return this;
    }

    @Override
    public HugeGraph hugegraph(boolean required) {
        return this;
    }

    @Override
    public String backend() {
        return this.storeProvider.type();
    }

    @Override
    public String backendVersion() {
        return this.storeProvider.version();
    }

    @Override
    public BackendStoreSystemInfo backendStoreSystemInfo() {
        return new BackendStoreSystemInfo(this.schemaTransaction());
    }

    @Override
    public BackendFeatures backendStoreFeatures() {
        return this.graphTransaction().storeFeatures();
    }

    @Override
    public void serverStarted() {
        LOG.info("Search olap property key for graph '{}'", this.name);
        this.schemaTransaction().initAndRegisterOlapTables();

        LOG.info("Restoring incomplete tasks for graph '{}'...", this.name);
        this.taskScheduler().restoreTasks();

        this.started = true;
    }

    @Override
    public boolean started() {
        return this.started;
    }

    @Override
    public void started(boolean started) {
        this.started = started;
    }

    @Override
    public boolean closed() {
        if (this.closed && !this.tx.closed()) {
            LOG.warn("The tx is not closed while graph '{}' is closed", this);
        }
        return this.closed;
    }

    @Override
    public GraphMode mode() {
        return this.mode;
    }

    @Override
    public void mode(GraphMode mode) {
        if (this.mode.loading() && mode == GraphMode.NONE &&
            "rocksdb".equalsIgnoreCase(this.backend())) {
            // Flush WAL to sst file after load data for rocksdb backend
            this.metadata(null, "flush");
        }
        LOG.info("Graph {} will work in {} mode", this, mode);
        this.mode = mode;
    }

    @Override
    public GraphReadMode readMode() {
        return this.readMode;
    }

    @Override
    public void readMode(GraphReadMode readMode) {
        if (this.readMode == readMode) {
            return;
        }
        this.clearVertexCache();
        this.readMode = readMode;
    }

    @Override
    public void waitStarted() {
        // Just for trigger Tx.getOrNewTransaction, then load 3 stores
        this.schemaTransaction();
        this.storeProvider.waitStoreStarted();
    }

    @Override
    public void initBackend() {
        LOG.info("Initializing Graph '{}' ...", this.name);
        this.loadGraphStore().open(this.configuration);

        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.init();
            this.storeProvider.initSystemInfo(this);
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
            this.loadGraphStore().close();
        }

        LOG.info("Graph '{}' has been initialized", this.name);
    }

    @Override
    public void clearBackend() {
        try {
            this.waitUntilAllTasksCompleted();
        } catch (Throwable t) {
            LOG.warn("raise error({}) when clear backend of graph({}/{})",
                     t.getMessage(), name(), graphSpace());
        }

        this.loadGraphStore().open(this.configuration);

        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.clear();
            this.schemaTransaction().clear();
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
            this.loadGraphStore().close();
        }

        LOG.info("Graph '{}' has been cleared", this.name);
    }

    @Override
    public void truncateGraph() {
        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.truncateGraph(this);
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        }
    }

    @Override
    public void truncateBackend() {
        this.waitUntilAllTasksCompleted();

        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.truncate();
            this.schemaTransaction().clear();
            this.schemaTransaction().resetIdCounter();
            this.storeProvider.initSystemInfo(this);
            this.serverStarted();
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        }

        LOG.info("Graph '{}' has been truncated", this.name);
    }

    private void clearVertexCache() {
        Future<?> future = this.graphEventHub.notify(Events.CACHE, "clear",
                                                     HugeType.VERTEX);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: vertex cache " +
                     "clear", e);
        }
    }

    private SchemaTransaction openSchemaTransaction() throws HugeException {
        this.checkGraphNotClosed();
        try {
            return new CachedSchemaTransaction(
                    MetaManager.instance().metaDriver(),
                    MetaManager.instance().cluster(), this.params);
        } catch (BackendException e) {
            String message = "Failed to open schema transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private GraphTransaction openGraphTransaction() throws HugeException {
        // Open a new one
        this.checkGraphNotClosed();
        try {
            if (virtualGraphEnable) {
                return new VirtualGraphTransaction(this.params, loadGraphStore());
            } else {
                return new CachedGraphTransaction(this.params, loadGraphStore());
            }
        } catch (BackendException e) {
            String message = "Failed to open graph transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private void checkGraphNotClosed() {
        E.checkState(!this.closed, "Graph '%s' has been closed", this);
    }

    private BackendStore loadGraphStore() {
        String name = this.configuration.get(CoreOptions.STORE_GRAPH);
        return this.storeProvider.loadGraphStore(name);
    }


    @Override
    @Watched
    public SchemaTransaction schemaTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: each schema operation will be auto committed,
         * Don't need to open tinkerpop tx by readWrite() and commit manually.
         */
        return this.tx.schemaTransaction();
    }


    @Watched
    private GraphTransaction graphTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: graph operations must be committed manually,
         * Maybe users need to auto open tinkerpop tx by readWrite().
         */
        this.tx.readWrite();
        return this.tx.graphTransaction();
    }

    private BackendStoreProvider loadStoreProvider() {
        return BackendProviderFactory.open(this.params);
    }

    private AbstractSerializer serializer() {
        String name = this.configuration.get(CoreOptions.SERIALIZER);
        LOG.debug("Loading serializer '{}' for graph '{}'", name, this.name);
        return SerializerFactory.serializer(name);
    }

    private Analyzer analyzer() {
        String name = this.configuration.get(CoreOptions.TEXT_ANALYZER);
        String mode = this.configuration.get(CoreOptions.TEXT_ANALYZER_MODE);
        LOG.debug("Loading text analyzer '{}' with mode '{}' for graph '{}'",
                  name, mode, this.name);
        return AnalyzerFactory.analyzer(name, mode);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> clazz)
            throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).onMapper(mapper ->
                                                        mapper.addRegistry(
                                                                HugeGraphIoRegistry.instance())
        ).create();
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction().addVertex(keyValues);
    }

    @Override
    public Vertex addVertex(Vertex vertex) {
        return this.graphTransaction().addVertex((HugeVertex) vertex);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        this.graphTransaction().removeVertex((HugeVertex) vertex);
    }

    @Override
    public void removeVertex(String label, Object id) {
        if (label != null) {
            VertexLabel vl = this.vertexLabel(label);
            // It's OK even if exist adjacent edges `vl.existsLinkLabel()`
            if (!vl.existsIndexLabel()) {
                // Improve perf by removeVertex(id)
                Id idValue = HugeVertex.getIdValue(id);
                HugeVertex vertex = new HugeVertex(this, idValue, vl);
                this.removeVertex(vertex);
                return;
            }
        }

        this.vertex(id).remove();
    }

    @Override
    public <V> void addVertexProperty(VertexProperty<V> p) {
        this.graphTransaction().addVertexProperty((HugeVertexProperty<V>) p);
    }

    @Override
    public <V> void removeVertexProperty(VertexProperty<V> p) {
        this.graphTransaction().removeVertexProperty((HugeVertexProperty<V>) p);
    }

    @Override
    public Edge addEdge(Edge edge) {
        return this.graphTransaction().addEdge((HugeEdge) edge);
    }

    @Override
    public void canAddEdge(Edge edge) {
        // pass
    }

    @Override
    public void removeEdge(Edge edge) {
        this.graphTransaction().removeEdge((HugeEdge) edge);
    }

    @Override
    public void removeEdge(String label, Object id) {
        if (label != null) {
            EdgeLabel el = this.edgeLabel(label);
            if (!el.existsIndexLabel()) {
                // Improve perf by removeEdge(id)
                Id idValue = HugeEdge.getIdValue(id, false);
                HugeEdge edge = new HugeEdge(this, idValue, el);
                this.removeEdge(edge);
                return;
            }
        }

        this.edge(id).remove();
    }

    @Override
    public <V> void addEdgeProperty(Property<V> p) {
        this.graphTransaction().addEdgeProperty((HugeEdgeProperty<V>) p);
    }

    @Override
    public <V> void removeEdgeProperty(Property<V> p) {
        this.graphTransaction().removeEdgeProperty((HugeEdgeProperty<V>) p);
    }

    @Override
    public Vertex vertex(Object id) {
        return this.graphTransaction().queryVertex(id);
    }

    @Override
    public Iterator<Vertex> vertices(Object... ids) {
        if (ids.length == 0) {
            return this.graphTransaction().queryVertices();
        }
        return this.graphTransaction().queryVertices(ids);
    }

    @Override
    public Iterator<Vertex> vertices(Query query) {
        return this.graphTransaction().queryVertices(query);
    }

    @Override
    public Iterator<Vertex> adjacentVertex(Object id) {
        return this.graphTransaction().queryAdjacentVertices(id);
    }

    @Override
    public Iterator<Vertex> adjacentVertexWithProp(Object... ids) {
        return this.graphTransaction().adjacentVertexWithProp(ids);
    }

    @Override
    public boolean checkAdjacentVertexExist() {
        if (this.checkAdjacentVertexExist == null) {
            this.checkAdjacentVertexExist =
                    this.graphTransaction().checkAdjacentVertexExist();
        }
        return this.checkAdjacentVertexExist;
    }

    @Override
    public Edge edge(Object id) {
        return this.graphTransaction().queryEdge(id);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds.length == 0) {
            return this.graphTransaction().queryEdges();
        }
        return this.graphTransaction().queryEdges(edgeIds);
    }

    @Override
    @Watched
    public Iterator<Edge> edges(Query query) {
        return this.graphTransaction().queryEdges(query);
    }

    @Override
    @Watched
    public Iterator<CIter<Edge>> edges(Iterator<Query> queryList) {
        return this.graphTransaction().queryEdges(queryList);
    }

    @Override
    @Watched
    public CIter<EdgeId> edgeIds(Query query) {
        return this.graphTransaction().queryEdgeIds(query);
    }

    @Override
    @Watched
    public Iterator<CIter<EdgeId>> edgeIds(Iterator<Query> queryList) {
        return this.graphTransaction().queryEdgeIds(queryList);
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        return this.graphTransaction().queryAdjacentVertices(edges);
    }

    @Override
    public Iterator<Edge> adjacentEdges(Id vertexId) {
        return this.graphTransaction().queryEdgesByVertex(vertexId);
    }

    @Override
    public Number queryNumber(Query query) {
        return this.graphTransaction().queryNumber(query);
    }

    @Override
    public Id addPropertyKey(PropertyKey pkey) {
        assert this.name.equals(pkey.graph().name());
        if (pkey.olap()) {
            this.clearVertexCache();
        }
        // this.metaManager.addPropertyKey(this.graphSpace, this.name, pkey);
        return this.schemaTransaction().addPropertyKey(pkey);
    }

    @Override
    public void updatePropertyKey(PropertyKey pkey) {
        assert this.name.equals(pkey.graph().name());
        // this.metaManager.updatePropertyKey(pkey);
        this.schemaTransaction().updatePropertyKey(pkey);
    }

    @Override
    public Id removePropertyKey(Id pkey) {
        if (this.propertyKey(pkey).olap()) {
            this.clearVertexCache();
        }
        return this.schemaTransaction().removePropertyKey(pkey);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        return this.schemaTransaction().getPropertyKeys();
    }

    @Override
    public PropertyKey propertyKey(Id id) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(id);
        E.checkArgument(pk != null, "Undefined property key with id: '%s'", id);
        return pk;
    }

    @Override
    public PropertyKey propertyKey(String name) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(name);
        E.checkArgument(pk != null,
                        "{\"code\":\"103-000\"," +
                        "\"attach\":[\"%s\"]," +
                        "\"message\":\"Undefined property key: '%s'\"}",
                        name, name);
        return pk;
    }

    @Override
    public Id clearPropertyKey(PropertyKey propertyKey) {
        if (propertyKey.oltp()) {
            return IdGenerator.ZERO;
        }
        this.clearVertexCache();
        return this.schemaTransaction().clearOlapPk(propertyKey);
    }

    @Override
    public boolean existsPropertyKey(String name) {
        return this.schemaTransaction().getPropertyKey(name) != null;
    }

    @Override
    public boolean existsOlapTable(PropertyKey pkey) {
        return this.schemaTransaction().existOlapTable(pkey.id());
    }

    @Override
    public void addVertexLabel(VertexLabel vertexLabel) {
        assert this.name.equals(vertexLabel.graph().name());
        // this.metaManager.addVertexLabel(this.graphSpace, this.name,vertexLabel);
        this.schemaTransaction().addVertexLabel(vertexLabel);
    }

    @Override
    public void updateVertexLabel(VertexLabel label) {
        assert this.name.equals(label.graph().name());
        // this.metaManager.updateVertexLabel(this.graphSpace, this.name,
        // label);
        this.schemaTransaction().updateVertexLabel(label);
    }

    @Override
    public Id removeVertexLabel(Id label) {
        return this.schemaTransaction().removeVertexLabel(label);
    }

    @Override
    public Collection<VertexLabel> vertexLabels() {
        return this.schemaTransaction().getVertexLabels();
    }

    @Override
    @Watched
    public VertexLabel vertexLabelOrNone(Id id) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(id);
        if (vl == null) {
            vl = VertexLabel.undefined(this, id);
        }
        return vl;
    }

    @Override
    public VertexLabel vertexLabel(Id id) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(id);
        E.checkArgument(vl != null, "Undefined vertex label with id: '%s'", id);
        return vl;
    }

    @Override
    public VertexLabel vertexLabel(String name) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(name);
        E.checkArgument(vl != null,
                        "{\"code\":\"103-001\"," +
                        "\"attach\":[\"%s\"]," +
                        "\"message\":\"Undefined vertex label: '%s'\"}",
                        name, name);
        return vl;
    }

    @Override
    public boolean existsVertexLabel(String name) {
        return this.schemaTransaction().getVertexLabel(name) != null;
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        List<EdgeLabel> edgeLabels = this.schemaTransaction().getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(vertexLabel)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addEdgeLabel(EdgeLabel label) {
        assert this.name.equals(label.graph().name());
        // this.metaManager.addEdgeLabel(this.graphSpace, this.name, label);
        this.schemaTransaction().addEdgeLabel(label);
    }

    @Override
    public void updateEdgeLabel(EdgeLabel label) {
        assert this.name.equals(label.graph().name());
        this.schemaTransaction().updateEdgeLabel(label);
    }

    @Override
    public Id removeEdgeLabel(Id id) {
        return this.schemaTransaction().removeEdgeLabel(id);
    }

    @Override
    public Collection<EdgeLabel> edgeLabels() {
        return this.schemaTransaction().getEdgeLabels();
    }

    @Override
    public EdgeLabel edgeLabelOrNone(Id id) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(id);
        if (el == null) {
            el = EdgeLabel.undefined(this, id);
        }
        return el;
    }

    @Override
    public EdgeLabel edgeLabel(Id id) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(id);
        E.checkArgument(el != null, "Undefined edge label with id: '%s'", id);
        return el;
    }

    @Override
    public EdgeLabel edgeLabel(String name) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(name);
        E.checkArgument(el != null,
                        "{\"code\":\"103-002\"," +
                        "\"attach\":[\"%s\"]," +
                        "\"message\":\"Undefined edge label: '%s'\"}",
                        name, name);
        return el;
    }

    @Override
    public boolean existsEdgeLabel(String name) {
        return this.schemaTransaction().getEdgeLabel(name) != null;
    }

    @Override
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        assert VertexLabel.OLAP_VL.equals(schemaLabel) ||
               this.name.equals(schemaLabel.graph().name());
        assert this.name.equals(indexLabel.graph().name());
        //  this.metaManager.addIndexLabel(this.graphSpace, this.name, indexLabel);
        this.schemaTransaction().addIndexLabel(schemaLabel, indexLabel);
    }

    @Override
    public void updateIndexLabel(IndexLabel label) {
        assert this.name.equals(label.graph().name());
        this.schemaTransaction().updateIndexLabel(label);
    }

    @Override
    public Id removeIndexLabel(Id id) {
        return this.schemaTransaction().removeIndexLabel(id);
    }

    @Override
    public Id rebuildIndex(SchemaElement schema) {
        return this.schemaTransaction().rebuildIndex(schema);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        return this.schemaTransaction().getIndexLabels();
    }

    @Override
    public IndexLabel indexLabel(Id id) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(id);
        E.checkArgument(il != null, "Undefined index label with id: '%s'", id);
        return il;
    }

    @Override
    public IndexLabel indexLabel(String name) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(name);
        E.checkArgument(il != null, "Undefined index label: '%s'", name);
        return il;
    }

    @Override
    public boolean existsIndexLabel(String name) {
        return this.schemaTransaction().getIndexLabel(name) != null;
    }

    @Override
    public Transaction tx() {
        return this.tx;
    }

    @Override
    public synchronized void close() throws Exception {
        AuthContext.useAdmin();
        if (this.closed()) {
            return;
        }

        LOG.info("Close graph {}", this);
        this.taskManager.closeScheduler(this.params);
        if (this.vGraph != null) {
            this.vGraph.close();
        }
        if ("rocksdb".equalsIgnoreCase(this.backend())) {
            this.metadata(null, "flush");
        }
        try {
            this.closeTx();
        } finally {
            this.closed = true;
            this.storeProvider.close();
            LockUtil.destroy(this.spaceGraphName());
        }
        // Make sure that all transactions are closed in all threads
        E.checkState(this.tx.closed(),
                     "Ensure tx closed in all threads when closing graph '%s'",
                     this.name);
    }

    public void clearSchedulerAndLock() {
        this.taskManager.forceRemoveScheduler(this.params);
        try {
            LockUtil.destroy(this.spaceGraphName());
        } catch (Exception e) {
            // Ignore
        }
    }

    @Override
    public HugeFeatures features() {
        return this.features;
    }

    @Override
    public synchronized Variables variables() {
        if (this.variables == null) {
            this.variables = new HugeVariables(this.params);
        }
        // Ensure variables() work after variables schema was cleared
        this.variables.initSchemaIfNeeded();
        return this.variables;
    }

    @Override
    public SchemaManager schema() {
        return new SchemaManager(this.schemaTransaction(), this);
    }

    @Override
    public Id getNextId(HugeType type) {
        return this.schemaTransaction().getNextId(type);
    }

    @Override
    public <T> T metadata(HugeType type, String meta, Object... args) {
        return this.graphTransaction().metadata(type, meta, args);
    }

    @Override
    public TaskScheduler taskScheduler() {
        TaskScheduler scheduler = this.taskManager.getScheduler(this.params);
        E.checkState(scheduler != null,
                     "Can't find task scheduler for graph '%s'", this);
        return scheduler;
    }

    @Override
    public AuthManager authManager() {
        throw new HugeException("Not support authManager");
    }

    @Override
    public void switchAuthManager(AuthManager authManager) {
    }

    @Override
    public HugeConfig configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.name());
    }

    @Override
    public final void proxy(HugeGraph graph) {
        this.params.graph(graph);
    }

    @Override
    public boolean sameAs(HugeGraph graph) {
        return this == graph;
    }

    @Override
    public long now() {
        return ((TinkerPopTransaction) this.tx()).openedTime();
    }

    @Override
    public <K, V> V option(TypedOption<K, V> option) {
        HugeConfig config = this.configuration();
        if (!ALLOWED_CONFIGS.contains(option)) {
            throw new NotAllowException("Not allowed to access config: %s",
                                        option.name());
        }
        return config.get(option);
    }

    @Override
    public void closeTx() {
        try {
            if (this.tx.isOpen()) {
                this.tx.close();
            }
        } finally {
            this.tx.destroyTransaction();
        }
    }

    private void waitUntilAllTasksCompleted() {
        long timeout = this.configuration.get(CoreOptions.TASK_WAIT_TIMEOUT);
        try {
            this.taskScheduler().waitUntilAllTasksCompleted(timeout);
        } catch (TimeoutException e) {
            throw new HugeException("Failed to wait all tasks to complete", e);
        }
    }

    @Override
    public void applyMutation(BackendMutation mutation) {
        this.graphTransaction().applyMutation(mutation);
    }

    @Override
    public void updatePropertyKey(PropertyKey old, PropertyKey update) {
        this.schemaTransaction().updatePropertyKey(old, update);
    }

    @Override
    public String nickname() {
        return this.nickname;
    }

    @Override
    public void nickname(String nickname) {
        this.nickname = nickname;
    }

    @Override
    public String creator() {
        return this.creator;
    }

    @Override
    public void creator(String creator) {
        this.creator = creator;
    }

    @Override
    public Date createTime() {
        return this.createTime;
    }

    @Override
    public void createTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public Date updateTime() {
        return this.updateTime;
    }

    @Override
    public void updateTime(Date updateTime) {
        this.updateTime = updateTime;

    }

    @Override
    public void refreshUpdateTime() {
        this.updateTime = new Date();
    }

    private static final class Txs {

        private final SchemaTransaction schemaTx;
        private final GraphTransaction graphTx;
        private long openedTime;

        public Txs(SchemaTransaction schemaTx,
                   GraphTransaction graphTx) {
            assert schemaTx != null && graphTx != null;
            this.schemaTx = schemaTx;
            this.graphTx = graphTx;
            this.openedTime = DateUtil.now().getTime();
        }

        public void commit() {
            this.graphTx.commit();
        }

        public void rollback() {
            this.graphTx.rollback();
        }

        public void close() {
            try {
                this.graphTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close GraphTransaction", e);
            }
        }

        public void openedTime(long time) {
            this.openedTime = time;
        }

        public long openedTime() {
            return this.openedTime;
        }

        @Override
        public String toString() {
            return String.format("{schemaTx=%s,graphTx=%s}",
                                 this.schemaTx, this.graphTx);
        }
    }

    private class StandardHugeGraphParams implements HugeGraphParams {

        private HugeGraph graph = StandardHugeGraph.this;

        private void graph(HugeGraph graph) {
            this.graph = graph;
        }

        @Override
        public HugeGraph graph() {
            return this.graph;
        }

        @Override
        public String name() {
            return StandardHugeGraph.this.name();
        }

        @Override
        public GraphMode mode() {
            return StandardHugeGraph.this.mode();
        }

        @Override
        public GraphReadMode readMode() {
            return StandardHugeGraph.this.readMode();
        }

        @Override
        public SchemaTransaction schemaTransaction() {
            return StandardHugeGraph.this.schemaTransaction();
        }

        @Override
        public GraphTransaction graphTransaction() {
            return StandardHugeGraph.this.graphTransaction();
        }

        @Override
        public GraphTransaction openTransaction() {
            // Open a new one
            return StandardHugeGraph.this.openGraphTransaction();
        }

        @Override
        public void closeTx() {
            StandardHugeGraph.this.closeTx();
        }

        @Override
        public boolean started() {
            return StandardHugeGraph.this.started();
        }

        @Override
        public boolean closed() {
            return StandardHugeGraph.this.closed();
        }

        @Override
        public boolean initialized() {
            return StandardHugeGraph.this.graphTransaction().storeInitialized();
        }

        @Override
        public BackendFeatures backendStoreFeatures() {
            return StandardHugeGraph.this.backendStoreFeatures();
        }

        @Override
        public BackendStore loadGraphStore() {
            return StandardHugeGraph.this.loadGraphStore();
        }

        @Override
        public EventHub schemaEventHub() {
            return StandardHugeGraph.this.schemaEventHub;
        }

        @Override
        public EventHub graphEventHub() {
            return StandardHugeGraph.this.graphEventHub;
        }

        @Override
        public EventHub indexEventHub() {
            return StandardHugeGraph.this.indexEventHub;
        }

        @Override
        public HugeConfig configuration() {
            return StandardHugeGraph.this.configuration();
        }

        @Override
        public AbstractSerializer serializer() {
            return StandardHugeGraph.this.serializer();
        }

        @Override
        public Analyzer analyzer() {
            return StandardHugeGraph.this.analyzer();
        }

        @Override
        public RateLimiter writeRateLimiter() {
            return StandardHugeGraph.this.writeRateLimiter;
        }

        @Override
        public RateLimiter readRateLimiter() {
            return StandardHugeGraph.this.readRateLimiter;
        }

        @Override
        public VirtualGraph vGraph() {
            return StandardHugeGraph.this.vGraph;
        }

        @Override
        public String schedulerType() {
            return StandardHugeGraph.this.schedulerType;
        }
    }

    private class TinkerPopTransaction extends AbstractThreadLocalTransaction {

        // Times opened from upper layer
        private final AtomicInteger refs;
        // Flag opened of each thread
        private final ThreadLocal<Boolean> opened;
        // Backend transactions
        private final ThreadLocal<Txs> transactions;

        public TinkerPopTransaction(Graph graph) {
            super(graph);

            this.refs = new AtomicInteger();
            this.opened = ThreadLocal.withInitial(() -> false);
            this.transactions = ThreadLocal.withInitial(() -> null);
        }

        public boolean closed() {
            int refs = this.refs.get();
            assert refs >= 0 : refs;
            return refs == 0;
        }

        /**
         * Commit tx if batch size reaches the specified value,
         * it may be used by Gremlin
         */
        @SuppressWarnings("unused")
        public void commitIfGtSize(int size) {
            // Only commit graph transaction data (schema auto committed)
            this.graphTransaction().commitIfGtSize(size);
        }

        @Override
        public void commit() {
            try {
                super.commit();
            } finally {
                this.setClosed();
            }
        }

        @Override
        public void rollback() {
            try {
                super.rollback();
            } finally {
                this.setClosed();
            }
        }

        @Override
        public <G extends Graph> G createThreadedTx() {
            throw Transaction.Exceptions.threadedTransactionsNotSupported();
        }

        @Override
        public boolean isOpen() {
            return this.opened.get();
        }

        @Override
        protected void doOpen() {
            this.getOrNewTransaction();
            this.setOpened();
        }

        @Override
        protected void doCommit() {
            this.verifyOpened();
            this.getOrNewTransaction().commit();
        }

        @Override
        protected void doRollback() {
            this.verifyOpened();
            this.getOrNewTransaction().rollback();
        }

        @Override
        protected void doClose() {
            this.verifyOpened();

            try {
                // Calling super.doClose() will clear listeners
                super.doClose();
            } finally {
                this.resetState();
            }
        }

        @Override
        public String toString() {
            return String.format("TinkerPopTransaction{opened=%s, txs=%s}",
                                 this.opened.get(), this.transactions.get());
        }

        public long openedTime() {
            return this.transactions.get().openedTime();
        }

        private void verifyOpened() {
            if (!this.isOpen()) {
                throw new HugeException("Transaction has not been opened");
            }
        }

        private void resetState() {
            this.setClosed();
            this.readWriteConsumerInternal.set(READ_WRITE_BEHAVIOR.AUTO);
            this.closeConsumerInternal.set(CLOSE_BEHAVIOR.ROLLBACK);
        }

        private void setOpened() {
            // The backend tx may be reused, here just set a flag
            assert !this.opened.get();
            this.opened.set(true);
            this.transactions.get().openedTime(DateUtil.now().getTime());
            this.refs.incrementAndGet();
        }

        private void setClosed() {
            // Just set flag opened=false to reuse the backend tx
            if (this.opened.get()) {
                this.opened.set(false);
                this.refs.decrementAndGet();
            }
        }

        private SchemaTransaction schemaTransaction() {
            return this.getOrNewTransaction().schemaTx;
        }

        private GraphTransaction graphTransaction() {
            return this.getOrNewTransaction().graphTx;
        }

        private Txs getOrNewTransaction() {
            /*
             * NOTE: this method may be called even tx is not opened,
             * the reason is for reusing backend tx.
             * so we don't call this.verifyOpened() here.
             */

            Txs txs = this.transactions.get();
            if (txs == null) {
                // TODO: close SchemaTransaction if GraphTransaction is error
                txs = new Txs(openSchemaTransaction(),
                              openGraphTransaction());
                this.transactions.set(txs);
            }
            return txs;
        }

        private void destroyTransaction() {
            if (this.isOpen()) {
                throw new HugeException(
                        "Transaction should be closed before destroying");
            }

            // Do close if needed, then remove the reference
            Txs txs = this.transactions.get();
            if (txs != null) {
                txs.close();
            }
            this.transactions.remove();
        }
    }
}