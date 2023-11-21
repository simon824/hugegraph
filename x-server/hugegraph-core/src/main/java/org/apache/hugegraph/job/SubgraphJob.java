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

package org.apache.hugegraph.job;

import static org.apache.hugegraph.config.OptionChecker.nonNegativeInt;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.iterator.Metadatable;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.task.StandardTaskScheduler;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.optimize.Text;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotFoundException;

public class SubgraphJob extends UserJob<Object> {

    public static final int TASK_RESULTS_MAX_SIZE = (int) HugeTraverser.DEF_CAPACITY;
    public static final String TASK_TYPE = "subgraph";
    public static final String SUB_INIT = "sub:init";
    public static final ConfigOption<Integer> MAX_WRITE_THREADS =
            new ConfigOption<>(
                    "batch.max_write_threads",
                    "The maximum threads for batch writing, " +
                    "if the value is 0, the actual value will be set to " +
                    "batch.max_write_ratio * restserver.max_worker_threads.",
                    nonNegativeInt(),
                    0);
    private static final Logger LOG = Log.logger(SubgraphJob.class);
    private static final int BUSY_TIME_OUT = 10;
    private static final long SPLIT_SIZE = 67108864L;
    /*
     * NOTE: VertexAPI and EdgeAPI should share a counter
     * */
    private static AtomicInteger batchWriteThreads = new AtomicInteger(0);
    public String taskType;
    public HugeConfig config;
    public AuthManager authManager;
    public HugeGraph origin;
    private int rangeLimit = 8000;
    private int batchSize = 2000;
    private int loopLimit = -1;

    public SubgraphJob(HugeConfig config, AuthManager authManager,
                       HugeGraph origin) {
        this.taskType = TASK_TYPE;
        this.config = config;
        this.authManager = authManager;
        this.origin = origin;
    }

    public static Id checkAndParseVertexId(String idValue) {
        if (idValue == null) {
            return null;
        }
        boolean uuid = idValue.startsWith("U\"");
        if (uuid) {
            idValue = idValue.substring(1);
        }
        try {
            Object id = (idValue.contains(":")) ? idValue :
                        JsonUtil.fromJson(idValue, Object.class);
            return uuid ? Text.uuid((String) id) : HugeVertex.getIdValue(id);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "The vertex id must be formatted as Number/String/UUID" +
                    ", but got '%s'", idValue));
        }
    }

    /*
     * BatchAPI#commit
     * */
    public static <R> R batchCommit(HugeConfig config, HugeGraph g, int size,
                                    Callable<R> callable) {
        int maxWriteThreads = config.get(MAX_WRITE_THREADS);
        for (int retry = 0; retry < BUSY_TIME_OUT &&
                            batchWriteThreads.get() >= maxWriteThreads; retry++) {
            // block the caller...
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int writingThreads = batchWriteThreads.incrementAndGet();

        if (writingThreads > maxWriteThreads) {
            batchWriteThreads.decrementAndGet();
            throw new HugeException("The rest server is too busy to write");
        }

        try {
            R result = commit(g, callable);
            return result;
        } finally {
            batchWriteThreads.decrementAndGet();
        }
    }

    /*
     * API#commit
     * */
    public static <R> R commit(HugeGraph g, Callable<R> callable) {
        Consumer<Throwable> rollback = (error) -> {
            if (error != null) {
            }
            try {
                g.tx().rollback();
            } catch (Throwable e) {
                ;
            }
        };

        try {
            R result = callable.call();
            g.tx().commit();
            return result;
        } catch (IllegalArgumentException | NotFoundException |
                 ForbiddenException e) {
            rollback.accept(null);
            throw e;
        } catch (RuntimeException e) {
            rollback.accept(e);
            throw e;
        } catch (Throwable e) {
            rollback.accept(e);
            // TODO: throw the origin exception 'e'
            throw new HugeException("Failed to commit", e);
        }
    }

    @Override
    public String type() {
        return this.taskType;
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);

        Object value = map.get("type");
        E.checkArgument(value instanceof String,
                        "Invalid type value '%s'", value);
        String type = (String) value;

        // get ids and steps
        value = map.get("ids");
        Object[] ids = null;
        Object conditions = map.get("steps");
        List<Map<String, Object>> steps = null;
        String shardStart = "";
        String shardEnd = "";
        if (value != null) {
            E.checkArgument(value instanceof List,
                            "Invalid ids value '%s' with class %s",
                            value, value.getClass());
            List<String> idsStr = (List<String>) value;
            ids = new HashSet<>(idsStr).toArray();
        } else if (conditions != null) {
            E.checkArgument(conditions instanceof List,
                            "Invalid steps value '%s' with class %s",
                            conditions, conditions.getClass());
            steps = (List<Map<String, Object>>) conditions;
        } else {
            shardStart = map.get("start").toString();
            shardEnd = map.get("end").toString();
        }

        value = map.get("auth");
        boolean auth = (value == null) ? false : (Boolean) value;

        value = map.get("sub_auth");
        boolean subAuth = (value == null) ? false : (Boolean) value;

        value = map.get("batch_size");
        this.batchSize = (Integer) value;

        value = map.get("range");
        this.rangeLimit = (Integer) value;

        value = map.get("loop_limit");
        this.loopLimit = (Integer) value;

        value = map.get("keep_start_p");
        boolean keepStartP = (Boolean) value;

        HugeConfig config = this.config;
        HugeGraph origin = this.origin;
        HugeGraph graph = this.graph();
        String graphSpace = origin.graphSpace();

        Date start = new Date();
        // load vertices and edges
        try {
            switch (type) {
                case "vertex":
                    deriveSubgraphByVertices(config, origin, graph, ids, steps
                            , keepStartP, shardStart, shardEnd);
                    break;
                case "edge":
                    deriveSubgraphByEdges(config, origin, graph, ids, steps,
                                          keepStartP, shardStart, shardEnd);
                    break;
                default:
                    throw new AssertionError(
                            String.format("Invalid type: '%s', valid type is " +
                                          "'vertex' or 'edge'", type));
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when create subraph.", e);
        } finally {
            if (graph.taskScheduler() instanceof StandardTaskScheduler) {
                LOG.info("close sub graph transaction");
                graph.tx().close();
            }

            if (origin.taskScheduler() instanceof StandardTaskScheduler) {
                LOG.info("close original graph transaction");
                origin.tx().close();
            }
        }

        // load permissions
        if (subAuth) {
            authManager.createGraphDefaultRole(graph.graphSpace(),
                                               graph.nickname());
            if (auth) {
                // avoid copying unauthenticated permissions to subgraph
                authManager.copyPermissionsToOther(graphSpace,
                                                   origin.nickname(),
                                                   graph.graphSpace(),
                                                   graph.nickname(),
                                                   auth && subAuth);
            }
        }
        Date stop = new Date();
        LOG.info("Load datas to subgraph total: {} s",
                 (stop.getTime() - start.getTime()) / 1000);
        return 0;
    }

    private void deriveSubgraphByVertices(HugeConfig config, HugeGraph origin,
                                          HugeGraph graph, Object[] ids,
                                          List<Map<String, Object>> steps,
                                          boolean keepStartP, String start,
                                          String end) throws Exception {
        if (ids != null) {
            deriveByVertexIds(config, origin, graph, ids);
        } else if (steps != null) {
            deriveByVertexStep(config, origin, graph, steps, keepStartP);
        } else {
            deriveByVertexShard(config, origin, graph, start, end);
        }
    }

    private void deriveByVertexIds(HugeConfig config,
                                   HugeGraph origin,
                                   HugeGraph graph,
                                   Object[] ids) throws Exception {
        Iterator<Vertex> vertexIterator = QueryResults.emptyIterator();
        Object[] idsFormat = new Id[ids.length];
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] != null && !ids[i].equals("")) {
                idsFormat[i] = checkAndParseVertexId(ids[i].toString());
            }
        }

        List<Object> vidList = new ArrayList<>();
        try {
            vertexIterator = origin.vertices(idsFormat);
            List<Vertex> vertices = listVerticesByIterator(vertexIterator,
                                                           vidList);
            if (vertices.size() == 0) {
                return;
            }
            loadVertices(config, graph, vertices);
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("Invalid vertex ids or " +
                                               "conditions", e);
        } finally {
            CloseableIterator.closeIterator(vertexIterator);
        }

        Object[] vids = vidList.toArray();
        GraphTraversal<?, Edge> traversalE = origin.traversal().V(vids).outE();
        try {
            Iterator<Edge> edgeIterator = traversalE;
            List<Edge> edges = listEdgeByIterator(edgeIterator, vidList, null);
            loadEdges(config, graph, edges, true);
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("Meet exception when " +
                                               "loading datas", e);
        } finally {
            traversalE.close();
        }
    }

    private void deriveByVertexStep(HugeConfig config,
                                    HugeGraph origin,
                                    HugeGraph graph,
                                    List<Map<String, Object>> steps,
                                    boolean keepStartP) throws Exception {
        deriveByVertexStep(config, origin, graph, steps, keepStartP, true);
        deriveByVertexStep(config, origin, graph, steps, keepStartP, false);
    }

    private void deriveByVertexStep(HugeConfig config,
                                    HugeGraph origin,
                                    HugeGraph graph,
                                    List<Map<String, Object>> steps,
                                    boolean keepStartP,
                                    boolean loadVertex) throws Exception {
        for (Map<String, Object> step : steps) {
            String label = null;
            if (step.get("label") != null) {
                label = step.get("label").toString();
            }
            Map<String, Object> props =
                    (Map<String, Object>) step.get("properties");
            // Convert relational operator like P.gt()/P.lt()
            for (Map.Entry<String, Object> prop : props.entrySet()) {
                Object value = prop.getValue();
                if (!keepStartP && value instanceof String &&
                    ((String) value).startsWith(TraversalUtil.P_CALL)) {
                    prop.setValue(TraversalUtil.parsePredicate((String) value));
                }
            }
            int offset = 0;
            int loop = 0;
            while (true) {
                GraphTraversal<?, Vertex> traversal = origin.traversal().V();
                traversal = traversal.range(offset, offset + this.rangeLimit);

                if (label != null) {
                    traversal = traversal.hasLabel(label);
                }

                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    traversal = traversal.has(entry.getKey(), entry.getValue());
                }

                if (loadVertex) {
                    List<Vertex> vertices;
                    try {
                        Iterator<Vertex> vertexIterator = traversal;
                        vertices = listVerticesByIterator(vertexIterator, null);
                        if (vertices.size() == 0) {
                            break;
                        }
                    } catch (NoSuchElementException e) {
                        throw new IllegalArgumentException("Meet exception when " +
                                                           "querying edges", e);
                    } finally {
                        traversal.close();
                    }

                    loadVertices(config, graph, vertices);
                } else {
                    GraphTraversal<?, Edge> traversalE = traversal.outE();
                    try {
                        Iterator<Edge> edgeIterator = traversalE;
                        List<Edge> edges = listEdgeByIterator(edgeIterator, null,
                                                              null);
                        if (edges.size() == 0) {
                            break;
                        }
                        loadEdges(config, graph, edges, true);
                    } catch (NoSuchElementException e) {
                        throw new IllegalArgumentException("Meet exception when " +
                                                           "loading datas", e);
                    } finally {
                        traversalE.close();
                    }
                }
                offset += this.rangeLimit;
                loop += 1;
                if (loopLimit > 0 && loop > loopLimit) {
                    break;
                }
            }
        }
    }

    private void deriveByVertexShard(HugeConfig config,
                                     HugeGraph origin,
                                     HugeGraph graph,
                                     String start,
                                     String end) throws Exception {
        List<Shard> shards = origin.metadata(HugeType.VERTEX, "splits",
                                             SPLIT_SIZE);
        long startLong = Long.parseLong(start);
        long endLong = Long.parseLong(end);
        int loop = 0;
        List<Vertex> vertices;
        for (Shard shard : shards) {
            long sStartLong = Long.parseLong(shard.start());
            long sEndLong = Long.parseLong(shard.end());
            if (sEndLong <= startLong || sStartLong >= endLong) {
                continue;
            }

            // load data by page in one shard
            String page = "";
            while (true) {
                ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
                query.scan(shard.start(), shard.end());
                query.page(page);
                query.limit(this.rangeLimit);
                Iterator<Vertex> vertexIterator = origin.vertices(query);
                List<Object> vidList = new ArrayList<>(rangeLimit);
                vertices = listVerticesByIterator(vertexIterator, vidList);
                if (vertices.size() == 0) {
                    break;
                }

                loadVertices(config, graph, vertices);
                GraphTraversal<?, Edge> traversalE =
                        origin.traversal().V(vidList.toArray()).outE();
                vidList.clear();
                try {
                    Iterator<Edge> edgeIterator = traversalE;
                    List<Edge> edges = listEdgeByIterator(edgeIterator, null, null);
                    loadEdges(config, graph, edges, false);
                } catch (NoSuchElementException e) {
                    throw new IllegalArgumentException("Meet exception when " +
                                                       "loading datas", e);
                } finally {
                    traversalE.close();
                }

                if (vertexIterator instanceof Metadatable) {
                    page = PageInfo.pageInfo(vertexIterator);
                    if (page == null) {
                        break;
                    }
                } else {
                    break;
                }

                loop += 1;
                if (loopLimit > 0 && loop > loopLimit) {
                    break;
                }
            }
        }
    }

    private void deriveSubgraphByEdges(HugeConfig config,
                                       HugeGraph origin,
                                       HugeGraph graph,
                                       Object[] ids,
                                       List<Map<String, Object>> steps,
                                       boolean keepStartP,
                                       String start,
                                       String end) throws Exception {
        if (ids != null) {
            deriveByEdgeIds(config, origin, graph, ids);
        } else if (steps != null) {
            deriveByEdgeStep(config, origin, graph, steps, keepStartP);
        } else {
            deriveByEdgeShard(config, origin, graph, start, end);
        }
    }

    private void deriveByEdgeIds(HugeConfig config, HugeGraph origin,
                                 HugeGraph graph, Object[] ids) {
        Object[] idsFormat = new Id[ids.length];
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] != null && !ids[i].equals("")) {
                idsFormat[i] = HugeEdge.getIdValue(ids[i].toString(), false);
            }
        }

        Iterator<Edge> edgeIterator;
        try {
            edgeIterator = origin.edges(ids);
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("Invalid edge ids or conditions",
                                               e);
        }

        Set<Object> vidSet = new HashSet<>();
        List<Edge> edges = listEdgeByIterator(edgeIterator, null, vidSet);
        Iterator<Vertex> vertexIterator = origin.traversal().V(vidSet.toArray());
        List<Vertex> vertices = listVerticesByIterator(vertexIterator, null);
        loadVertices(config, graph, vertices);
        loadEdges(config, graph, edges, false);
    }

    private void deriveByEdgeStep(HugeConfig config, HugeGraph origin,
                                  HugeGraph graph,
                                  List<Map<String, Object>> steps,
                                  boolean keepStartP) throws Exception {
        for (Map<String, Object> step : steps) {
            int offset = 0;
            String label = null;
            if (step.get("label") != null) {
                label = step.get("label").toString();
            }
            Map<String, Object> props =
                    (Map<String, Object>) step.get("properties");
            // Convert relational operator like P.gt()/P.lt()
            for (Map.Entry<String, Object> prop : props.entrySet()) {
                Object value = prop.getValue();
                if (!keepStartP && value instanceof String &&
                    ((String) value).startsWith(TraversalUtil.P_CALL)) {
                    prop.setValue(TraversalUtil.parsePredicate((String) value));
                }
            }

            int loop = 0;
            while (true) {
                GraphTraversal<?, Edge> traversal = origin.traversal().E();
                traversal = traversal.range(offset, offset + this.rangeLimit);

                if (label != null) {
                    traversal = traversal.hasLabel(label);
                }

                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    traversal = traversal.has(entry.getKey(), entry.getValue());
                }

                Set<Object> vidSet = new HashSet<>();
                List<Edge> edges;
                try {
                    Iterator<Edge> edgeIterator = traversal;
                    edges = listEdgeByIterator(edgeIterator, null, vidSet);
                    if (edges.size() == 0) {
                        break;
                    }
                    offset += this.rangeLimit;
                } catch (NoSuchElementException e) {
                    throw new IllegalArgumentException("Meet exception when " +
                                                       "querying edges", e);
                } finally {
                    traversal.close();
                }

                GraphTraversal<?, Vertex> traversalV = origin.traversal().V(vidSet.toArray());
                try {
                    Iterator<Vertex> vertexIterator = traversalV;
                    List<Vertex> vertices =
                            listVerticesByIterator(vertexIterator, null);
                    loadVertices(config, graph, vertices);
                    loadEdges(config, graph, edges, false);
                } catch (NoSuchElementException e) {
                    throw new IllegalArgumentException("Meet exception when " +
                                                       "loading datas", e);
                } finally {
                    traversalV.close();
                }
                loop += 1;
                if (loopLimit > 0 && loop > loopLimit) {
                    break;
                }
            }
        }
    }

    private void deriveByEdgeShard(HugeConfig config, HugeGraph origin,
                                   HugeGraph graph, String start,
                                   String end) throws Exception {
        List<Shard> shards = origin.metadata(HugeType.EDGE_OUT, "splits",
                                             SPLIT_SIZE);
        long startLong = Long.parseLong(start);
        long endLong = Long.parseLong(end);
        int loop = 0;
        for (Shard shard : shards) {
            long sStartLong = Long.parseLong(shard.start());
            long sEndLong = Long.parseLong(shard.end());
            if (sEndLong <= startLong || sStartLong >= endLong) {
                continue;
            }
            String page = "";
            while (true) {
                ConditionQuery query = new ConditionQuery(HugeType.EDGE_OUT);
                query.scan(shard.start(), shard.end());
                query.page(page);
                query.limit(this.rangeLimit);

                Iterator<Edge> edgeIterator = origin.edges(query);
                Set<Object> vidSet = new HashSet<>();
                List<Edge> edges;
                edges = listEdgeByIterator(edgeIterator, null, vidSet);
                if (edges.size() == 0) {
                    break;
                }

                GraphTraversal<?, Vertex> traversalV =
                        origin.traversal().V(vidSet.toArray());
                try {
                    Iterator<Vertex> vertexIterator = traversalV;
                    List<Vertex> vertices =
                            listVerticesByIterator(vertexIterator, null);
                    loadVertices(config, graph, vertices);
                    loadEdges(config, graph, edges, false);
                } catch (NoSuchElementException e) {
                    throw new IllegalArgumentException("Meet exception when " +
                                                       "loading datas", e);
                } finally {
                    traversalV.close();
                }

                if (edgeIterator instanceof Metadatable) {
                    page = PageInfo.pageInfo(edgeIterator);
                    if (page == null) {
                        break;
                    }
                } else {
                    break;
                }

                loop += 1;
                if (loopLimit > 0 && loop > loopLimit) {
                    break;
                }
            }
        }
    }

    private List<Vertex> listVerticesByIterator(Iterator<?> vertexIterator,
                                                List<Object> vids) {
        List<Vertex> vertices = new ArrayList<>();
        boolean add = (vids != null);
        try {
            while (vertexIterator.hasNext()) {
                Vertex vertex = (Vertex) vertexIterator.next();
                vertices.add(vertex);
                if (add) {
                    vids.add(vertex.id());
                }
            }
        } finally {
            CloseableIterator.closeIterator(vertexIterator);
        }
        return vertices;
    }

    private List<Edge> listEdgeByIterator(Iterator<?> edgeIterator,
                                          List<Object> bothV,
                                          Set<Object> vids) {
        List<Edge> edges = new ArrayList<>();
        try {
            while (edgeIterator.hasNext()) {
                Edge edge = (Edge) edgeIterator.next();
                Object outId = edge.outVertex().id();
                Object inId = edge.inVertex().id();
                /*
                 * filter edges whose related vertices are in bothV list
                 * bothV != null only in case deriveByIds
                 * */
                if (bothV != null && !(bothV.contains(outId) &&
                                       bothV.contains(inId))) {
                    continue;
                }
                // collect related vertex ids
                if (vids != null) {
                    vids.add(outId);
                    vids.add(inId);
                }
                edges.add(edge);
            }
        } finally {
            CloseableIterator.closeIterator(edgeIterator);
        }
        return edges;
    }

    private Object[] loadVertices(HugeConfig config, HugeGraph graph,
                                  List<Vertex> vertices) {
        Date start = new Date();
        int size = vertices.size();
        int begin = 0;
        int end = 0;
        List<Id> vids = new ArrayList<>();
        while (begin < size) {
            if (size > begin + this.batchSize) {
                end = begin + this.batchSize;
            } else {
                end = size;
            }
            List<Vertex> batchV = vertices.subList(begin, end);
            Set<Id> vidBatch =
                    batchCommit(config, graph, batchV.size(), () -> {
                        Set<Id> newIds = new HashSet<>(batchV.size());
                        for (Vertex vertex : batchV) {
                            newIds.add((Id) graph.addVertex(vertex).id());
                        }
                        return newIds;
                    });
            vids.addAll(vidBatch);
            begin += this.batchSize;
        }
        Date stop = new Date();
        // LOG.info("Load {} vertices duration: {} s", vertices.size(),
        //        (stop.getTime() - start.getTime()) / 1000);
        return vids.toArray();
    }

    public void loadEdges(HugeConfig config, HugeGraph g, List<Edge> edges,
                          boolean checkVertex) {
        Date start = new Date();
        int size = edges.size();
        int begin = 0;
        int end;
        while (begin < size) {
            if (size > begin + this.batchSize) {
                end = begin + this.batchSize;
            } else {
                end = size;
            }
            List<Edge> batchE = edges.subList(begin, end);
            batchCommit(config, g, batchE.size(), () -> {
                if (!checkVertex) {
                    for (Edge edge : batchE) {
                        g.addEdge(edge);
                    }
                } else {
                    loadValidEdges(g, edges);
                }
                return batchE.size();
            });
            begin += this.batchSize;
        }
        Date stop = new Date();
        // LOG.info("load {} edges duration: {} s", edges.size(),
        //        (stop.getTime() - start.getTime()) / 1000);
    }

    private void loadValidEdges(HugeGraph graph, List<Edge> edges) {
        int size = edges.size();
        Object[] ids = new Id[size];
        int index = 0;
        for (Edge edge : edges) {
            ids[index++] = edge.inVertex().id();
        }

        Iterator<Vertex> vertexIterator;

        // graph.vertices(ids) result not keep ids sequence now!
        vertexIterator = graph.vertices(ids);

        Map<Object, HugeVertex> vertexMap = new HashMap<>();
        try {
            while (vertexIterator.hasNext()) {
                HugeVertex vertex = (HugeVertex) vertexIterator.next();
                vertexMap.put(vertex.id().asObject(), vertex);
            }
        } finally {
            CloseableIterator.closeIterator(vertexIterator);
        }

        for (int i = 0; i < size; i++) {
            Object id = ids[i];
            Object key;
            if (id instanceof String) {
                key = id;
            } else {
                key = IdGenerator.of(id).asObject();
            }
            HugeVertex vertex = vertexMap.get(key);
            if (vertex != null) {
                graph.addEdge(edges.get(i));
            }
        }
    }
}

