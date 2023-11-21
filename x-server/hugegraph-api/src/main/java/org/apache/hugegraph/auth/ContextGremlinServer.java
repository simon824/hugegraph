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

package org.apache.hugegraph.auth;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.Context;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.ContextThreadPoolExecutor;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.ThreadFactoryUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;

/**
 * GremlinServer with custom ServerGremlinExecutor, which can pass Context
 */
public class ContextGremlinServer extends GremlinServer {

    private static final Logger LOGGER = Log.logger(GremlinServer.class);

    private static final String G_PREFIX = "__g_";

    private final EventHub eventHub;

    public ContextGremlinServer(final Settings settings, EventHub eventHub) {
        /*
         * pass custom Executor https://github.com/apache/tinkerpop/pull/813
         */
        super(settings, newGremlinExecutorService(settings));
        this.eventHub = eventHub;
        this.listenChanges();
    }

    static ExecutorService newGremlinExecutorService(Settings settings) {
        if (settings.gremlinPool == 0) {
            settings.gremlinPool = CoreOptions.CPUS;
        }
        int size = settings.gremlinPool;
        ThreadFactory factory = ThreadFactoryUtil.create("exec-%d");
        return new ContextThreadPoolExecutor(size, size, factory);
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            HugeGraphAuthProxy.setAdmin();
            event.checkArgs(String.class, HugeGraph.class);
            String name = (String) event.args()[0];
            HugeGraph graph = (HugeGraph) event.args()[1];
            this.injectGraph(name, graph);
            LOGGER.info("[SERVER] Create graph with graph space {}, name {}", graph.graphSpace(),
                        name);
            HugeGraphAuthProxy.resetContext();
            return null;
        });
        this.eventHub.listen(Events.GRAPH_DROP, event -> {
            HugeGraphAuthProxy.setAdmin();
            event.checkArgs(String.class);
            String name = (String) event.args()[0];
            this.removeGraph(name);
            LOGGER.info("[SERVER] Remove graph with name {}", name);
            HugeGraphAuthProxy.resetContext();
            return null;
        });
    }

    private void unlistenChanges() {
        this.eventHub.unlisten(Events.GRAPH_CREATE);
        this.eventHub.unlisten(Events.GRAPH_DROP);
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        try {
            return super.stop();
        } finally {
            this.unlistenChanges();
        }
    }

    public void injectAuthGraph() {
        HugeGraphAuthProxy.setContext(Context.admin());

        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        for (String name : manager.getGraphNames()) {
            Graph graph = manager.getGraph(name);
            graph = new HugeGraphAuthProxy((HugeGraph) graph);
            manager.putGraph(name, graph);
        }
    }

    public void injectTraversalSource() {
        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        for (String graph : manager.getGraphNames()) {
            GraphTraversalSource g = manager.getGraph(graph).traversal();
            String gName = G_PREFIX + graph;
            if (manager.getTraversalSource(gName) != null) {
                throw new HugeException(
                        "Found existing name '%s' in global bindings, " +
                        "it may lead to gremlin query error.", gName);
            }
            // Add a traversal source for all graphs with customed rule.
            manager.putTraversalSource(gName, g);
        }
    }

    public void injectGraph(String name, HugeGraph graph) {
        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        GremlinExecutor executor = this.getServerGremlinExecutor()
                                       .getGremlinExecutor();
        manager.putGraph(name, graph);

        GraphTraversalSource g = manager.getGraph(name).traversal();
        manager.putTraversalSource(G_PREFIX + name, g);
        Whitebox.invoke(executor, "globalBindings",
                        new Class<?>[]{String.class, Object.class},
                        "put", name, graph);
    }

    private void removeGraph(String name) {
        GraphManager manager = this.getServerGremlinExecutor()
                                   .getGraphManager();
        GremlinExecutor executor = this.getServerGremlinExecutor()
                                       .getGremlinExecutor();
        try {
            manager.removeGraph(name);
            manager.removeTraversalSource(G_PREFIX + name);
            Whitebox.invoke(executor, "globalBindings",
                            new Class<?>[]{Object.class},
                            "remove", name);
        } catch (Exception e) {
            throw new HugeException("Failed to remove graph '%s' from " +
                                    "gremlin server context", e, name);
        }
    }
}