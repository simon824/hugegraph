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

package org.apache.hugegraph.backend.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.HugeTaskResult;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public abstract class AbstractBackendStoreProvider
        implements BackendStoreProvider {

    protected static final Logger LOG = Log.logger(BackendStoreProvider.class);
    protected Map<String, BackendStore> stores = null;
    private String graph = null;
    private EventHub storeEventHub = new EventHub("store");

    public final void notifyAndWaitEvent(String event) {
        Future<?> future = this.storeEventHub.notify(event, this);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: {}", event, e);
        }
    }

    protected final void checkOpened() {
        E.checkState(this.graph != null && this.stores != null,
                     "The BackendStoreProvider has not been opened");
    }

    protected abstract BackendStore newSchemaStore(String store);

    protected abstract BackendStore newGraphStore(String store);

    @Override
    public void listen(EventListener listener) {
        this.storeEventHub.listen(EventHub.ANY_EVENT, listener);
    }

    @Override
    public void unlisten(EventListener listener) {
        this.storeEventHub.unlisten(EventHub.ANY_EVENT, listener);
    }

    @Override
    public String graph() {
        this.checkOpened();
        return this.graph;
    }

    @Override
    public void open(String graph) {
        LOG.debug("Graph '{}' open StoreProvider", this.graph);
        E.checkArgument(graph != null, "The graph name can't be null");
        E.checkArgument(!graph.isEmpty(), "The graph name can't be empty");

        this.graph = graph;
        this.stores = new ConcurrentHashMap<>();

        this.storeEventHub.notify(Events.STORE_OPEN, this);
    }

    @Override
    public void waitStoreStarted() {
        // pass
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("Graph '{}' close StoreProvider", this.graph);
        this.checkOpened();
        this.storeEventHub.notify(Events.STORE_CLOSE, this);
    }

    @Override
    public void init() {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            store.init();
        }
        this.notifyAndWaitEvent(Events.STORE_INIT);

        LOG.debug("Graph '{}' store has been initialized", this.graph);
    }

    @Override
    public void clear() throws BackendException {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            // Just clear tables of store, not clear space
            store.clear(false);
        }
        for (BackendStore store : this.stores.values()) {
            // Only clear space of store
            store.clear(true);
        }
        this.notifyAndWaitEvent(Events.STORE_CLEAR);

        LOG.debug("Graph '{}' store has been cleared", this.graph);
    }

    @Override
    public void truncate() {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            store.truncate();
        }
        this.notifyAndWaitEvent(Events.STORE_TRUNCATE);

        LOG.debug("Graph '{}' store has been truncated", this.graph);
    }

    @Override
    public void initSystemInfo(HugeGraph graph) {
        this.checkOpened();
        BackendStoreSystemInfo info = graph.backendStoreSystemInfo();
        info.init();
        this.notifyAndWaitEvent(Events.STORE_INITED);

        // Init Task Schema
        new HugeTask.Schema(graph).initSchemaIfNeeded();
        new HugeTaskResult.Schema(graph).initSchemaIfNeeded();

        LOG.debug("Graph '{}' system info has been initialized", this.graph);
    }

    @Override
    public void createOlapTable(HugeGraph graph, Id pkId) {
        String g = graph.option(CoreOptions.STORE_GRAPH);
        BackendStore store = this.stores.get(g);
        LOG.debug("Store provider start create olap table {}", pkId);
        store.createOlapTable(pkId);
    }

    @Override
    public void initAndRegisterOlapTable(HugeGraph graph, Id pkId) {
        String g = graph.option(CoreOptions.STORE_GRAPH);
        BackendStore store = this.stores.get(g);
        store.checkAndRegisterOlapTable(pkId);
    }

    @Override
    public void clearOlapTable(HugeGraph graph, Id pkId) {
        String g = graph.option(CoreOptions.STORE_GRAPH);
        BackendStore store = this.stores.get(g);
        store.clearOlapTable(pkId);
    }

    @Override
    public void removeOlapTable(HugeGraph graph, Id pkId) {
        String g = graph.option(CoreOptions.STORE_GRAPH);
        BackendStore store = this.stores.get(g);
        store.removeOlapTable(pkId);
    }

    @Override
    public boolean existOlapTable(HugeGraph graph, Id pkId) {
        String g = graph.option(CoreOptions.STORE_GRAPH);
        BackendStore store = this.stores.get(g);
        return store.existOlapTable(pkId);
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        LOG.debug("The '{}' StoreProvider load SchemaStore '{}'",
                  this.type(), name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = this.newSchemaStore(name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.debug("The '{}' StoreProvider load GraphStore '{}'",
                  this.type(), name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = this.newGraphStore(name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        return store;
    }

    @Override
    public EventHub storeEventHub() {
        return this.storeEventHub;
    }
}