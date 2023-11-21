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

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;

public interface BackendStoreProvider {

    // Backend store type
    public String type();

    // Backend store version
    public String version();

    // Graph name (that's database name)
    public String graph();

    public BackendStore loadSchemaStore(String name);

    public BackendStore loadGraphStore(String name);

    public void open(String name);

    public void waitStoreStarted();

    public void close();

    public void init();

    public void clear();

    public void truncate();

    public default void truncateGraph(HugeGraph graph) {
        throw new UnsupportedOperationException(
                "BackendStoreProvider.truncateGraph()");
    }

    public void initSystemInfo(HugeGraph graph);

    public default void createOlapTable(HugeGraph graph, Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStoreProvider.createOlapTable()");
    }

    public default void initAndRegisterOlapTable(HugeGraph graph, Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStoreProvider.checkAndRegisterOlapTable()");
    }

    public default void clearOlapTable(HugeGraph graph, Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStoreProvider.clearOlapTable()");
    }

    public default void removeOlapTable(HugeGraph graph, Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStoreProvider.removeOlapTable()");
    }

    public default boolean existOlapTable(HugeGraph graph, Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStoreProvider.existOlapTable()");
    }

    public void listen(EventListener listener);

    public void unlisten(EventListener listener);

    public EventHub storeEventHub();
}
