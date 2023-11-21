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

package org.apache.hugegraph.backend.serializer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeIndex;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.type.HugeType;
import org.apache.tinkerpop.gremlin.structure.Edge;

public interface GraphSerializer {

    public BackendEntry writeVertex(HugeVertex vertex);

    public BackendEntry writeOlapVertex(HugeVertex vertex);

    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop);

    public HugeVertex readVertex(HugeGraph graph, BackendEntry entry);

    public HugeVertex readVertex(HugeGraph graph, BackendEntry entry,
                                 boolean withEdgeProperties);

    public BackendEntry writeEdge(HugeEdge edge);

    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop);

    public HugeEdge readEdge(HugeGraph graph, BackendEntry entry);

    public CIter<Edge> readEdges(HugeGraph graph, BackendEntry bytesEntry,
                                 boolean withEdgeProperties,
                                 boolean lightWeight);

    public CIter<EdgeId> readEdgeIds(HugeGraph graph, BackendEntry bytesEntry);

    public BackendEntry writeIndex(HugeIndex index);

    public HugeIndex readIndex(HugeGraph graph, ConditionQuery query,
                               BackendEntry entry);

    public BackendEntry writeId(HugeType type, Id id);

    public Query writeQuery(Query query);
}
