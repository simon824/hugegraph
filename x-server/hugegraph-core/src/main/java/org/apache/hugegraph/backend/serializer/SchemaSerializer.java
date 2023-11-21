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
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;

public interface SchemaSerializer {

    public BackendEntry writeVertexLabel(VertexLabel vertexLabel);

    public VertexLabel readVertexLabel(HugeGraph graph, BackendEntry entry);

    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel);

    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry entry);

    public BackendEntry writePropertyKey(PropertyKey propertyKey);

    public PropertyKey readPropertyKey(HugeGraph graph, BackendEntry entry);

    public BackendEntry writeIndexLabel(IndexLabel indexLabel);

    public IndexLabel readIndexLabel(HugeGraph graph, BackendEntry entry);
}
