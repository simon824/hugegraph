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

/**
 * Splitted from StandardTaskScheduler on 2022-1-4 by Scorpiour
 * Provide general transaction for queries;
 *
 * @since 2022-01-04
 */

package org.apache.hugegraph.task;

import java.util.Iterator;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.task.HugeTask.P;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TaskTransaction extends GraphTransaction {

    public static final String TASK = P.TASK;

    public TaskTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);
    }

    public HugeVertex constructVertex(HugeTask<?> task) {
        if (!this.graph().existsVertexLabel(TASK)) {
            throw new HugeException("Schema is missing for task(%s) '%s'",
                                    task.id(), task.name());
        }
        return this.constructVertex(false, task.asArray());
    }

    public void deleteIndex(HugeVertex vertex) {
        // Delete the old record if exist
        Iterator<Vertex> old = this.queryTaskInfos(vertex.id());
        HugeVertex oldV = (HugeVertex) QueryResults.one(old);
        if (oldV == null) {
            return;
        }
        this.deleteIndexIfNeeded(oldV, vertex);
    }

    private boolean deleteIndexIfNeeded(HugeVertex oldV, HugeVertex newV) {
        if (!oldV.value(P.STATUS).equals(newV.value(P.STATUS))) {
            // Only delete vertex if index value changed else override it
            this.updateIndex(this.indexLabel(P.STATUS).id(), oldV, true);
            return true;
        }
        return false;
    }

    protected String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    protected String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    private String createPropertyKey(String name, DataType dataType,
                                     Cardinality cardinality) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.params().schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    private IndexLabel indexLabel(String field) {
        String name = Hidden.hide("task-index-by-" + field);
        HugeGraph graph = this.graph();
        return graph.indexLabel(name);
    }
}
