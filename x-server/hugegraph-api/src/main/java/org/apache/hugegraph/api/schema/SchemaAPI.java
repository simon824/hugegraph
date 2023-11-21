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

package org.apache.hugegraph.api.schema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/schema")
@Singleton
public class SchemaAPI extends API {

    private static final Logger LOG = Log.logger(SchemaAPI.class);

    private static final String SCHEMA = "schema";
    private static final String JSON = "json";
    private static final String GROOVY = "groovy";

    private static final Set<String> FORMATS = ImmutableSet.of(JSON, GROOVY);

    public static String getSchema(GraphManager manager, String graphSpace,
                                   String graph, String format) {
        HugeGraph g = graph(manager, graphSpace, graph);
        SchemaManager schema = g.schema();

        List<PropertyKey> propertyKeys = schema.getPropertyKeys();
        List<VertexLabel> vertexLabels = schema.getVertexLabels();
        List<EdgeLabel> edgeLabels = schema.getEdgeLabels();
        List<IndexLabel> indexLabels = schema.getIndexLabels();

        if (JSON.equals(format)) {
            Map<String, List<?>> schemaMap = new LinkedHashMap<>(4);
            schemaMap.put("propertykeys", propertyKeys);
            schemaMap.put("vertexlabels", vertexLabels);
            schemaMap.put("edgelabels", edgeLabels);
            schemaMap.put("indexlabels", indexLabels);
            return manager.serializer().writeMap(schemaMap);
        } else {
            StringBuilder builder = new StringBuilder();
            for (PropertyKey propertyKey : propertyKeys) {
                builder.append(propertyKey.convert2Groovy())
                       .append("\n");
            }
            if (!propertyKeys.isEmpty()) {
                builder.append("\n");
            }
            for (VertexLabel vertexLabel : vertexLabels) {
                builder.append(vertexLabel.convert2Groovy())
                       .append("\n");
            }
            if (!vertexLabels.isEmpty()) {
                builder.append("\n");
            }
            for (EdgeLabel edgeLabel : edgeLabels) {
                builder.append(edgeLabel.convert2Groovy())
                       .append("\n");
            }
            if (!edgeLabels.isEmpty()) {
                builder.append("\n");
            }
            for (IndexLabel indexLabel : indexLabels) {
                builder.append(indexLabel.convert2Groovy())
                       .append("\n");
            }
            return manager.serializer().writeMap(
                    ImmutableMap.of("schema", builder.toString()));
        }
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$graphspace=$graphspace $owner=$graph " +
                            "$action=schema_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("format")
                       @DefaultValue("json") String format) {
        LOG.debug("Graph [{}] list all schema with format '{}'", graph, format);

        E.checkArgument(FORMATS.contains(format),
                        "Invalid format '%s', valid format is '%s' or '%s'",
                        format, JSON, GROOVY);
        return getSchema(manager, graphSpace, graph, format);
    }

    @PUT
    @Timed
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$graphspace=$graphspace $owner=$graph " +
                            "$action=schema_write"})
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @QueryParam("action") String action,
                         Map<String, String> schemaMap) {
        LOG.debug("Graph [{}] set schema '{}'", graph, schemaMap);

        HugeGraph g = graph(manager, graphSpace, graph);
        E.checkArgument(schemaMap.size() == 1 &&
                        schemaMap.containsKey(SCHEMA),
                        "Must provide 'schema' in request body");
        GraphManager.prepareSchema(g, schemaMap.get(SCHEMA));
        return manager.serializer().writeMap(ImmutableMap.of(SCHEMA,
                                                             "inited"));
    }
}
