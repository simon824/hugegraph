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

package org.apache.hugegraph.api.traversers;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_ELEMENTS_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.exception.ErrorCode;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.SameNeighborTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/sameneighbors")
@Singleton
public class SameNeighborsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @QueryParam("vertex") String vertex,
                      @QueryParam("other") String other,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get same neighbors between '{}' and '{}' with " +
                  "direction {}, edge label {}, max degree '{}' and limit '{}'",
                  graph, vertex, other, direction, edgeLabel, maxDegree, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(vertex);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        DebugMeasure measure = new DebugMeasure();
        HugeGraph g = graph(manager, graphSpace, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);
        Set<Id> neighbors = traverser.sameNeighbors(sourceId, targetId, dir,
                                                    edgeLabel, maxDegree, limit);
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());
        return manager.serializer(g, measure.getResult()).writeList("same_neighbors", neighbors);
    }


    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String sameNeighbors(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace,
                                @PathParam("graph") String graph,
                                Request req) {
        LOG.debug("Graph [{}] get same neighbors batch, '{}'", graph, req.toString());

        Directions dir = Directions.convert(EdgeAPI.parseDirection(req.direction));

        DebugMeasure measure = new DebugMeasure();
        HugeGraph g = graph(manager, graphSpace, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);
        List<Object> vertexPair = req.vertexPair;
        E.checkArgument(vertexPair.size() >= 2,
                        ErrorCode.VERTEX_PAIR_LENGTH_ERROR.format());

        List<Id> vertexIds = new ArrayList<>();
        for (Object obj : vertexPair) {
            vertexIds.add(HugeVertex.getIdValue(obj));
        }

        Set<Id> neighbors = traverser.sameNeighbors(vertexIds, dir, req.labels,
                                                    req.maxDegree, req.limit);
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());
        List<Id> result = CollectionUtil.toList(neighbors);
        return manager.serializer(g, measure.getResult())
                      .writeList("same_neighbors", result);
    }


    private static class Request {
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_ELEMENTS_LIMIT);
        @JsonProperty("vertex_list")
        private List<Object> vertexPair;
        @JsonProperty("direction")
        private String direction;
        @JsonProperty("labels")
        private List<String> labels;

        @Override
        public String toString() {
            ObjectMapper om = new ObjectMapper();
            String vertexStr = "";
            try {
                vertexStr = om.writeValueAsString(this.vertexPair);
            } catch (Exception e) {
            }
            return String.format("SameNeighborsBatchRequest{vertex=%s,direction=%s,label=%s," +
                                 "max_degree=%d,limit=%d", vertexStr, this.direction,
                                 this.labels, this.maxDegree, this.limit);
        }
    }
}
