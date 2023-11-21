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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.KneighborTraverser;
import org.apache.hugegraph.traversal.algorithm.records.KneighborRecords;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/egonet")
@Singleton
public class EgonetAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(EgonetAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String sourceV,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_depth") int depth,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get egonet from '{}' with direction '{}', " +
                  "edge label '{}', max depth '{}', max degree '{}' and " +
                  "limit '{}'", graph, sourceV, direction, edgeLabel, depth,
                  maxDegree, limit);
        DebugMeasure measure = new DebugMeasure();
        Id source = VertexAPI.checkAndParseVertexId(sourceV);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graphSpace, graph);

        Set<Id> ids;
        try (KneighborTraverser traverser = new KneighborTraverser(g)) {
            ids = traverser.kneighbor(source, dir, edgeLabel,
                                      depth, maxDegree, limit);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
        }

        return manager.serializer(g, measure.getResult())
                      .writeList("vertices", ids);
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of request can't be null");
        E.checkArgument(request.sources.size() > 0,
                        "The sources of request can't be empty");
        E.checkArgument(request.steps != null,
                        "The steps of request can't be null");
        if (request.countOnly) {
            E.checkArgument(!request.withVertex && !request.withPath,
                            "Can't return vertex or path when count only");
        }

        LOG.debug("Graph [{}] get egonet from source vertex '{}', " +
                  "with steps '{}', limit '{}', count_only '{}', " +
                  "with_vertex '{}', with_path '{}' and with_edge '{}'",
                  graph, request.sources, request.steps, request.limit,
                  request.countOnly, request.withVertex, request.withPath,
                  request.withEdge);
        DebugMeasure measure = new DebugMeasure();
        HugeGraph g = graph(manager, graphSpace, graph);
        Set<Id> sourcesId = new HashSet<>(8 * request.sources.size());
        for (Object source : request.sources) {
            sourcesId.add(HugeVertex.getIdValue(source));
        }

        Steps steps = steps(g, request.steps);

        KneighborRecords results;
        try (KneighborTraverser traverser = new KneighborTraverser(g)) {
            results = traverser.customizedKneighbor(sourcesId, steps,
                                                    request.maxDepth,
                                                    request.limit,
                                                    request.withEdge);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
        }

        Set<Id> neighbors = request.countOnly ?
                            ImmutableSet.of() : results.idSet(request.limit);
        neighbors.addAll(sourcesId);
        long size = neighbors.size();
        if (size > request.limit) {
            size = request.limit;
        }

        HugeTraverser.PathSet paths = new HugeTraverser.PathSet();
        if (request.withPath) {
            paths.addAll(results.paths(request.limit));
        }

        Iterator<Vertex> verticesIter = QueryResults.emptyIterator();
        Iterator<Edge> edgesIter = QueryResults.emptyIterator();
        if (request.withVertex && !request.countOnly) {
            Set<Id> ids = new HashSet<>(neighbors);
            if (request.withPath) {
                for (HugeTraverser.Path p : paths) {
                    ids.addAll(p.vertices());
                }
            }
            if (!ids.isEmpty()) {
                verticesIter = g.vertices(ids.toArray());
                measure.addIterCount(ids.size(), 0);
            }
        }

        if (request.withEdge && !request.countOnly) {
            Iterator<Edge> edges = results.getEdges();
            if (edges == null) {
                Set<Id> ids = results.getEdgeIds();
                if (!ids.isEmpty()) {
                    edges = g.edges(ids.toArray());
                }
            }
            if (edges != null) {
                edgesIter = edges;
            }
        }

        try {
            return manager.serializer(g, measure.getResult())
                          .writeNodesWithPath("egonet",
                                              new ArrayList<>(neighbors),
                                              size, paths, verticesIter,
                                              edgesIter,
                                              null);
        } finally {
            CloseableIterator.closeIterator(verticesIter);
            CloseableIterator.closeIterator(edgesIter);
        }
    }

    private static class Request {

        @JsonProperty("sources")
        public Set<Object> sources;
        @JsonProperty("steps")
        public VESteps steps;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_ELEMENTS_LIMIT);
        @JsonProperty("count_only")
        public boolean countOnly = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_path")
        public boolean withPath = false;
        @JsonProperty("with_edge")
        public boolean withEdge = false;

        @Override
        public String toString() {
            return String.format("PathRequest{source=%s,steps=%s," +
                                 "maxDepth=%s,limit=%s,countOnly=%s," +
                                 "withVertex=%s,withPath=%s,withEdge=%s}",
                                 this.sources, this.steps, this.maxDepth,
                                 this.limit, this.countOnly, this.withVertex,
                                 this.withPath, this.withEdge);
        }
    }
}
