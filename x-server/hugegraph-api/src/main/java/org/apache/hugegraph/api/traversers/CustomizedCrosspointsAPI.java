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

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_CAPACITY;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/customizedcrosspoints")
@Singleton
public class CustomizedCrosspointsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static List<CustomizedCrosspointsTraverser.PathPattern>
    pathPatterns(HugeGraph graph, CrosspointsRequest request) {
        int stepSize = request.pathPatterns.size();
        List<CustomizedCrosspointsTraverser.PathPattern> pathPatterns;
        pathPatterns = new ArrayList<>(stepSize);
        for (PathPattern pattern : request.pathPatterns) {
            CustomizedCrosspointsTraverser.PathPattern pathPattern;
            pathPattern = new CustomizedCrosspointsTraverser.PathPattern();
            for (Step step : pattern.steps) {
                pathPattern.add(step.jsonToStep(graph));
            }
            pathPatterns.add(pathPattern);
        }
        return pathPatterns;
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       CrosspointsRequest request) {
        E.checkArgumentNotNull(request,
                               "The crosspoints request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of crosspoints request " +
                               "can't be null");
        E.checkArgument(request.pathPatterns != null &&
                        !request.pathPatterns.isEmpty(),
                        "The steps of crosspoints request can't be empty");

        LOG.debug("Graph [{}] get customized crosspoints from source vertex " +
                  "'{}', with path_pattern '{}', with_path '{}', with_vertex " +
                  "'{}', capacity '{}' and limit '{}'", graph, request.sources,
                  request.pathPatterns, request.withPath, request.withVertex,
                  request.capacity, request.limit);

        DebugMeasure measure = new DebugMeasure();
        HugeGraph g = graph(manager, graphSpace, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        CustomizedCrosspointsTraverser traverser =
                new CustomizedCrosspointsTraverser(g);
        List<CustomizedCrosspointsTraverser.PathPattern> patterns;
        patterns = pathPatterns(g, request);

        CustomizedCrosspointsTraverser.CrosspointsPaths paths;
        try {
            paths = traverser.crosspointsPaths(sources, patterns,
                                               request.capacity,
                                               request.limit);
        } finally {
            CloseableIterator.closeIterator(sources);
        }
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (!request.withVertex) {
            return manager.serializer(g, measure.getResult())
                          .writeCrosspoints(paths, iter, request.withPath);
        }
        Set<Id> ids = new HashSet<>();
        if (request.withPath) {
            for (HugeTraverser.Path p : paths.paths()) {
                ids.addAll(p.vertices());
            }
        } else {
            ids = paths.crosspoints();
        }
        if (!ids.isEmpty()) {
            iter = g.vertices(ids.toArray());
            measure.addIterCount(ids.size(), 0);
        }
        try {
            return manager.serializer(g, measure.getResult())
                          .writeCrosspoints(paths, iter, request.withPath);
        } finally {
            CloseableIterator.closeIterator(iter);
        }
    }

    private static class CrosspointsRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("path_patterns")
        public List<PathPattern> pathPatterns;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_path")
        public boolean withPath = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("CrosspointsRequest{sourceVertex=%s," +
                                 "pathPatterns=%s,withPath=%s,withVertex=%s," +
                                 "capacity=%s,limit=%s}", this.sources,
                                 this.pathPatterns, this.withPath,
                                 this.withVertex, this.capacity, this.limit);
        }
    }

    private static class PathPattern {

        @JsonProperty("steps")
        public List<Step> steps;

        @Override
        public String toString() {
            return String.format("PathPattern{steps=%s", this.steps);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "maxDegree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree);
        }

        private CustomizedCrosspointsTraverser.Step jsonToStep(HugeGraph g) {
            return new CustomizedCrosspointsTraverser.Step(g, this.direction,
                                                           this.labels,
                                                           this.properties,
                                                           this.maxDegree,
                                                           this.skipDegree);
        }
    }
}
