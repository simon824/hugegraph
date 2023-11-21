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

package org.apache.hugegraph.api.job;

import java.util.Map;

import javax.annotation.security.RolesAllowed;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.job.CypherJob;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;

@Path("graphspaces/{graphspace}/graphs/{graph}/jobs/cypher")
@Singleton
public class CypherAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);


    private static final Histogram CYPHER_JOB_INPUT_HISTOGRAM =
            MetricsUtil.registerHistogram(CypherAPI.class, "cypher-input");

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$graphspace=$graphspace $owner=$graph " +
                            "$action=gremlin_execute"})
    public Map<String, Id> post(@Context GraphManager manager,
                                @Context HttpHeaders headers,
                                @PathParam("graphspace") String graphSpace,
                                @PathParam("graph") String graph,
                                String cypher) {
        LOG.debug("Graph [{}] schedule cypher job: {}", graph, cypher);
        CYPHER_JOB_INPUT_HISTOGRAM.update(cypher.length());
        HugeGraph g = graph(manager, graphSpace, graph);

        String auth = headers.getHeaderString("ICBC-Authorization");
        if (auth == null) {
            auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        }

        GremlinAPI.GremlinRequest request = new GremlinAPI.GremlinRequest();
        request.gremlin(cypher);
        request.aliase("graph", graph);
        request.aliase("graphspace", graphSpace);
        request.aliase("authorization", auth);
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name(cypher)
               .input(request.toJson())
               .context(HugeGraphAuthProxy.getContextString())
               .job(new CypherJob());
        return ImmutableMap.of("task_id", builder.schedule().id());
    }

}
