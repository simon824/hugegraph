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

package org.apache.hugegraph.api.cypher;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.cypher.CypherExcuteClient;
import org.apache.hugegraph.cypher.CypherModel;
import org.apache.hugegraph.util.E;

import com.codahale.metrics.annotation.Timed;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;

/**
 * @author lynn.bond@hotmail.com on 2023/2/22
 */
@Path("graphspaces/{graphspace}/graphs/{graph}/cypher")
@Singleton
public class CypherAPI extends API {

    CypherExcuteClient client = new CypherExcuteClient();

    @GET
    @Timed
    @CompressInterceptor.Compress(buffer = (1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public CypherModel query(@Context HttpHeaders headers,
                             @PathParam("graphspace") String graphspace,
                             @PathParam("graph") String graph,
                             @QueryParam("cypher") String cypher) {

        return this.queryByCypher(headers, graphspace, graph, cypher);
    }

    @POST
    @Timed
    @CompressInterceptor.Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public CypherModel post(@Context HttpHeaders headers,
                            @PathParam("graphspace") String graphspace,
                            @PathParam("graph") String graph,
                            String cypher) {

        return this.queryByCypher(headers, graphspace, graph, cypher);
    }

    private CypherModel queryByCypher(HttpHeaders headers, String graphspace,
                                      String graph, String cypher) {
        E.checkArgument(graphspace != null && !graphspace.isEmpty(),
                        "The graphspace parameter can't be null or empty");
        E.checkArgument(graph != null && !graph.isEmpty(),
                        "The graph parameter can't be null or empty");
        E.checkArgument(cypher != null && !cypher.isEmpty(),
                        "The cypher parameter can't be null or empty");

        String graphInfo = graphspace + "-" + graph;
        Map<String, String> aliases = new HashMap<>(2, 1);
        aliases.put("graph", graphInfo);
        aliases.put("g", "__g_" + graphInfo);

        return this.client.client(headers).submitQuery(cypher, aliases);
    }

}
