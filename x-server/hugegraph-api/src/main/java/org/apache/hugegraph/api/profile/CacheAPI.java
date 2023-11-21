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

package org.apache.hugegraph.api.profile;

import java.util.Set;

import javax.annotation.security.RolesAllowed;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.E;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/cache")
@Singleton
public class CacheAPI extends API {

    private static final Set<String> ALLOW_ACTION = ImmutableSet.of(
            "clear"
    );

    @PUT
    @Timed
    @Path("schema")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String schema(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @QueryParam("action") String action) {
        E.checkArgument(ALLOW_ACTION.contains(action),
                        "Invalid action '%s'", action);
        switch (action) {
            case "clear":
                manager.meta().notifySchemaCacheClear(graphSpace, graph);
                break;
            default:
                throw new AssertionError(String.format(
                        "Invalid action '%s'", action));
        }
        return manager.serializer().writeMap(ImmutableMap.of(
                String.join("/", graphSpace, graph), "schema-cache-cleared"
        ));
    }

    @PUT
    @Timed
    @Path("graph")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String graph(@Context GraphManager manager,
                        @PathParam("graphspace") String graphSpace,
                        @PathParam("graph") String graph,
                        @QueryParam("action") String action) {
        E.checkArgument(ALLOW_ACTION.contains(action),
                        "Invalid action '%s'", action);
        switch (action) {
            case "clear":
                manager.meta().notifyGraphCacheClear(graphSpace, graph);
                break;
            default:
                throw new AssertionError(String.format(
                        "Invalid action '%s'", action));
        }
        return manager.serializer().writeMap(ImmutableMap.of(
                String.join("/", graphSpace, graph), "graph-cache-cleared"
        ));
    }
}
