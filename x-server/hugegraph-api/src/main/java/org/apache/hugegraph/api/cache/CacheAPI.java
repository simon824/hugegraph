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

package org.apache.hugegraph.api.cache;

import static org.apache.hugegraph.api.API.APPLICATION_JSON;
import static org.apache.hugegraph.api.API.APPLICATION_JSON_WITH_CHARSET;

import java.util.Map;

import javax.annotation.security.RolesAllowed;

import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/graph/cache")
@Singleton
public class CacheAPI {

    private static final Logger LOGGER
            = Log.logger(RestServer.class);
    private static final String CACHE_ACTION = "action";
    private static final String TARGET = "target";
    private static final String CACHE_ACTION_CLEAR = "clear";
    private static final String CACHE_ACTION_RELOAD = "reload";

    @PUT
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public Map<String, String> manage(
            @Context GraphManager manager,
            @PathParam("graphspace") String graphSpace,
            @PathParam("graph") String graph,
            Map<String, Object> actionMap) {
        LOGGER.debug("Manage cache with action '{}'", actionMap);
        E.checkArgument(actionMap != null && actionMap.size() == 2 &&
                        actionMap.containsKey(CACHE_ACTION),
                        "Invalid request body '%s'", actionMap);
        Object value = actionMap.get(CACHE_ACTION);
        E.checkArgument(value instanceof String,
                        "Invalid action type '%s', must be string",
                        value.getClass());
        String action = (String) value;
        if (action.equals("clear")) {
            E.checkArgument(actionMap.containsKey(TARGET),
                            "Please pass '%s' for cache manage", TARGET);
            value = actionMap.get(TARGET);
            E.checkArgument(value instanceof String,
                            "The '%s' must be String, but got %s",
                            TARGET, value.getClass());
            Target target = Target.valueOf((String) value);
            switch (target) {
                case GRAPH:
                    MetaManager.instance().notifyGraphCacheClear(graphSpace,
                                                                 graph);
                    break;
                case SCHEMA:
                    MetaManager.instance().notifySchemaCacheClear(graphSpace,
                                                                  graph);
                    break;
                case ALL:
                    MetaManager.instance().notifyGraphCacheClear(graphSpace,
                                                                 graph);
                    MetaManager.instance().notifySchemaCacheClear(graphSpace,
                                                                  graph);
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Invalid target type '%s'", target));
            }

            return ImmutableMap.of(target.name(), "cleared");
        }
        throw new AssertionError(String.format(
                "Invalid graph action: '%s'", action));
    }

    enum Target {

        SCHEMA,
        GRAPH,
        ALL
    }
}
