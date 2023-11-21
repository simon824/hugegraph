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

package org.apache.hugegraph.api.auth;

import java.util.List;

import javax.annotation.security.RolesAllowed;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("auth/managers")
@Singleton
public class ManagerAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String createManager(@Context GraphManager manager,
                                JsonManager jsonManager) {
        LOG.debug("Create manager: {}", jsonManager);

        String user = jsonManager.user;
        HugePermission type = jsonManager.type;
        String graphSpace = jsonManager.graphSpace;
        AuthManager authManager = manager.authManager();
        E.checkArgument(type == HugePermission.SPACE ||
                        type == HugePermission.ADMIN,
                        "The type could be 'SPACE' or 'ADMIN'");
        E.checkArgument(authManager.findUser(user, false) != null ||
                        authManager.findGroup(user, false) != null,
                        "The user or group is not exist");

        if (type == HugePermission.SPACE) {
            E.checkArgument(manager.graphSpace(graphSpace) != null,
                            "The graph space is not exist");

            authManager.createSpaceManager(graphSpace, user);
        } else {
            authManager.createAdminManager(user);
        }

        return manager.serializer()
                      .writeMap(ImmutableMap.of("user", user, "type", type,
                                                "graphspace", graphSpace));
    }

    @DELETE
    @Timed
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @QueryParam("user") String user,
                       @QueryParam("type") HugePermission type,
                       @QueryParam("graphspace") String graphSpace) {
        LOG.debug("Delete graph manager: {} {} {}", user, type, graphSpace);
        E.checkArgument(!"admin".equals(user) ||
                        type != HugePermission.ADMIN,
                        "User 'admin' can't be removed from ADMIN");

        AuthManager authManager = manager.authManager();
        E.checkArgument(type == HugePermission.SPACE ||
                        type == HugePermission.ADMIN,
                        "The type could be 'SPACE' or 'ADMIN'");
        E.checkArgument(authManager.findUser(user, false) != null ||
                        authManager.findGroup(user, false) != null,
                        "The user or group is not exist");

        if (type == HugePermission.SPACE) {
            E.checkArgument(manager.graphSpace(graphSpace) != null,
                            "The graph space is not exist");

            authManager.deleteSpaceManager(graphSpace, user);
        } else {
            authManager.deleteAdminManager(user);
        }
    }

    @GET
    @Timed
    @Consumes(APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @QueryParam("type") HugePermission type,
                       @QueryParam("graphspace") String graphSpace) {
        LOG.debug("list graph manager: {} {}", type, graphSpace);

        AuthManager authManager = manager.authManager();
        E.checkArgument(type == HugePermission.SPACE ||
                        type == HugePermission.ADMIN,
                        "The type could be 'SPACE' or 'ADMIN'");

        List<String> adminManagers;
        if (type == HugePermission.SPACE) {
            E.checkArgument(manager.graphSpace(graphSpace) != null,
                            "The graph space is not exist");

            adminManagers = authManager.listSpaceManager(graphSpace);
        } else {
            adminManagers = authManager.listAdminManager();
        }

        return manager.serializer().writeList("admins", adminManagers);
    }

    @GET
    @Timed
    @Path("check")
    @Consumes(APPLICATION_JSON)
    public String checkAdmin(@Context GraphManager manager,
                             @QueryParam("type") HugePermission type,
                             @QueryParam("graphspace") String graphSpace) {
        LOG.debug("check if current user is graph manager: {} {}",
                  type, graphSpace);

        E.checkArgument(type == HugePermission.SPACE ||
                        type == HugePermission.ADMIN,
                        "The type could be 'SPACE' or 'ADMIN'");
        AuthManager authManager = manager.authManager();
        String user = authManager.username();

        boolean result;
        if (type == HugePermission.SPACE) {
            E.checkNotNull(manager.graphSpace(graphSpace), "graphspace");

            result = authManager.isSpaceManager(graphSpace, user);
        } else {
            result = authManager.isAdminManager(user);
        }

        return manager.serializer().writeMap(ImmutableMap.of("check", result));
    }

    @GET
    @Timed
    @Path("default")
    @Consumes(APPLICATION_JSON)
    public String checkDefaultRole(@Context GraphManager manager,
                                   @QueryParam("graphspace") String graphSpace,
                                   @QueryParam("role") String role,
                                   @QueryParam("graph") String graph) {
        LOG.debug("check if current user is default role: {} {} {}",
                  role, graphSpace, graph);
        AuthManager authManager = manager.authManager();
        String user = authManager.username();

        E.checkArgument(StringUtils.isNotEmpty(role) &&
                        StringUtils.isNotEmpty(graphSpace),
                        "Must pass graphspace and role params");

        HugeDefaultRole defaultRole =
                HugeDefaultRole.valueOf(role.toUpperCase());
        boolean hasGraph = defaultRole.equals(HugeDefaultRole.OBSERVER);
        E.checkArgument(!hasGraph || StringUtils.isNotEmpty(graph),
                        "Must set a graph for observer");

        boolean result;
        if (hasGraph) {
            result = authManager.isDefaultRole(graphSpace, graph, user,
                                               defaultRole);
        } else {
            result = authManager.isDefaultRole(graphSpace, user,
                                               defaultRole);
        }
        return manager.serializer().writeMap(ImmutableMap.of("check", result));
    }

    private static class JsonManager implements Checkable {

        @JsonProperty("user")
        private String user;
        @JsonProperty("type")
        private HugePermission type;
        @JsonProperty("graphspace")
        private String graphSpace = "";

        @Override
        public void checkCreate(boolean isBatch) {
        }

        @Override
        public void checkUpdate() {
        }
    }
}
