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

import static org.apache.hugegraph.auth.StandardAuthManager.DEFAULT_SETTER_ROLE_KEY;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.security.RolesAllowed;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeRole;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/auth/roles")
@Singleton
public class RoleAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonRole jsonRole) {
        LOG.debug("Graph space [{}] create role: {}", graphSpace, jsonRole);
        checkCreatingBody(jsonRole);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeRole role = jsonRole.build(graphSpace);
        AuthManager authManager = manager.authManager();
        E.checkArgument(!HugeDefaultRole.isDefault(role.name()) &&
                        !HugeDefaultRole.isDefaultNickname(role.nickname()),
                        "Create role name or nickname like default role is " +
                        "forbidden");
        role.id(authManager.createRole(graphSpace, role, false));
        return manager.serializer().writeAuthElement(role);
    }

    public boolean isExistedNickname(AuthManager authManager,
                                     String graphSpace, String nickname) {
        if (StringUtils.isEmpty(nickname)) {
            return false;
        }

        List<HugeRole> roles = authManager.listAllRoles(graphSpace, -1, false);
        for (HugeRole role : roles) {
            if (nickname.equals(role.nickname())) {
                return true;
            }
        }

        return HugeDefaultRole.isDefaultNickname(nickname);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("id") String id,
                         JsonRole jsonRole) {
        LOG.debug("Graph space [{}] update role: {}", graphSpace, jsonRole);
        checkUpdatingBody(jsonRole);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeRole role;
        AuthManager authManager = manager.authManager();
        try {
            role = authManager.getRole(graphSpace, UserAPI.parseId(id), false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid role id: " + id);
        }
        role = jsonRole.build(role);
        E.checkArgument(!HugeDefaultRole.isDefault(role.name()),
                        "Update default role is forbidden");
        E.checkArgument(StringUtils.isEmpty(jsonRole.nickname) ||
                        !isExistedNickname(authManager, graphSpace,
                                           role.nickname()),
                        "Role nickname is existed");
        role = authManager.updateRole(graphSpace, role, false);
        return manager.serializer().writeAuthElement(role);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph space [{}] list roles", graphSpace);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        List<HugeRole> roles = manager.authManager().listAllRoles(graphSpace,
                                                                  limit, false);
        return manager.serializer().writeAuthElements("roles",
                                                      filterDefaultRoles(roles));
    }

    public List<HugeRole> filterDefaultRoles(List<HugeRole> roles) {
        List<HugeRole> filteredRoles = roles.stream().filter(
                                                    (HugeRole role) ->
                                                            !role.name().endsWith(DEFAULT_SETTER_ROLE_KEY))
                                            .collect(Collectors.toList());
        return filteredRoles;
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOG.debug("Graph space [{}] get role: {}", graphSpace, id);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        AuthManager authManager = manager.authManager();
        HugeRole role = authManager.getRole(graphSpace, IdGenerator.of(id),
                                            false);
        return manager.serializer().writeAuthElement(role);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"analyst"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOG.debug("Graph space [{}] delete role: {}", graphSpace, id);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);
        E.checkArgument(!HugeDefaultRole.isDefault(id),
                        "Delete default role is forbidden");

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteRole(graphSpace, IdGenerator.of(id), false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid role id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "role_creator",
                                   "role_create", "role_update"})
    private static class JsonRole implements Checkable {

        @JsonProperty("role_name")
        private String name;
        @JsonProperty("role_nickname")
        private String nickname;
        @JsonProperty("role_description")
        private String description;

        public HugeRole build(HugeRole role) {
            E.checkArgument(this.name == null || role.name().equals(this.name),
                            "The name of role can't be updated");
            if (this.description != null) {
                role.description(this.description);
            }
            if (StringUtils.isNotEmpty(this.nickname)) {
                GraphManager.checkNickname(this.nickname);
                role.nickname(this.nickname);
            }
            return role;
        }

        public HugeRole build(String graphSpace) {
            HugeRole role = new HugeRole(this.name, graphSpace);
            role.description(this.description);
            if (StringUtils.isNotEmpty(this.nickname)) {
                GraphManager.checkNickname(this.nickname);
                role.nickname(this.nickname);
            } else {
                role.nickname(this.name);
            }
            return role;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            GraphManager.checkAuthName(this.name);
        }

        @Override
        public void checkUpdate() {
        }
    }
}
