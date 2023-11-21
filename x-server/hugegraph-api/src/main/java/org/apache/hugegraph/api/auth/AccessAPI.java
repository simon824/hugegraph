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

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.backend.id.Id;
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

@Path("graphspaces/{graphspace}/auth/accesses")
@Singleton
public class AccessAPI extends API {

    private static final Logger LOGGER = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonAccess jsonAccess) {
        LOGGER.debug("Graph space [{}] create access: {}", graphSpace, jsonAccess);
        checkCreatingBody(jsonAccess);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);
        String role = jsonAccess.role;
        E.checkArgument(!HugeDefaultRole.isDefault(role),
                        "Update default role is forbidden");
        HugeAccess access = jsonAccess.build(graphSpace);
        AuthManager authManager = manager.authManager();
        access.id(authManager.createAccess(graphSpace, access, false));
        return manager.serializer().writeAuthElement(access);
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
                         JsonAccess jsonAccess) {
        LOGGER.debug("Graph space [{}] update access: {}", graphSpace, jsonAccess);
        checkUpdatingBody(jsonAccess);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);
        HugeAccess access;
        AuthManager authManager = manager.authManager();
        try {
            access = authManager.getAccess(graphSpace,
                                           UserAPI.parseId(id),
                                           false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
        access = jsonAccess.build(access);
        access = authManager.updateAccess(graphSpace, access, false);
        return manager.serializer().writeAuthElement(access);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("role") String role,
                       @QueryParam("target") String target,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOGGER.debug("Graph space [{}] list belongs by role {} or target {}", graphSpace, role,
                     target);
        E.checkArgument(role == null || target == null,
                        "Can't pass both role and target at the same time");

        List<HugeAccess> belongs;
        AuthManager authManager = manager.authManager();
        if (role != null) {
            Id id = UserAPI.parseId(role);
            belongs = authManager.listAccessByRole(graphSpace, id,
                                                   limit, false);
        } else if (target != null) {
            Id id = UserAPI.parseId(target);
            belongs = authManager.listAccessByTarget(graphSpace, id,
                                                     limit, false);
        } else {
            belongs = authManager.listAllAccess(graphSpace, limit, false);
        }
        return manager.serializer().writeAuthElements("accesses", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOGGER.debug("Graph space [{}] get access: {}", graphSpace, id);

        AuthManager authManager = manager.authManager();
        HugeAccess access = authManager.getAccess(graphSpace,
                                                  UserAPI.parseId(id),
                                                  false);
        return manager.serializer().writeAuthElement(access);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"analyst"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOGGER.debug("Graph space [{}] delete access: {}", graphSpace, id);

        try {
            AuthManager authManager = manager.authManager();
            String role = id.split("->")[0];
            E.checkArgument(!HugeDefaultRole.isDefault(role),
                            "Delete default role is forbidden");
            authManager.deleteAccess(graphSpace,
                                     UserAPI.parseId(id),
                                     false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "access_creator",
                                   "access_create", "access_update"})
    private static class JsonAccess implements Checkable {

        @JsonProperty("role")
        private String role;
        @JsonProperty("target")
        private String target;
        @JsonProperty("access_permission")
        private HugePermission permission;
        @JsonProperty("access_description")
        private String description;

        public HugeAccess build(HugeAccess access) {
            E.checkArgument(this.role == null ||
                            access.source().equals(UserAPI.parseId(this.role)),
                            "The role of access can't be updated");
            E.checkArgument(this.target == null ||
                            access.target().equals(UserAPI.parseId(this.target)),
                            "The target of access can't be updated");
            E.checkArgument(this.permission == null ||
                            access.permission().equals(this.permission),
                            "The permission of access can't be updated");
            if (this.description != null) {
                access.description(this.description);
            }
            return access;
        }

        public HugeAccess build(String graphSpace) {
            HugeAccess access = new HugeAccess(graphSpace,
                                               UserAPI.parseId(this.role),
                                               UserAPI.parseId(this.target));
            access.permission(this.permission);
            access.description(this.description);
            return access;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.role,
                                   "The role of access can't be null");
            E.checkArgumentNotNull(this.target,
                                   "The target of access can't be null");
            E.checkArgumentNotNull(this.permission,
                                   "The permission of access can't be null");
            this.permission.checkCreatable();
        }

        @Override
        public void checkUpdate() {
        }
    }
}
