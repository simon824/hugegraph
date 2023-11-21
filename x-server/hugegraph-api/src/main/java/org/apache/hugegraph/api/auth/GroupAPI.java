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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
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

@Path("auth/groups")
@Singleton
public class GroupAPI extends API {
    public static final String ALL_GRAPH_SPACES = "*";
    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String create(@Context GraphManager manager,
                         JsonGroup jsonGroup) {
        LOG.debug("Create group: {}", jsonGroup);
        checkCreatingBody(jsonGroup);
        HugeGroup group = jsonGroup.build();
        AuthManager authManager = manager.authManager();
        group.id(authManager.createGroup(group, false));
        return manager.serializer().writeAuthElement(group);
    }

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @Path("batch/{id}")
    @RolesAllowed({"admin"})
    public Map<String, Object> batchLoad(@Context GraphManager manager,
                                         @PathParam("id") String id,
                                         Map<String, Object> action) {
        List<String> users = JsonUtil.fromJson(
                JsonUtil.toJson(action.get("users")), List.class);
        AuthManager authManager = manager.authManager();
        try {
            authManager.findGroup(id, false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }

        List<String> existedBelongs = new ArrayList<>();
        List<String> newBelongs = new ArrayList<>();
        String update = (String) action.get("action");
        for (String user : users) {
            HugeBelong belong = new HugeBelong(IdGenerator.of(user),
                                               IdGenerator.of(id));
            belong.setId();
            List<Id> ids = Arrays.asList(belong.id());
            List<HugeBelong> exist = authManager.listBelong(ALL_GRAPH_SPACES,
                                                            ids,
                                                            false);
            if (exist.size() != 0) {
                existedBelongs.add(belong.id().asString());
                if ("load".equals(update)) {
                    continue;
                }
            } else {
                newBelongs.add(belong.id().asString());
                if ("remove".equals(update)) {
                    continue;
                }
            }
            switch (update) {
                case "load":
                    LOG.debug("Load users to group: {}", id);
                    authManager.createBelong(ALL_GRAPH_SPACES, belong, false);
                    break;
                case "remove":
                    LOG.debug("Remove users from group: {}", id);
                    authManager.deleteBelong(ALL_GRAPH_SPACES, belong.id(), false);
                    break;
                default:
                    throw new AssertionError(String.format("Invalid action: '%s'",
                                                           update));
            }
        }

        Map<String, Object> result = new HashMap<>();
        switch (update) {
            case "load":
                result.put("loaded", newBelongs);
                result.put("existed", existedBelongs);
                break;
            case "remove":
                result.put("removed", existedBelongs);
                result.put("invalid", newBelongs);
                break;
            default:
                throw new AssertionError(String.format("Invalid action: '%s'",
                                                       update));
        }
        return result;
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String update(@Context GraphManager manager,
                         @PathParam("id") String id,
                         JsonGroup jsonGroup) {
        LOG.debug("Update group: {}", jsonGroup);
        checkUpdatingBody(jsonGroup);

        HugeGroup group;
        AuthManager authManager = manager.authManager();
        try {
            group = authManager.findGroup(id, false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }
        group = jsonGroup.build(group);
        group = authManager.updateGroup(group, false);
        return manager.serializer().writeAuthElement(group);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @QueryParam("user") String user,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("List groups");
        AuthManager authManager = manager.authManager();
        boolean required = !isManager(authManager, authManager.username());

        List<HugeGroup> groups;
        if (StringUtils.isEmpty(user)) {
            groups = manager.authManager().listGroups(limit, required);
        } else {
            groups = manager.authManager().listGroupsByUser(user, limit,
                                                            required);
        }
        return manager.serializer().writeAuthElements("groups", groups);
    }

    public boolean isManager(AuthManager authManager, String username) {
        return authManager.isAdminManager(username) ||
               authManager.isSpaceManager(username) ||
               authManager.isDefaultRole(username, HugeDefaultRole.ANALYST);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("id") String id) {
        LOG.debug("Get group: {}", id);

        AuthManager authManager = manager.authManager();
        boolean required = !isManager(authManager, authManager.username());
        HugeGroup group = authManager.findGroup(id, required);
        return manager.serializer().writeAuthElement(group);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @PathParam("id") String id) {
        LOG.debug("Delete group: {}", id);

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteGroup(IdGenerator.of(id), false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"group_creator",
                                   "group_create", "group_update"})
    private static class JsonGroup implements Checkable {

        @JsonProperty("group_name")
        private String name;
        @JsonProperty("group_nickname")
        private String nickname;
        @JsonProperty("group_description")
        private String description;

        public HugeGroup build(HugeGroup group) {
            E.checkArgument(this.name == null || group.name().equals(this.name),
                            "The name of group can't be updated");
            if (this.description != null) {
                group.description(this.description);
            }
            if (this.nickname != null) {
                group.nickname(this.nickname);
            }
            return group;
        }

        public HugeGroup build() {
            String name = (HugeGroup.isGroup(this.name)) ? this.name :
                          HugeGroup.ID_PREFIX + this.name;
            HugeGroup group = new HugeGroup(name);
            group.description(this.description);
            String nickname = (StringUtils.isNotEmpty(this.nickname)) ?
                              this.nickname : name;
            group.nickname(nickname);
            return group;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of group can't be null");
            E.checkArgument(!this.name.contains("->"),
                            "The name of group contains illegal phrase");
            E.checkArgument(!this.name.startsWith(">"),
                            "The name of group cannot start with '>'");
        }

        @Override
        public void checkUpdate() {
        }
    }
}
