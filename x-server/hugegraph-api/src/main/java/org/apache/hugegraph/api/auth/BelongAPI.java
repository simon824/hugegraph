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
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeDefaultRole;
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

@Path("graphspaces/{graphspace}/auth/belongs")
@Singleton
public class BelongAPI extends API {

    private static final Logger LOGGER = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonBelong jsonBelong) {
        LOGGER.debug("Graph space [{}] create belong: {}", graphSpace, jsonBelong);
        checkCreatingBody(jsonBelong);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeBelong belong = jsonBelong.build(graphSpace);
        AuthManager authManager = manager.authManager();
        String role = belong.target().asString();
        if (HugeDefaultRole.isDefault(role)) {
            if (!authManager.isAdminManager(authManager.username()) &&
                HugeDefaultRole.SPACE.toString().equals(role)) {
                throw new HugeException("Forbidden to set role %s", role);
            }
        }
        belong.id(authManager.createBelong(graphSpace, belong, false));
        return manager.serializer().writeAuthElement(belong);
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
                         JsonBelong jsonBelong) {
        LOGGER.debug("Graph space [{}] update belong: {}", graphSpace, jsonBelong);
        checkUpdatingBody(jsonBelong);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeBelong belong;
        AuthManager authManager = manager.authManager();
        try {
            belong = authManager.getBelong(graphSpace,
                                           UserAPI.parseId(id),
                                           false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
        belong = jsonBelong.build(belong);
        belong = authManager.updateBelong(graphSpace, belong, false);
        return manager.serializer().writeAuthElement(belong);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("user") String user,
                       @QueryParam("role") String role,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOGGER.debug("Graph space [{}] list belongs by user {} or role {}", graphSpace, user, role);
        E.checkArgument(user == null || role == null,
                        "Can't pass both user and role at the same time");

        List<HugeBelong> belongs;
        AuthManager authManager = manager.authManager();
        if (user != null) {
            Id id = UserAPI.parseId(user);
            belongs = authManager.listBelongBySource(graphSpace, id, "*",
                                                     limit, false);
        } else if (role != null) {
            Id id = UserAPI.parseId(role);
            belongs = authManager.listBelongByTarget(graphSpace, id, "*",
                                                     limit, false);
        } else {
            belongs = authManager.listAllBelong(graphSpace, limit, false);
        }
        return manager.serializer()
                      .writeAuthElements("belongs",
                                         filterDefaultBelongs(belongs));
    }

    public List<HugeBelong> filterDefaultBelongs(List<HugeBelong> belongs) {
        List<HugeBelong> filteredBelongs = belongs.stream().filter(
                (HugeBelong belong) ->
                        !belong.target().asString()
                               .endsWith(DEFAULT_SETTER_ROLE_KEY)
        ).collect(Collectors.toList());
        return filteredBelongs;
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOGGER.debug("Graph space [{}] get belong: {}", graphSpace, id);

        AuthManager authManager = manager.authManager();
        HugeBelong belong = authManager.getBelong(graphSpace,
                                                  UserAPI.parseId(id),
                                                  false);
        return manager.serializer().writeAuthElement(belong);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"analyst"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOGGER.debug("Graph space [{}] delete belong: {}", graphSpace, id);

        try {
            AuthManager authManager = manager.authManager();
            String role = id.split("->")[2];
            if (HugeDefaultRole.isDefault(role)) {
                if (!authManager.isAdminManager(authManager.username()) &&
                    HugeDefaultRole.SPACE.toString().equals(role)) {
                    throw new HugeException("Forbidden to delete role %s",
                                            role);
                }
            }
            authManager.deleteBelong(graphSpace, UserAPI.parseId(id), false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "belong_creator",
                                   "belong_create", "belong_update"})
    private static class JsonBelong implements Checkable {

        @JsonProperty("user")
        private String user;
        @JsonProperty("group")
        private String group;
        @JsonProperty("role")
        private String role;
        @JsonProperty("belong_description")
        private String description;
        @JsonProperty("link")
        private String link = HugeBelong.UR;

        public HugeBelong build(HugeBelong belong) {
            E.checkArgument(HugeBelong.isLink(this.link),
                            "Link must in 'ur', 'ug' and 'gr'");

            E.checkArgument(this.source() == null ||
                            belong.source().equals(
                                    UserAPI.parseId(this.source())),
                            "The source of belong can't be updated");
            E.checkArgument(this.target() == null ||
                            belong.target().equals(
                                    UserAPI.parseId(this.target())),
                            "The target of belong can't be updated");
            if (this.description != null) {
                belong.description(this.description);
            }
            return belong;
        }

        public HugeBelong build(String graphSpace) {
            HugeBelong belong;
            String link = (StringUtils.isEmpty(this.link)) ? HugeBelong.UR :
                          this.link;
            belong = new HugeBelong(graphSpace,
                                    getId(this.user),
                                    getId(this.group),
                                    getId(this.role),
                                    link);
            belong.description(this.description);
            return belong;
        }

        public Id getId(String id) {
            return (StringUtils.isEmpty(id)) ? null : UserAPI.parseId(id);
        }

        public String source() {
            if (HugeBelong.GR.equals(this.link)) {
                return this.group;
            }
            return this.user;
        }

        public String target() {
            if (HugeBelong.UG.equals(this.link)) {
                return this.group;
            }
            return this.role;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(HugeBelong.isLink(this.link),
                            "Link must in 'ur', 'ug' and 'gr', but got '%s'",
                            this.link);
            E.checkArgumentNotNull(this.source(),
                                   "The source of belong can't be null");
            E.checkArgumentNotNull(this.target(),
                                   "The target of belong can't be null");
        }

        @Override
        public void checkUpdate() {

        }
    }
}
