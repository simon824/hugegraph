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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeResource;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.hugegraph.auth.SchemaDefine;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
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

@Path("graphspaces/{graphspace}/auth/targets")
@Singleton
public class TargetAPI extends API {

    private static final Logger LOGGER = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonTarget jsonTarget) {
        LOGGER.debug("Graph space [{}] create target: {}", graphSpace, jsonTarget);
        checkCreatingBody(jsonTarget);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeTarget target = jsonTarget.build(graphSpace);
        E.checkArgument(!HugeDefaultRole.isDefaultTarget(target.name()),
                        "Create target with default suffix is forbidden");

        AuthManager authManager = manager.authManager();
        target.id(authManager.createTarget(graphSpace, target, false));
        return manager.serializer().writeAuthElement(
                new HugeTargetResponse(target));
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
                         JsonTarget jsonTarget) {
        LOGGER.debug("Graph space [{}] update target: {}", graphSpace, jsonTarget);
        checkUpdatingBody(jsonTarget);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeTarget target;
        AuthManager authManager = manager.authManager();
        try {
            target = authManager.getTarget(graphSpace, UserAPI.parseId(id),
                                           false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
        target = jsonTarget.build(target);
        E.checkArgument(!HugeDefaultRole.isDefaultTarget(target.name()),
                        "Update default target is forbidden");
        target = authManager.updateTarget(graphSpace, target, false);

        return manager.serializer().writeAuthElement(
                new HugeTargetResponse(target));
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOGGER.debug("Graph space [{}] list targets", graphSpace);

        AuthManager authManager = manager.authManager();
        List<HugeTarget> targets = authManager.listAllTargets(graphSpace,
                                                              limit, false);
        List<HugeTargetResponse> respList = new ArrayList<>();
        for (HugeTarget target : targets) {
            respList.add(new HugeTargetResponse(target));
        }
        return manager.serializer().writeAuthElements("targets", respList);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOGGER.debug("Graph space [{}] get target: {}", graphSpace, id);

        AuthManager authManager = manager.authManager();
        HugeTarget target = authManager.getTarget(graphSpace,
                                                  UserAPI.parseId(id), false);
        return manager.serializer().writeAuthElement(
                new HugeTargetResponse(target));
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"analyst"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOGGER.debug("Graph space [{}] delete target: {}", graphSpace, id);
        E.checkArgument(!HugeDefaultRole.isDefaultTarget(id),
                        "Delete default target is forbidden");

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteTarget(graphSpace, UserAPI.parseId(id), false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "target_creator",
                                   "target_create", "target_update"})
    private static class JsonTarget implements Checkable {

        @JsonProperty("target_name")
        private String name;
        @JsonProperty("target_graph")
        private String graph;
        @JsonProperty("target_description")
        private String description;
        @JsonProperty("target_resources") // error when List<HugeResource>
        private List<Map<String, Object>> resources;

        public HugeTarget build(HugeTarget target) {
            E.checkArgument(this.name == null ||
                            target.name().equals(this.name),
                            "The name of target can't be updated");
            E.checkArgument(this.graph == null ||
                            target.graph().equals(this.graph),
                            "The graph of target can't be updated");
            if (this.description != null) {
                target.description(this.description);
            }
            if (this.resources != null) {
                target.resources(this.resources);
            }
            return target;
        }

        public HugeTarget build(String graphSpace) {
            HugeTarget target = new HugeTarget(this.name, graphSpace,
                                               this.graph, this.description);
            if (this.resources != null) {
                target.resources(this.resources);
            }
            return target;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            GraphManager.checkAuthName(this.name);
            E.checkArgumentNotNull(this.graph,
                                   "The graph of target can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(this.resources != null ||
                            this.description != null,
                            "Expect one of target description/resources");
        }
    }

    /*
     * only used for deserializable response
     * */
    public static class HugeTargetResponse extends SchemaDefine.Entity {
        private final List<HugeResource> empty = new ArrayList<>();
        private String name;
        private String graphSpace;
        private String graph;
        private String description;
        private List<HugeResource> resources;

        public HugeTargetResponse(HugeTarget target) {
            this.id = target.id();
            this.name = target.name();
            this.graphSpace = target.graphSpace();
            this.graph = target.graph();
            this.description = target.description();
            this.create = target.create();
            this.creator = target.creator();
            this.update = target.update();
            List<HugeResource> ress = new ArrayList<>();
            for (Map.Entry<String, List<HugeResource>> item :
                    target.resources().entrySet()) {
                ress.addAll(item.getValue());
            }
            this.resources = ress;
        }

        // methods below are never used
        @Override
        public ResourceType type() {
            return ResourceType.GRANT;
        }

        @Override
        public String label() {
            return HugeTarget.P.TARGET;
        }

        @Override
        public String name() {
            return this.name;
        }

        public String graphSpace() {
            return this.graphSpace;
        }

        public String graph() {
            return this.graph;
        }

        public String description() {
            return this.description;
        }

        public List<HugeResource> resources() {
            return this.resources;
        }

        @Override
        public String toString() {
            return String.format("HugeTarget(%s)", this.id);
        }

        @Override
        protected boolean property(String key, Object value) {
            if (super.property(key, value)) {
                return true;
            }
            switch (key) {
                case HugeTarget.P.NAME:
                    this.name = (String) value;
                    break;
                case HugeTarget.P.GRAPHSPACE:
                    this.graphSpace = (String) value;
                    break;
                case HugeTarget.P.GRAPH:
                    this.graph = (String) value;
                    break;
                case HugeTarget.P.DESCRIPTION:
                    this.description = (String) value;
                    break;
                case HugeTarget.P.RESS:
                    // this.resources = (Map<String, List<HugeResource>>)) value;
                    this.resources = JsonUtil.fromJson(JsonUtil.toJson(value),
                                                       new TypeReference<List<HugeResource>>() {
                                                       });
                    break;
                default:
                    throw new AssertionError("Unsupported key: " + key);
            }
            return true;
        }

        @Override
        public Map<String, Object> asMap() {
            E.checkState(this.name != null, "Target name can't be null");

            Map<String, Object> map = new HashMap<>();

            map.put(Graph.Hidden.unHide(HugeTarget.P.NAME), this.name);
            map.put(Graph.Hidden.unHide(HugeTarget.P.GRAPHSPACE), this.graphSpace);
            map.put(Graph.Hidden.unHide(HugeTarget.P.GRAPH), this.graph);
            if (this.description != null) {
                map.put(Graph.Hidden.unHide(HugeTarget.P.DESCRIPTION), this.description);
            }

            if (this.resources != null && this.resources != empty) {
                map.put(Graph.Hidden.unHide(HugeTarget.P.RESS), this.resources);
            }

            return super.asMap(map);
        }

        @Override
        protected Object[] asArray() {
            E.checkState(this.name != null, "Target name can't be null");

            List<Object> list = new ArrayList<>(16);

            list.add(T.label);
            list.add(HugeTarget.P.TARGET);

            list.add(HugeTarget.P.NAME);
            list.add(this.name);

            list.add(HugeTarget.P.GRAPHSPACE);
            list.add(this.graphSpace);

            list.add(HugeTarget.P.GRAPH);
            list.add(this.graph);

            list.add(HugeTarget.P.DESCRIPTION);
            list.add(this.description);

            if (this.resources != null && this.resources != empty) {
                list.add(HugeTarget.P.RESS);
                list.add(this.resources);
            }

            return super.asArray(list);
        }
    }
}
