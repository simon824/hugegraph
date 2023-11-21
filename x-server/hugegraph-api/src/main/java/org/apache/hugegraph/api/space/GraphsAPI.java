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

package org.apache.hugegraph.api.space;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator.RequiredPerm;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.SchemaTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.job.SubgraphJob;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.task.StandardTaskScheduler;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.SecurityContext;

@Path("graphspaces/{graphspace}/graphs")
@Singleton
public class GraphsAPI extends API {

    private static final Logger LOGGER = Log.logger(RestServer.class);
    private static final String GRAPH_ACTION = "action";
    private static final String UPDATE = "update";
    private static final String CLEAR_SCHEMA = "clear_schema";
    private static final String GRAPH_ACTION_CLEAR = "clear";
    private static final String GRAPH_ACTION_RELOAD = "reload";
    private static final String GRAPH_DESCRIPTION = "description";

    private static HugeGraph initGraph(GraphManager manager, String graphSpace,
                                       HugeGraph origin,
                                       Map<String, Object> configs,
                                       boolean create,
                                       boolean initSchema) {
        E.checkArgument(configs.get("name") != null &&
                        StringUtils.isNotEmpty(configs.get("name").toString()),
                        "Missing subgraph name");
        String subName = configs.get("name").toString();
        configs.put("store", subName);
        HugeGraph graph;
        if (create) {
            LOGGER.debug("Create graph {} with config options '{}' in graph space '{}'", subName,
                         configs, graphSpace);
            String creator = manager.authManager().username();
            graph = manager.createGraph(graphSpace, subName,
                                        creator, convConfig(configs), true);
        } else {
            graph = graph(manager, graphSpace, subName);
        }

        if (initSchema) {
            copySchema(origin, graph);
        }

        return graph;
    }

    private static Map<String, Object> convConfig(Map<String, Object> config) {
        Map<String, Object> result = new HashMap<>(config.size());
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toString());
        }
        return result;
    }

    private static void copySchema(HugeGraph origin, HugeGraph graph) {
        SchemaManager schema = origin.schema();
        SchemaTransaction schemaTransaction = graph.schemaTransaction();
        for (PropertyKey pk : schema.getPropertyKeys()) {
            schemaTransaction.addPropertyKey(pk);
        }
        for (VertexLabel vl : schema.getVertexLabels()) {
            schemaTransaction.addVertexLabel(vl);
        }
        for (EdgeLabel el : schema.getEdgeLabels()) {
            schemaTransaction.addEdgeLabel(el);
        }
        for (IndexLabel il : schema.getIndexLabels()) {
            SchemaLabel sl = IndexLabel.getElement(il.graph(),
                                                   il.baseType(),
                                                   il.baseValue());
            schemaTransaction.addIndexLabel(sl, il);
        }
    }

    private static String getSubgraphType(String type, String mode) {
        return String.join("-", type, mode);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @Context SecurityContext sc) {
        LOGGER.debug("List graphs in graph space {}", graphSpace);
        if (null == manager.graphSpace(graphSpace)) {
            throw new HugeException("Graphspace not exist!");
        }
        Set<String> graphs = manager.graphs(graphSpace);
        LOGGER.debug("Get graphs list from graph manager with size {}", graphs.size());
        // Filter by user role
        Set<String> filterGraphs = new HashSet<>();
        for (String graph : graphs) {
            LOGGER.debug("Get graph {} and verify auth", graph);
            String role = RequiredPerm.roleFor(graphSpace, graph,
                                               HugePermission.READ);
            if (sc.isUserInRole(role)) {
                try {
                    graph(manager, graphSpace, graph);
                    filterGraphs.add(graph);
                } catch (ForbiddenException ignored) {
                    // ignore
                }
            } else {
                LOGGER.debug("The user not in role for graph {}", graph);
            }
        }
        LOGGER.debug("Finish list graphs with size {}", filterGraphs.size());
        return ImmutableMap.of("graphs", filterGraphs);
    }

    @GET
    @Timed
    @Path("profile")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public Object listProfile(@Context GraphManager manager,
                              @PathParam("graphspace") String graphSpace,
                              @QueryParam("prefix") String prefix,
                              @Context SecurityContext sc) {
        LOGGER.debug("List graphs profile in graph space {}", graphSpace);
        if (null == manager.graphSpace(graphSpace)) {
            throw new HugeException("Graphspace not exist!");
        }
        Set<String> graphs = manager.graphs(graphSpace);
        LOGGER.debug("Get graphs list from graph manager with size {}", graphs.size());

        AuthManager authManager = manager.authManager();
        GraphSpace gs = space(manager, graphSpace);
        String gsNickname = gs.nickname();
        String user = authManager.username();
        String defaultGraph = authManager.getDefaultGraph(graphSpace, user);
        // Filter by user role
        List<Map<String, Object>> result = new ArrayList<>();
        List<Map<String, Object>> filterGraphs = new ArrayList<>();
        for (String graph : graphs) {
            LOGGER.debug("Get graph {} and verify auth", graph);
            String role = RequiredPerm.roleFor(graphSpace, graph,
                                               HugePermission.READ);
            if (sc.isUserInRole(role)) {
                try {
                    HugeGraph hg = graph(manager, graphSpace, graph);
                    HugeConfig config = (HugeConfig) hg.configuration();
                    String configResp = ConfigUtil.writeConfigToString(config);
                    Map<String, Object> profile =
                            JsonUtil.fromJson(configResp, Map.class);
                    profile.put("name", graph);
                    profile.put("nickname", hg.nickname());
                    if (!isPrefix(profile, prefix)) {
                        continue;
                    }

                    boolean graphDefaulted = StringUtils.equals(graph,
                                                                defaultGraph);
                    profile.put("default", graphDefaulted);
                    profile.put("graphspace_nickname", gsNickname);
                    // set default graph first
                    if (graphDefaulted) {
                        result.add(profile);
                    } else {
                        filterGraphs.add(profile);
                    }
                } catch (ForbiddenException ignored) {
                    // ignore
                }
            } else {
                LOGGER.debug("The user not in role for graph {}", graph);
            }
        }
        LOGGER.debug("Finish list graphs profile with size {}", filterGraphs.size());
        result.addAll(filterGraphs);
        return result;
    }

    public boolean isPrefix(Map<String, Object> profile, String prefix) {
        if (StringUtils.isEmpty(prefix)) {
            return true;
        }
        // graph name or nickname is not empty
        String name = profile.get("name").toString();
        String nickname = profile.get("nickname").toString();
        return name.startsWith(prefix) || nickname.startsWith(prefix);
    }

    @GET
    @Timed
    @Path("{graph}/default")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object setDefault(@Context GraphManager manager,
                             @PathParam("graphspace") String graphSpace,
                             @PathParam("graph") String graph) {
        LOGGER.debug("Set default graph by graph space {} and name '{}'", graphSpace, graph);
        AuthManager authManager = manager.authManager();
        String user = authManager.username();
        String originGraph = authManager.getDefaultGraph(graphSpace, user);
        if (StringUtils.isNotEmpty(originGraph)) {
            authManager.unsetDefaultGraph(graphSpace, originGraph, user);
        }
        authManager.setDefaultGraph(graphSpace, graph, user);
        return ImmutableMap.of("name", user, "default_graph", graph);
    }

    @GET
    @Timed
    @Path("/default")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String getDefault(@Context GraphManager manager,
                             @PathParam("graphspace") String graphSpace) {
        LOGGER.debug("Get default graph if exists, return null otherwise");
        AuthManager authManager = manager.authManager();
        String user = authManager.username();
        String result = authManager.getDefaultGraph(graphSpace, user);
        if (StringUtils.isNotEmpty(result)) {
            HugeGraph g = manager.graph(graphSpace, result);
            if (g == null) {
                authManager.unsetDefaultGraph(graphSpace, result, user);
                result = "";
            }
        }

        result = (result == null) ? "" : result;
        return manager.serializer().writeMap(
                ImmutableMap.of("name", user, "default_graph", result));
    }

    @GET
    @Timed
    @Path("{graph}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst", "$graphspace=$graphspace $owner=$graph"})
    public Object get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph) {
        LOGGER.debug("Get graph by graph space {} and name '{}'", graphSpace, graph);
        if (null == manager.graphSpace(graphSpace)) {
            throw new HugeException("Graphspace not exist!");
        }
        HugeGraph g = graph(manager, graphSpace, graph);
        Map<String, Object> configs = manager.graphConfig(graphSpace, graph);
        String description = (String) configs.get(GRAPH_DESCRIPTION);
        if (description == null) {
            description = Strings.EMPTY;
        }
        return ImmutableMap.of("name", g.name(),
                               "nickname", g.nickname(),
                               "backend", g.backend(),
                               "description", description);
    }

    @POST
    @Timed
    @Path("{name}")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public Object create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("name") String name,
                         Map<String, Object> configs) {
        LOGGER.debug("Create graph {} with config options '{}' in graph space '{}'", name, configs,
                     graphSpace);
        GraphSpace gs = manager.graphSpace(graphSpace);
        E.checkArgumentNotNull(gs, "Not existed graph space: '%s'", graphSpace);

        String creator = manager.authManager().username();
        HugeGraph graph = manager.createGraph(graphSpace, name, creator,
                                              convConfig(configs), true);
        if (gs.auth()) {
            manager.authManager().createGraphDefaultRole(graphSpace,
                                                         graph.nickname());
        }
        if (graph.taskScheduler() instanceof StandardTaskScheduler) {
            graph.tx().close();
        }
        String description = (String) configs.get(GRAPH_DESCRIPTION);
        if (description == null) {
            description = Strings.EMPTY;
        }
        Object result = ImmutableMap.of("name", graph.name(),
                                        "nickname", graph.nickname(),
                                        "backend", graph.backend(),
                                        "description", description);
        // TODO: duplicated log
        //LOGGER.getServerLogger()
        //      .logCreateGraph(name, graph.configuration().toString());
        //LOGGER.getAuditLogger()
        //      .logCreateGraph(graphSpace, name, creator);
        return result;
    }

    @GET
    @Timed
    @Path("{graph}/conf")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String getConf(@Context GraphManager manager,
                          @PathParam("graphspace") String graphSpace,
                          @PathParam("graph") String graph) {

        LOGGER.debug("Get graph configuration by name '{}'", graph);

        // HugeGraph g = graph4admin(manager, graphSpace, graph);
        HugeGraph g = graph(manager, graphSpace, graph);

        HugeConfig config = (HugeConfig) g.configuration();
        String configResp = ConfigUtil.writeConfigToString(config);
        Map<String, Object> profile = JsonUtil.fromJson(configResp, Map.class);
        profile.put("name", graph);
        profile.put("nickname", g.nickname());
        return JsonUtil.toJson(profile);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public Map<String, String> manage(
            @Context GraphManager manager,
            @PathParam("graphspace") String graphSpace,
            @PathParam("name") String name,
            Map<String, Object> actionMap) {
        LOGGER.debug("Manage graph with action '{}'", actionMap);
        E.checkArgument(actionMap != null && actionMap.size() == 2 &&
                        actionMap.containsKey(GRAPH_ACTION),
                        "Invalid request body '%s'", actionMap);
        Object value = actionMap.get(GRAPH_ACTION);
        E.checkArgument(value instanceof String,
                        "Invalid action type '%s', must be string",
                        value.getClass());
        String action = (String) value;
        switch (action) {
            case "update":
                // TODO if we need to update infos other than nickname
                LOGGER.debug("Update graph: '{}'", name);
                E.checkArgument(actionMap.containsKey(UPDATE),
                                "Please pass '%s' for graph update",
                                UPDATE);
                value = actionMap.get(UPDATE);
                E.checkArgument(value instanceof Map,
                                "The '%s' must be map, but got %s",
                                UPDATE, value.getClass());
                @SuppressWarnings("unchecked")
                Map<String, Object> graphMap = (Map<String, Object>) value;
                String graphName = (String) graphMap.get("name");
                E.checkArgument(graphName.equals(name),
                                "Different name in update body with in path");
                HugeGraph exist = graph(manager, graphSpace, name);
                if (exist == null) {
                    throw new NotFoundException(
                            "Can't find graph with name '%s'", graphName);
                }

                String nickname = (String) graphMap.get("nickname");
                if (!Strings.isEmpty(nickname)) {
                    GraphManager.checkNickname(nickname);

                    boolean sameNickname = nickname.equals(exist.nickname());
                    E.checkArgument(sameNickname ||
                                    !manager.isExistedGraphNickname(graphSpace,
                                                                    nickname),
                                    "Nickname '%s' for %s has existed",
                                    nickname, graphSpace);
                    manager.updateGraphNickname(graphSpace, name, nickname);
                    exist.nickname(nickname);
                }
                exist.refreshUpdateTime();
                return ImmutableMap.of(name, "updated");
            case GRAPH_ACTION_CLEAR:
                String username = manager.authManager().username();
                HugeGraph g = graph(manager, graphSpace, name);
                if ((Boolean) actionMap.getOrDefault(CLEAR_SCHEMA, false)) {
                    g.truncateBackend();
                } else {
                    g.truncateGraph();
                }
                // truncateBackend() will open tx, so must close here(commit)
                g.tx().commit();
                manager.meta().notifyGraphClear(graphSpace, name);
                LOGGER.info("[AUDIT] Clear graph with graph space {}, name {}, username {}",
                            graphSpace,
                            name, username);
                return ImmutableMap.of(name, "cleared");
            case GRAPH_ACTION_RELOAD:
                manager.reload(graphSpace, name);
                return ImmutableMap.of(name, "reloaded");
            default:
                throw new AssertionError(String.format(
                        "Invalid graph action: '%s'", action));
        }
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public void delete(@Context GraphManager manager,
                       @PathParam("name") String name,
                       @PathParam("graphspace") String graphSpace) {
        HugeGraphAuthProxy.setAdmin();
        GraphSpace gs = manager.graphSpace(graphSpace);
        HugeGraph graph = manager.graph(graphSpace, name);
        manager.dropGraph(graphSpace, name, true);
        if (gs.auth()) {
            manager.authManager().deleteGraphDefaultRole(graphSpace,
                                                         graph.nickname());
        }
        LOGGER.info("[AUDIT] Remove graph with graph space {}, name {}, username {}", graphSpace,
                    name,
                    manager.authManager().username());

    }

    @PUT
    @Timed
    @Path("manage")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public Object reload(@Context GraphManager manager,
                         Map<String, String> actionMap) {

        LOGGER.info("[SERVER] Manage graph with action map {}", actionMap);
        E.checkArgument(actionMap != null &&
                        actionMap.containsKey(GRAPH_ACTION),
                        "Please pass '%s' for graphs manage", GRAPH_ACTION);
        String action = actionMap.get(GRAPH_ACTION);
        if (action.equals(GRAPH_ACTION_RELOAD)) {
            manager.reload();
            return ImmutableMap.of("graphs", "reloaded");
        }
        throw new AssertionError(String.format(
                "Invalid graphs action: '%s'", action));
    }

    @PUT
    @Timed
    @Path("{graph}/compact")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String compact(@Context GraphManager manager,
                          @PathParam("graphspace") String graphSpace,
                          @PathParam("graph") String graph) {

        HugeGraph g = graph(manager, graphSpace, graph);
        return JsonUtil.toJson(g.metadata(null, "compact"));
    }

    @PUT
    @Timed
    @Path("{graph}/flush")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public String flush(@Context GraphManager manager,
                        @PathParam("graphspace") String graphSpace,
                        @PathParam("graph") String graph) {

        HugeGraph g = graph(manager, graphSpace, graph);
        if (g.backend().equals("rocksdb")) {
            g.metadata(null, "flush");
        }
        String jsonResult = JsonUtil.toJson(ImmutableMap.of(graph, "flushed"));
        LOGGER.info("[SERVER] Flush graph with graph {}", graph);
        return jsonResult;
    }

    @PUT
    @Timed
    @Path("{graph}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst", "$graphspace=$graphspace $owner=$graph"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @PathParam("graphspace") String graphSpace,
                                       @PathParam("graph") String graph,
                                       GraphMode mode) {
        LOGGER.debug("Set mode to: '{}' of graph '{}'", mode, graph);

        E.checkArgument(mode != null, "Graph mode can't be null");
        HugeGraph g = graph(manager, graphSpace, graph);
        g.mode(mode);
        // mode(m) might trigger tx open, must close(commit)
        g.tx().commit();
        HugeConfig config = (HugeConfig) g.configuration();
        if (config.get(CoreOptions.BACKEND).equals("hstore")) {
            g.metadata(null, "mode", mode);
        }
        return ImmutableMap.of("mode", mode);
    }

    @GET
    @Timed
    @Path("{graph}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst", "$graphspace=$graphspace $owner=$graph"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @PathParam("graphspace") String graphSpace,
                                       @PathParam("graph") String graph) {
        LOGGER.debug("Get mode of graph '{}'", graph);

        HugeGraph g = graph(manager, graphSpace, graph);
        return ImmutableMap.of("mode", g.mode());
    }

    @PUT
    @Timed
    @Path("{graph}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("analyst")
    public Map<String, GraphReadMode> graphReadMode(
            @Context GraphManager manager,
            @PathParam("graphspace") String graphSpace,
            @PathParam("graph") String graph,
            GraphReadMode readMode) {
        LOGGER.debug("Set graph-read-mode to: '{}' of graph '{}'", readMode, graph);

        E.checkArgument(readMode != null,
                        "Graph-read-mode can't be null");
        E.checkArgument(readMode == GraphReadMode.ALL ||
                        readMode == GraphReadMode.OLTP_ONLY,
                        "Graph-read-mode could be ALL or OLTP_ONLY");
        HugeGraph g = graph(manager, graphSpace, graph);
        manager.graphReadMode(graphSpace, graph, readMode);
        g.readMode(readMode);
        return ImmutableMap.of("graph_read_mode", readMode);
    }

    @GET
    @Timed
    @Path("{graph}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst", "$graphspace=$graphspace $owner=$graph"})
    public Map<String, GraphReadMode> graphReadMode(
            @Context GraphManager manager,
            @PathParam("graphspace") String graphSpace,
            @PathParam("graph") String graph) {
        LOGGER.debug("Get graph-read-mode of graph '{}'", graph);

        HugeGraph g = graph(manager, graphSpace, graph);
        return ImmutableMap.of("graph_read_mode", g.readMode());
    }

    @POST
    @Timed
    @Path("/{graph}/clone")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public Object clone(@Context HugeConfig config,
                        @Context GraphManager manager,
                        @PathParam("graphspace") String graphSpace,
                        @PathParam("graph") String name,
                        CloneJson cloneJson) {
        HugeGraphAuthProxy.setAdmin();
        AuthManager authManager = manager.authManager();
        HugeGraph origin = graph(manager, graphSpace, name);
        GraphSpace gs = manager.graphSpace(graphSpace);
        String cloneSpace = (StringUtils.isNotEmpty(cloneJson.graphSpace)) ?
                            cloneJson.graphSpace : graphSpace;
        boolean auth = manager.graphSpace(cloneSpace).auth();
        E.checkArgument(!auth ||
                        authManager.isSpaceManager(cloneSpace,
                                                   authManager.username()),
                        "User not authorized to clone to space %s, " +
                        cloneSpace);

        HugeGraph graph = initGraph(manager, cloneSpace, origin,
                                    cloneJson.configs, cloneJson.create,
                                    cloneJson.initSchema);
        String id = "";
        if (cloneJson.loadData) {
            Map<String, Object> taskInput = new HashMap<>();
            taskInput.put("batch_size", cloneJson.batchSize);
            taskInput.put("range", cloneJson.range);
            taskInput.put("loop_limit", cloneJson.loopLimit);
            taskInput.put("keep_start_p", false);
            taskInput.put("auth", auth);
            taskInput.put("sub_auth", gs.auth());

            //taskInput.put("type", getSubgraphType("vertex", "shards"));
            taskInput.put("type", "vertex");
            taskInput.put("start", "0");
            taskInput.put("end", "65556");
            JobBuilder<Object> builder = JobBuilder.of(graph);
            builder.name(SubgraphJob.SUB_INIT)
                   .input(JsonUtil.toJson(taskInput))
                   .context(HugeGraphAuthProxy.getContextString())
                   .job(new SubgraphJob(config, authManager, origin));
            id = builder.schedule().id().asString();
        }

        return ImmutableMap.of("task_id", id);
    }

    @POST
    @Timed
    @Path("/{graph}/subgraph")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public Object subgraph(@Context HugeConfig config,
                           @Context GraphManager manager,
                           @PathParam("graphspace") String graphSpace,
                           @PathParam("graph") String name,
                           SubgraphJson subgraphJson) {
        HugeGraphAuthProxy.setAdmin();
        Map<String, Object> taskInput = new HashMap<>();
        E.checkArgument(subgraphJson.type != null, "Missing field: type");
        String type = subgraphJson.type;
        E.checkArgument("vertex".equals(type) || "edge".equals(type),
                        "Invalid value: type");

        taskInput.put("type", subgraphJson.type);
        taskInput.put("batch_size", subgraphJson.batchSize);
        taskInput.put("range", subgraphJson.range);
        taskInput.put("loop_limit", subgraphJson.loopLimit);
        taskInput.put("keep_start_p", subgraphJson.keepStartP);

        AuthManager authManager = manager.authManager();
        HugeGraph origin = graph(manager, graphSpace, name);
        GraphSpace gs = manager.graphSpace(graphSpace);
        HugeGraph graph = initGraph(manager, graphSpace, origin,
                                    subgraphJson.configs,
                                    subgraphJson.create,
                                    subgraphJson.initSchema);
        taskInput.put("auth", gs.auth());

        List<Id> taskIds = new ArrayList<>();

        boolean deriveByIds = !(subgraphJson.ids == null ||
                                subgraphJson.ids.isEmpty());
        boolean deriveBySteps = !(subgraphJson.steps == null ||
                                  subgraphJson.steps.isEmpty());
        boolean deriveByShard = !(subgraphJson.shard == null ||
                                  subgraphJson.shard.isEmpty());

        // init an asyn task for each step
        if (deriveByIds) {
            taskInput.put("ids", subgraphJson.ids);
            taskInput.put("type", subgraphJson.type);
            JobBuilder<Object> builder = JobBuilder.of(graph);
            builder.name(SubgraphJob.SUB_INIT)
                   .input(JsonUtil.toJson(taskInput))
                   .context(HugeGraphAuthProxy.getContextString())
                   .job(new SubgraphJob(config, authManager, origin));
            taskIds.add(builder.schedule().id());
        } else if (deriveBySteps) {
            List<Map<String, Object>> conditions =
                    new ArrayList<>(subgraphJson.steps.size());
            for (StepEntity step : subgraphJson.steps) {
                Map<String, Object> condition = new HashMap<>();
                condition.put("label", step.label);
                Map<String, Object> props = step.properties;
                condition.put("properties", props);
                conditions.add(condition);
            }
            taskInput.put("steps", conditions);
            JobBuilder<Object> builder = JobBuilder.of(graph);
            taskInput.put("type", subgraphJson.type);
            builder.name(SubgraphJob.SUB_INIT)
                   .input(JsonUtil.toJson(taskInput))
                   .context(HugeGraphAuthProxy.getContextString())
                   .job(new SubgraphJob(config, authManager, origin));
            taskIds.add(builder.schedule().id());
        } else if (deriveByShard) {
            taskInput.put("type", subgraphJson.type);
            taskInput.put("start", subgraphJson.shard.get("start"));
            taskInput.put("end", subgraphJson.shard.get("end"));
            JobBuilder<Object> builder = JobBuilder.of(graph);
            builder.name(SubgraphJob.SUB_INIT)
                   .input(JsonUtil.toJson(taskInput))
                   .context(HugeGraphAuthProxy.getContextString())
                   .job(new SubgraphJob(config, authManager, origin));
            taskIds.add(builder.schedule().id());
        }

        return ImmutableMap.of("task_ids", taskIds);
    }

    protected static class StepEntity {

        @JsonProperty("label")
        public String label;
        @JsonProperty("properties")
        public Map<String, Object> properties = ImmutableMap.of();

        @Override
        public String toString() {
            return String.format("StepEntity{label=%s,properties=%s}",
                                 this.label, this.properties);
        }
    }

    private static class SubgraphJson {

        @JsonProperty("configs")
        public Map<String, Object> configs;
        @JsonProperty("type")
        public String type;
        @JsonProperty("ids")
        public List<String> ids;
        @JsonProperty("steps")
        public List<StepEntity> steps;
        @JsonProperty("shard")
        public Map<String, Object> shard;
        @JsonProperty("batch_size")
        public Integer batchSize = 8000;
        @JsonProperty("range")
        public Integer range = 200000;
        @JsonProperty("loop_limit")
        public Integer loopLimit = -1;
        @JsonProperty("create")
        public boolean create = true;
        @JsonProperty("init_schema")
        public boolean initSchema = true;
        @JsonProperty("keep_start_p")
        public boolean keepStartP = false;

        public static SubgraphJson fromJson(Object json) {
            SubgraphJson subgraphJson;
            if (json instanceof String) {
                subgraphJson = JsonUtil.fromJson((String) json,
                                                 SubgraphJson.class);
            } else {
                // Optimized json with SubgraphJson object
                E.checkArgument(json instanceof SubgraphJson,
                                "Invalid role value: %s", json);
                subgraphJson = (SubgraphJson) json;
            }
            return subgraphJson;
        }

        @Override
        public String toString() {
            return String.format("Subgraph{name=%s,configs=%s,type=%s,ids=%s," +
                                 "steps=%s}", this.configs.get("name"),
                                 this.configs, this.type, this.ids, this.steps);
        }

        public String toJson() {
            return JsonUtil.toJson(this);
        }
    }

    private static class CloneJson {
        @JsonProperty("graphspace")
        public String graphSpace;
        @JsonProperty("configs")
        public Map<String, Object> configs;
        @JsonProperty("batch_size")
        public Integer batchSize = 8000;
        @JsonProperty("range")
        public Integer range = 200000;
        @JsonProperty("loop_limit")
        public Integer loopLimit = -1;
        @JsonProperty("create")
        public boolean create = true;
        @JsonProperty("init_schema")
        public boolean initSchema = true;
        @JsonProperty("load_data")
        public boolean loadData = false;

        public static CloneJson fromJson(Object json) {
            CloneJson cloneJson;
            if (json instanceof String) {
                cloneJson = JsonUtil.fromJson((String) json,
                                              CloneJson.class);
            } else {
                // Optimized json with SubgraphJson object
                E.checkArgument(json instanceof CloneJson,
                                "Invalid value: %s", json);
                cloneJson = (CloneJson) json;
            }
            return cloneJson;
        }

        @Override
        public String toString() {
            return String.format("GraphClone{name=%s,configs=%s,load_data=%s}",
                                 this.configs.get("name"), this.configs,
                                 this.loadData);
        }

        public String toJson() {
            return JsonUtil.toJson(this);
        }
    }
}
