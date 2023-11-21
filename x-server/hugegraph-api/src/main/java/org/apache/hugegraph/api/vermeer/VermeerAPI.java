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

package org.apache.hugegraph.api.vermeer;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.job.VermeerJob;
import org.apache.hugegraph.rest.RestResult;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.util.JsonUtil;
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
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("vermeer")
@Singleton
public class VermeerAPI extends API {
    public static final String INNER_JOB_ID = "inner.job.id";
    private static final Logger LOG = Log.logger(VermeerAPI.class);
    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    private VermeerDriver vermeerDriver;

    public VermeerDriver vermeerDriver() {
        if (this.vermeerDriver != null) {
            return this.vermeerDriver;
        }
        String url =
                configProvider.get().get(ServerOptions.VERMEER_SERVER_URL);
        int timeout =
                configProvider.get().get(ServerOptions.VERMEER_SERVER_REQUEST_TIMEOUT);
        this.vermeerDriver = new VermeerDriver(url, timeout);
        return this.vermeerDriver;
    }

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> post(@Context GraphManager manager,
                                    JsonTask jsonTask) throws Exception {
        // vermeerGraph = {graphSpace}-{graph}
        String vermeerGraph = jsonTask.graph;
        LOG.info("Schedule vermeer job: {}, graph is {}", jsonTask,
                 vermeerGraph);
        String[] split = vermeerGraph.split("-");
        String graphSpace = split[0];
        String graph = split[1];

        String url =
                configProvider.get().get(ServerOptions.VERMEER_SERVER_URL);
        int timeout =
                configProvider.get().get(ServerOptions.VERMEER_SERVER_REQUEST_TIMEOUT);

        Map<String, Object> input = new HashMap<>();
        input.put("task_type", jsonTask.taskType);
        input.put("graph", jsonTask.graph);
        input.put("params", jsonTask.params);
        input.put("url", url);
        input.put("timeout", timeout);

        HugeGraph g = graph(manager, graphSpace, graph);
        String taskName = "vermeer-task:";
        switch (jsonTask.taskType) {
            case "compute":
                taskName += jsonTask.params.get("compute.algorithm").toString();
                break;
            case "load":
                taskName += jsonTask.taskType;
                break;
            default:
                throw new AssertionError(String.format("Invalid task_type: '%s'",
                                                       jsonTask.taskType));
        }

        Map<String, Object> vermeerParams = new HashMap<>();
        vermeerParams.put("task_type", jsonTask.taskType);
        vermeerParams.put("graph", jsonTask.graph);
        vermeerParams.put("params", jsonTask.params);
        String jobId = submitVermeerTaskAsyn(vermeerParams);
        input.put(INNER_JOB_ID, jobId);

        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name(taskName)
               .input(JsonUtil.toJson(input))
               .context(HugeGraphAuthProxy.getContextString())
               .job(new VermeerJob());
        HugeTask<?> task = builder.schedule();
        return ImmutableMap.of("task_id", task.id());
    }

    @GET
    @Timed
    @Path("/vermeergraphs")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map getGraphsInfo() {
        String path = "graphs";
        return this.vermeerDriver().get(path).readObject(Map.class);
    }

    @GET
    @Timed
    @Path("/vermeergraphs/{graphname}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map getGraphInfoByName(@PathParam("graphname") String graphName) {
        String path = String.join("/", "graphs", graphName);
        return this.vermeerDriver().get(path).readObject(Map.class);
    }

    @GET
    @Timed
    @Path("/vermeertasks")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map getTasksInfo() {
        String path = "tasks";
        return this.vermeerDriver().get(path).readObject(Map.class);
    }

    @GET
    @Timed
    @Path("/vermeertasks/{task_id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map getTaskInfoById(@PathParam("task_id") String taskId) {
        String path = String.join("/", "task", taskId);
        return this.vermeerDriver().get(path).readObject(Map.class);
    }

    @DELETE
    @Timed
    @Path("/vermeergraphs/{graphname}")
    @StatusFilter.Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map delete(@PathParam("graphname") String graphName) {
        String path = String.join("/", "graphs", graphName);
        return this.vermeerDriver().delete(path, new HashMap<>())
                   .readObject(Map.class);
    }

    private String submitVermeerTaskAsyn(Map<String, Object> vermeerParams) throws
                                                                            Exception {
        RestResult res =
                this.vermeerDriver().post("tasks/create", vermeerParams);
        Map innerMap;
        if (res.status() == 200) {
            innerMap = (Map) (res.readObject(Map.class).get("task"));
        } else {
            throw new Exception("The vermeerDriver is abnormal. " +
                                "Please check whether the vermeer URL is " +
                                "configured correctly or whether the vermeer " +
                                "service is alive");
        }

        return String.valueOf(innerMap.get("id"));
    }

    private static class JsonTask {
        @JsonProperty("task_type")
        public String taskType;
        @JsonProperty("graph")
        public String graph;
        @JsonProperty("params")
        public Map<String, Object> params;
    }
}
