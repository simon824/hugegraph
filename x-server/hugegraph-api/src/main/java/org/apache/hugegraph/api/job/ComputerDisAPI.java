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

package org.apache.hugegraph.api.job;

import static org.apache.hugegraph.backend.query.Query.NO_LIMIT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.groovy.util.Maps;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.job.ComputerDisJob;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.k8s.K8sDriverProxy;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

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

@Path("graphspaces/{graphspace}/graphs/{graph}/jobs/computerdis")
@Singleton
public class ComputerDisAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static TaskStatus parseStatus(String status) {
        try {
            return TaskStatus.valueOf(status);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Status value must be in %s, but got '%s'",
                    Arrays.asList(TaskStatus.values()), status));
        }
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> post(@Context GraphManager manager,
                                    @PathParam("graphspace") String graphSpace,
                                    @PathParam("graph") String graph,
                                    JsonTask jsonTask) {
        checkCreatingBody(jsonTask);
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled(),
                        "The k8s api is not enable.");
        // 图空间不为DEFALUT的，对其参数进行校验
        if (!("DEFAULT".equals(graphSpace))) {
            E.checkArgument((Objects.nonNull(jsonTask.params.get("k8s.master_cpu")) &&
                             StringUtils.isNoneBlank(
                                     jsonTask.params.get("k8s.master_cpu").toString())) &&
                            (Objects.nonNull(jsonTask.params.get("k8s.worker_cpu")) &&
                             StringUtils.isNoneBlank(
                                     jsonTask.params.get("k8s.worker_cpu").toString())) &&
                            (Objects.nonNull(jsonTask.params.get("k8s.master_request_memory")) &&
                             StringUtils.isNoneBlank(
                                     jsonTask.params.get("k8s.master_request_memory")
                                                    .toString())) &&
                            (Objects.nonNull(jsonTask.params.get("k8s.worker_request_memory")) &&
                             StringUtils.isNoneBlank(
                                     jsonTask.params.get("k8s.worker_request_memory")
                                                    .toString())) &&
                            (Objects.nonNull(jsonTask.params.get("k8s.master_memory")) &&
                             StringUtils.isNoneBlank(
                                     jsonTask.params.get("k8s.master_memory").toString())) &&
                            (Objects.nonNull(jsonTask.params.get("k8s.worker_memory")) &&
                             StringUtils.isNoneBlank(
                                     jsonTask.params.get("k8s.worker_memory").toString())),
                            "The params k8s.master_cpu k8s.worker_cpu k8s.master_request_memory " +
                            "k8s.worker_request_memory"
                            + " k8s.master_memory k8s.worker_memory must be set.", jsonTask.params);
        }
        LOG.info("Schedule computer dis job: {}, graph is {}", jsonTask, graph);

        // username is "" means generate token from current context
        String token = "";
        if (manager.isAuthRequired()) {
            String username = manager.authManager().username();
            token = manager.authManager().createToken(username);
        }

        GraphSpace space = space(manager, graphSpace);
        String namespace = space.olapNamespace();

        Map<String, Object> input = new HashMap<>();
        input.put("graph", graphSpace + "/" + graph);
        input.put("algorithm", jsonTask.algorithm);
        input.put("params", jsonTask.params);
        input.put("worker", jsonTask.worker);
        input.put("token", token);
        input.put("cluster", manager.cluster());
        input.put("pd.peers", manager.pdPeers());
        input.put("namespace", namespace);
        HugeGraph g = graph(manager, graphSpace, graph);
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name("computer-dis:" + jsonTask.algorithm)
               .input(JsonUtil.toJson(input))
               .context(HugeGraphAuthProxy.getContextString())
               .job(new ComputerDisJob());
        HugeTask<?> task = builder.schedule();
        return ImmutableMap.of("task_id", task.id());
    }

    @DELETE
    @Timed
    @Path("/{id}")
    @Status(Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> delete(@Context GraphManager manager,
                                      @PathParam("graphspace") String graphSpace,
                                      @PathParam("graph") String graph,
                                      @PathParam("id") long id) {
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled(),
                        "The k8s api is not enable.");
        LOG.info("Graph [{}] delete computer job: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        E.checkArgument(ComputerDisJob.COMPUTER_DIS.equals(task.type()),
                        "The task is not computer-dis task.");

        scheduler.delete(IdGenerator.of(id));
        return ImmutableMap.of("task_id", id, "message", "success");
    }

    @PUT
    @Timed
    @Path("/{id}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> cancel(@Context GraphManager manager,
                                      @PathParam("graphspace") String graphSpace,
                                      @PathParam("graph") String graph,
                                      @PathParam("id") long id) {
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled(),
                        "The k8s api is not enable.");
        LOG.info("Graph [{}] cancel computer job: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        E.checkArgument(ComputerDisJob.COMPUTER_DIS.equals(task.type()),
                        "The task is not computer-dis task.");

        if (!task.completed() && !task.cancelling()) {
            scheduler.cancel(task);
            if (task.cancelling()) {
                return task.asMap();
            }
        }

        assert task.completed() || task.cancelling();
        return ImmutableMap.of("task_id", id);
    }

    @GET
    @Timed
    @Path("/{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @PathParam("graphspace") String graphSpace,
                                   @PathParam("graph") String graph,
                                   @PathParam("id") long id) {
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled(),
                        "The k8s api is not enable.");
        LOG.debug("Graph [{}] get task info", graph);
        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<Object> task = scheduler.task(IdGenerator.of(id));
        E.checkArgument(ComputerDisJob.COMPUTER_DIS.equals(task.type()),
                        "The task is not computer-dis task.");
        return task.asMap();
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @PathParam("graphspace") String graphSpace,
                                    @PathParam("graph") String graph,
                                    @QueryParam("limit")
                                    @DefaultValue("100") long limit,
                                    @QueryParam("page") String page) {
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled(),
                        "The k8s api is not enable.");
        LOG.debug("Graph [{}] get task list", graph);

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        Iterator<HugeTask<Object>> iter = scheduler.tasks(null,
                                                          NO_LIMIT, page);
        List<Object> tasks = new ArrayList<>();
        while (iter.hasNext()) {
            HugeTask<Object> task = iter.next();
            if (ComputerDisJob.COMPUTER_DIS.equals(task.type())) {
                tasks.add(task.asMap(false));
            }
        }
        if (limit != NO_LIMIT && tasks.size() > limit) {
            tasks = tasks.subList(0, (int) limit);
        }

        if (page == null) {
            return Maps.of("tasks", tasks);
        } else {
            return Maps.of("tasks", tasks, "page", PageInfo.pageInfo(iter));
        }
    }

    private static class JsonTask implements Checkable {

        @JsonProperty("algorithm")
        public String algorithm;
        @JsonProperty("worker")
        public int worker;
        @JsonProperty("params")
        public Map<String, Object> params;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.algorithm != null &&
                            K8sDriverProxy.isValidAlgorithm(this.algorithm),
                            "The algorithm is not existed.");
            E.checkArgument(this.worker >= 1 &&
                            this.worker <= 100,
                            "The worker should be in [1, 100].");
        }

        @Override
        public void checkUpdate() {
        }
    }
}
