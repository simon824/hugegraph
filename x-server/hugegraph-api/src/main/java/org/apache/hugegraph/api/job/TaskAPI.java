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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.groovy.util.Maps;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;

import jakarta.inject.Singleton;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

@Path("graphspaces/{graphspace}/graphs/{graph}/tasks")
@Singleton
public class TaskAPI extends API {

    public static final String ACTION_CANCEL = "cancel";
    private static final Logger LOGGER = Log.logger(RestServer.class);
    private static final long NO_LIMIT = -1L;

    private static TaskStatus parseStatus(String status) {
        try {
            return TaskStatus.valueOf(status);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Status value must be in %s, but got '%s'",
                    Arrays.asList(TaskStatus.values()), status));
        }
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @PathParam("graphspace") String graphSpace,
                                    @PathParam("graph") String graph,
                                    @QueryParam("status") String status,
                                    @QueryParam("ids") List<Long> ids,
                                    @QueryParam("limit")
                                    @DefaultValue("100") long limit,
                                    @QueryParam("page") String page) {
        LOGGER.debug("Graph [{}] list tasks with status {}, ids {}, limit {}, page {}", graph,
                     status, ids, limit, page);
        TaskScheduler scheduler = graph(manager, graphSpace, graph).taskScheduler();

        Iterator<HugeTask<Object>> iter;

        if (!ids.isEmpty()) {
            E.checkArgument(status == null,
                            "Not support status when query task by ids, " +
                            "but got status='%s'", status);
            E.checkArgument(page == null,
                            "Not support page when query task by ids, " +
                            "but got page='%s'", page);
            // Set limit to NO_LIMIT to ignore limit when query task by ids
            limit = NO_LIMIT;
            List<Id> idList = ids.stream().map(IdGenerator::of)
                                 .collect(Collectors.toList());
            iter = scheduler.tasks(idList);
        } else {
            if (status == null) {
                iter = scheduler.tasks(null, limit, page);
            } else {
                iter = scheduler.tasks(parseStatus(status), limit, page);
            }
        }

        List<Object> tasks = new ArrayList<>();
        while (iter.hasNext()) {
            tasks.add(iter.next().asMap(false));
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

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @PathParam("graphspace") String graphSpace,
                                   @PathParam("graph") String graph,
                                   @PathParam("id") long id) {
        LOGGER.debug("Graph [{}] get task: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        if (null != task) {
            return task.asMap();
        }
        throw new NotFoundException("Task not found");
    }

    /**
     * 以文件下载的方式 下载任务执行结果
     *
     * @param manager
     * @param graphSpace
     * @param graph
     * @param id
     * @return
     */
    @GET
    @Timed
    @Path("{id}/download")
    @CompressInterceptor.Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response download(@Context GraphManager manager,
                             @PathParam("graphspace") String graphSpace,
                             @PathParam("graph") String graph,
                             @PathParam("id") long id) {

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        Object resultObject = null;
        try {
            HugeTask<?> task = scheduler.task(IdGenerator.of(id));
            resultObject = task.result();
        } catch (Exception e) {
            resultObject = e.getMessage();
        }
        String fileName = String.format("task_result_%d.txt", id);
        return Response.ok(resultObject).status(200).header("Content-Disposition",
                                                            String.format("attachment; " +
                                                                          "filename=\"%s\"",
                                                                          fileName)).build();
    }

    @DELETE
    @Timed
    @Path("{id}")
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @PathParam("id") long id,
                       @QueryParam("force") boolean force) {

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        E.checkArgument(task != null, "There is no task with id '%s'", id);

        scheduler.delete(IdGenerator.of(id), force);

        LOGGER.info("[SERVER] DeleteTask with graphSpace {}, id {}", graphSpace, id);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> update(@Context GraphManager manager,
                                      @PathParam("graphspace")
                                      String graphSpace,
                                      @PathParam("graph") String graph,
                                      @PathParam("id") long id,
                                      @QueryParam("action") String action) {

        if (!ACTION_CANCEL.equals(action)) {
            throw new NotSupportedException(String.format(
                    "Not support action '%s'", action));
        }

        TaskScheduler scheduler = graph(manager, graphSpace, graph)
                .taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        if (!task.completed() && !task.cancelling()) {
            scheduler.cancel(task);
            try {
                task = scheduler.waitUntilTaskCompleted(task.id(), 30);
            } catch (TimeoutException t) {
                throw new HugeException(String.format(
                        "cancel task '%s' timeout", id));
            }
            if (task.cancelling() || task.completed() || task.cancelled()) {
                LOGGER.info("[SERVER] CancelTask with graphSpace {}, id {}", graphSpace, task.id());
                return task.asMap();
            }
        }

        assert task.completed() || task.cancelling();
        throw new BadRequestException(String.format(
                "Can't cancel task '%s' which is completed or cancelling",
                id));
    }
}
