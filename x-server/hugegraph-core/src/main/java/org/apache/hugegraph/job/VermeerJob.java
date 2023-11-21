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

package org.apache.hugegraph.job;

import static org.apache.hugegraph.util.JsonUtil.fromJson;
import static org.apache.hugegraph.util.JsonUtil.toJson;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.rest.RestResult;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.vermeer.VermeerDriver;
import org.slf4j.Logger;

/**
 * This class is used for call vermeer-api to run OLAP algorithms
 */

public class VermeerJob extends UserJob<Object> {
    public static final String VERMEER_TASK = "vermeer-task";
    public static final String INNER_STATUS = "inner.status";
    public static final String INNER_JOB_ID = "inner.job.id";
    private static final Logger LOG = Log.logger(VermeerJob.class);
    private VermeerDriver vermeerDriver;
    private String url;
    private int timeout;
    private String innerJobId;

    public VermeerDriver vermeerDriver() {
        if (this.vermeerDriver != null) {
            return this.vermeerDriver;
        }
        this.vermeerDriver = new VermeerDriver(this.url, this.timeout);
        return this.vermeerDriver;
    }


    @Override
    public String type() {
        return VERMEER_TASK;
    }

    @Override
    protected void cancelled() {
        super.cancelled();
        String path = String.join("/", "task", "cancel", innerJobId);
        RestResult result = this.vermeerDriver().get(path);

        if (result.status() == 200) {
            LOG.info(String.format("Success cancel vermeer task, vermeer-task-id = %s ",
                                   innerJobId));
        } else if (result.status() == 400) {
            Map map = result.readObject(Map.class);
            LOG.warn(String.format("Cancel vermeer task failed, please check " +
                                   "manually: " + "caused by:%s", map.get("message")));
        } else {
            LOG.warn("Cancel computer task failed, unable to connect to vermeer ");
        }
        // 释放资源
        this.vermeerDriver.close();
        this.vermeerDriver = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");

        Map<String, Object> map = fromJson(input, Map.class);
        this.url = (String) map.get("url");
        this.timeout = (int) map.get("timeout");

        Map<String, Object> vermeerParams = new HashMap<>();
        // for task retry
        vermeerParams.put("task_type", map.get("task_type"));
        vermeerParams.put("graph", map.get("graph"));
        vermeerParams.put("params", map.get("params"));

        String status = map.containsKey(INNER_STATUS) ?
                        map.get(INNER_STATUS).toString() : null;
        String jobId = map.containsKey(INNER_JOB_ID) ?
                       map.get(INNER_JOB_ID).toString() : null;

        if (jobId == null) {
            jobId = submitVermeerTaskAsyn(vermeerParams);
            LOG.info("New vermeer task {} is submitted to vermeer :{}", jobId,
                     vermeerParams);
            this.innerJobId = jobId;
            map = fromJson(this.task().input(), Map.class);
            map.put(INNER_JOB_ID, jobId);
            this.task().input(toJson(map));
            LOG.info("Submit a new computer job, ID is {}", jobId);
        }

        // Watch job status here
        waitVermeerTaskSuccess(jobId, this.task());
        map = fromJson(this.task().input(), Map.class);
        status = map.get(INNER_STATUS).toString();
        if ("error".equals(status)) {
            throw new Exception(String.format("Vermeer task failed:%s",
                                              vermeerParams));
        }
        this.vermeerDriver.close();
        this.vermeerDriver = null;
        return status;
    }

    private void waitVermeerTaskSuccess(String id, HugeTask<Object> task) throws
                                                                          InterruptedException {

        Map<String, Object> innerMap = fromJson(this.task().input(), Map.class);

        String jobStatus;
        do {
            RestResult res = this.vermeerDriver().get(
                    String.join("/", "task", id));
            Map map = res.readObject(Map.class);
            jobStatus = (String) ((Map) map.get("task")).get("status");
            innerMap.put(INNER_STATUS, jobStatus);
            Thread.sleep(5000);
            this.task().input(toJson(innerMap));
        } while (!("error".equals(jobStatus) || "complete".equals(jobStatus) ||
                   "loaded".equals(jobStatus)));
        // Update jobId is missing
        String jobId = innerMap.containsKey(INNER_JOB_ID) ?
                       innerMap.get(INNER_JOB_ID).toString() : null;
        if (null == jobId && this.innerJobId != null) {
            innerMap.put(INNER_JOB_ID, this.innerJobId);
        }

        // We overwrite the task status by jobStatus
        if ("complete".equals(jobStatus) || "loaded".equals(jobStatus)) {
            this.task().result(TaskStatus.SUCCESS, jobStatus);
        } else {
            this.task().result(TaskStatus.FAILED, jobStatus);
        }
        // Update computer stage info
        this.save();
        LOG.debug("Task {} stage changed, current status is {}}",
                  this.task().id(), jobStatus);
    }

    private String submitVermeerTaskAsyn(Map<String, Object> vermeerParams) throws
                                                                            Exception {
        RestResult res =
                this.vermeerDriver().post("tasks/create", vermeerParams);
        Map innerMap;
        if (res.status() == 200) {
            innerMap = (Map) (res.readObject(Map.class).get("task"));
        } else {
            throw new Exception(String.format("Failed to request Vermeer, Vermeer's " +
                                              "response :  %s" +
                                              "Please check whether the " +
                                              "vermeer URL is " +
                                              "configured correctly or " +
                                              "whether the vermeer " +
                                              "service is alive",
                                              res.readObject(Map.class)));
        }

        return String.valueOf(innerMap.get("id"));
    }
}
