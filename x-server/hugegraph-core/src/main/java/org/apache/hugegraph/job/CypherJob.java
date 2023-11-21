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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.cypher.CypherExcuteClient;
import org.apache.hugegraph.cypher.CypherModel;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;

/**
 * Cypher执行异步任务
 */
public class CypherJob extends UserJob<Object> {

    public static final int TASK_RESULTS_MAX_SIZE = (int) HugeTraverser.DEF_CAPACITY;
    public String taskType = "cypher";
    public CypherExcuteClient cypherExcuteClient = null;

    public CypherJob() {
        cypherExcuteClient = new CypherExcuteClient();
    }

    @Override
    public String type() {
        return taskType;
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);

        // 获取cypher语句
        Object value = map.get("gremlin");
        E.checkArgument(value instanceof String,
                        "Invalid cypher value '%s'", value);
        String cypher = (String) value;

        value = map.get("aliases");
        E.checkArgument(value instanceof Map,
                        "Invalid aliases value '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, String> aliasesRequest = (Map<String, String>) value;
        String graphspace = aliasesRequest.get("graphspace");
        String graph = aliasesRequest.get("graph");
        String auth = aliasesRequest.get("authorization");
        E.checkArgument(StringUtils.isNotEmpty(graphspace), "Invalid graphspace value '%s'",
                        graphspace);
        E.checkArgument(StringUtils.isNotEmpty(graph), "Invalid graph value '%s'", graph);
        E.checkArgument(StringUtils.isNotEmpty(auth), "Invalid auth value '%s'", auth);

        String graphInfo = graphspace + "-" + graph;
        Map<String, String> aliases = new HashMap<>(2, 1);
        aliases.put("graph", graphInfo);
        aliases.put("g", "__g_" + graphInfo);

        CypherModel cypherModel = this.cypherExcuteClient.client(auth)
                                                         .submitQuery(cypher,
                                                                      aliases);
        checkResultsSize(cypherModel.result.getData());
        return cypherModel;

    }


    private void checkResultsSize(Object results) {
        int size = 0;
        if (results instanceof Collection) {
            size = ((Collection<?>) results).size();
        }
        if (size > TASK_RESULTS_MAX_SIZE) {
            throw new LimitExceedException(
                    "Job results size %s has exceeded the max limit %s",
                    size, TASK_RESULTS_MAX_SIZE);
        }
    }
}
