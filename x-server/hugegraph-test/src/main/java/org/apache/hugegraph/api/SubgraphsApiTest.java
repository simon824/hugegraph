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

package org.apache.hugegraph.api;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class SubgraphsApiTest extends BaseApiTest {

    private static final String PATH = "graphspaces/DEFAULT/graphs";
    private static final int WAIT_LIMIT = 5;

    private static final String VERTEX = "vertex";
    private static final String EDGE = "edge";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    public void createSubgraphIfNotExisted() {
        String subgraph = "sub";
        Response r = client().get(PATH);
        String result = assertResponseStatus(200, r);
        Map<String, List<String>> resultMap =
                JsonUtil.fromJson(result,
                                  new TypeReference<Map<String, List<String>>>() {
                                  });
        if (!resultMap.get("graphs").contains(subgraph)) {
            createSubgraph(subgraph);
        }
    }

    public void createSubgraph(String subgraph) {
        String body = "{\n" +
                      "  \"backend\": \"hstore\",\n" +
                      "  \"serializer\": \"binary\",\n" +
                      "  \"store\": \"%s\",\n" +
                      "  \"search.text_analyzer\": \"jieba\",\n" +
                      "  \"search.text_analyzer_mode\": \"INDEX\"\n" +
                      "}";
        String config = String.format(body, subgraph);
        String url = String.join(DELIMETER, PATH, subgraph);
        Response r = client().post(url, config);
        assertResponseStatus(201, r);
    }

    public void waitAllTaskSuccess(String graph) {
        int count = 0;
        boolean keep = true;
        do {
            String taskPath = String.format("/graphs/%s/tasks", graph);
            Response r = client().get("/graphs/hugegraphapi/tasks");
            String content = assertResponseStatus(200, r);
            List<Map<String, Object>> tasks = JsonUtil.fromJson(content,
                                                                List.class);
            keep = false;
            for (Map<String, Object> task : tasks) {
                String status = assertMapContains(task, "task_status");
                if (TaskStatus.COMPLETED_STATUSES.contains(TaskStatus.fromName(status))) {
                    Assert.assertEquals(status, TaskStatus.SUCCESS.name());
                } else {
                    keep = true;
                    break;
                }
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new HugeException("test subgraph exception: %s",
                                        e.getMessage());
            }
            count += 1;
        } while (count < WAIT_LIMIT && keep);
    }

    //@AfterClass
    public void clearSub() {
        String update = "{\n" +
                        "  \"action\": \"clear\",\n" +
                        "  \"clear_schema\": true\n" +
                        "}";
        Response r = client().put(PATH, "sub", update, ImmutableMap.of());
        assertResponseStatus(200, r);
    }

    public String partBody(String graph, boolean create, boolean initSchema,
                           String type) {
        return String.format("  \"configs\":{\n" +
                             "    \"backend\": \"hstore\",\n" +
                             "    \"serializer\": \"binary\",\n" +
                             "    \"name\": \"%s\",\n" +
                             "    \"search.text_analyzer\": \"jieba\",\n" +
                             "    \"search.text_analyzer_mode\": \"INDEX\"\n" +
                             "  },\n" +
                             "  \"create\":%s,\n" +
                             "  \"init_schema\":%s,\n" +
                             "  \"type\":\"%s\",\n",
                             graph, create, initSchema, type);
    }

    @Test
    public void testSubgraphVertexIds() {
        String graph = "subvids";
        String body = "{\n" + partBody(graph, true, true, VERTEX) +
                      "  \"ids\": [\"1:marko\", \"1:josh\"],\n" +
                      "  \"steps\": [],\n" +
                      "  \"shard\":{}\n" +
                      "}";
        Response r = this.client().post(PATH + "/hugegraphapi/subgraph", body);
        assertResponseStatus(201, r);
    }

    @Test
    public void testSubgraphVertexIdsNumber() {
        String graph = "subvidsnum";
        String body = "{\n" + partBody(graph, true, true, VERTEX) +
                      "  \"ids\": [279, 302],\n" +
                      "  \"steps\": [],\n" +
                      "  \"shard\":{}\n" +
                      "}";
        Response r = this.client().post(PATH + "/hugegraphapi/subgraph", body);
        assertResponseStatus(201, r);
    }

    @Test
    public void testSubgraphEdgeIds() {
        String graph = "subeids";
        String body = "{\n" + partBody(graph, true, true, EDGE) +
                      "  \"ids\": [\"1:marko\", \"1:josh\"],\n" +
                      "  \"steps\": [],\n" +
                      "  \"shard\":{}\n" +
                      "}";
        Response r = this.client().post(PATH + "/hugegraphapi/subgraph", body);
        assertResponseStatus(201, r);
    }

    @Test
    public void testSubgraphVertexSteps() {
        String graph = "subvsteps";
        String body = "{\n" + partBody(graph, true, true, VERTEX) +
                      "  \"steps\":[\n" +
                      "          {\n" +
                      "                \"label\": \"person\",\n" +
                      "                \"properties\": {\n" +
                      "                  \"city\": \"Beijing\"\n" +
                      "                }\n" +
                      "            }\n" +
                      "        ]\n" +
                      "}";
        Response r = this.client().post(PATH + "/hugegraphapi/subgraph", body);
        assertResponseStatus(201, r);
    }

    @Test
    public void testSubgraphEdgeSteps() {
        String graph = "subesteps";
        String body = "{\n" + partBody(graph, true, true, EDGE) +
                      "  \"steps\":[\n" +
                      "          {\n" +
                      "                \"label\": \"knows\",\n" +
                      "                \"properties\": {\n" +
                      "                  \"weight\": \"P.gt(0.1)\"\n" +
                      "                }\n" +
                      "            }\n" +
                      "        ]\n" +
                      "}";
        Response r = this.client().post(PATH + "/hugegraphapi/subgraph", body);
        assertResponseStatus(201, r);
    }

    @Test
    public void testSubgraphVertexShard() {
        Response r = client().get(TRAVERSERS_API + "/vertices/shards",
                                  ImmutableMap.of("split_size", 1048576));
        String content = assertResponseStatus(200, r);
        List<Map> shards = assertJsonContains(content, "shards");
        org.junit.Assert.assertNotNull(shards);
        Assert.assertFalse(shards.isEmpty());

        String end = assertMapContains(shards.get(shards.size() - 1), "end");

        String graph = "subvshards";
        String body = String.format("{\n" + partBody(graph, true, true, VERTEX) +
                                    "  \"shard\":{\n" +
                                    "    \"start\": \"0\",\n" +
                                    "    \"end\": \"%s\"\n" +
                                    "  }\n" +
                                    "}", end);
        r = this.client().post(PATH + "/hugegraphapi/subgraph", body);
        assertResponseStatus(201, r);
    }

    @Test
    public void testSubgraphEdgeShard() {
        Response r = client().get(TRAVERSERS_API + "/edges/shards",
                                  ImmutableMap.of("split_size", 1048576));
        String content = assertResponseStatus(200, r);
        List<Map> shards = assertJsonContains(content, "shards");
        org.junit.Assert.assertNotNull(shards);
        Assert.assertFalse(shards.isEmpty());

        String end = assertMapContains(shards.get(shards.size() - 1), "end");
        String graph = "subeshards";
        String body = String.format("{\n" +
                                    partBody(graph, true, true, EDGE) +
                                    "  \"loop_limit\": 2,\n" +
                                    "  \"range\":\"4\",\n" +
                                    "  \"batch_size\":\"2000\"," +
                                    "  \"ids\": [],\n" +
                                    "  \"steps\": [],\n" +
                                    "  \"shard\":{\n" +
                                    "    \"start\": \"0\",\n" +
                                    "    \"end\": \"%s\"\n" +
                                    "  }\n" +
                                    "}", end);
        r = this.client().post(PATH + "/hugegraphapi/subgraph", body);
        assertResponseStatus(201, r);
    }

    @Test
    public void testClone() {
        String graph = "clone";
        String body = "{\n" +
                      cloneBody(graph, true, true, true) +
                      "\n}";
        Response r = this.client().post(PATH + "/hugegraphapi/clone", body);
        assertResponseStatus(201, r);
    }

    public String cloneBody(String graph, boolean create, boolean initSchema,
                            boolean loadData) {
        return String.format("  \"configs\":{\n" +
                             "    \"backend\": \"hstore\",\n" +
                             "    \"serializer\": \"binary\",\n" +
                             "    \"name\": \"%s\",\n" +
                             "    \"search.text_analyzer\": \"jieba\",\n" +
                             "    \"search.text_analyzer_mode\": \"INDEX\"\n" +
                             "  },\n" +
                             "  \"create\":%s,\n" +
                             "  \"init_schema\":%s,\n" +
                             "  \"load_data\":%s\n",
                             graph, create, initSchema, loadData);
    }
}
