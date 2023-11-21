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

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class GraphSpaceApiTest extends BaseApiTest {

    private static final String PATH = "graphspaces";

    @Before
    public void removeSpaces() {
        Response r = this.client().get(PATH);
        String result = r.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> spaces = (List<String>) resultMap.get("graphSpaces");
        for (String space : spaces) {
            if (!"DEFAULT".equals(space)) {
                this.client().delete(PATH, space);
            }
        }
    }

    @Test
    public void testAddSpaceNamespace() {
        String body = "{\n" +
                      "  \"name\": \"test_add_no_ns\",\n" +
                      "  \"nickname\":\"测试无名称空间\",\n" +
                      "  \"description\": \"no namespace\",\n" +
                      "  \"cpu_limit\": 1000,\n" +
                      "  \"memory_limit\": 1024,\n" +
                      "  \"storage_limit\": 1000,\n" +
                      "  \"compute_cpu_limit\": 0,\n" +
                      "  \"compute_memory_limit\": 0,\n" +
                      "  \"oltp_namespace\": null,\n" +
                      "  \"olap_namespace\": null,\n" +
                      "  \"storage_namespace\": null,\n" +
                      "  \"operator_image_path\": \"aaa\",\n" +
                      "  \"internal_algorithm_image_url\": \"aaa\",\n" +
                      "  \"max_graph_number\": 100,\n" +
                      "  \"max_role_number\": 100,\n" +
                      "  \"auth\": false,\n" +
                      "  \"configs\": {}\n" +
                      "}";
        Response r = this.client().post(PATH, body);
        assertResponseStatus(201, r);

        String body2 = "{\n" +
                       "  \"name\": \"test_add_has_ns\",\n" +
                       "  \"nickname\":\"测试有名称空间\",\n" +
                       "  \"description\": \"has namespace\",\n" +
                       "  \"cpu_limit\": 1000,\n" +
                       "  \"memory_limit\": 1024,\n" +
                       "  \"storage_limit\": 1000,\n" +
                       "  \"compute_cpu_limit\": 0,\n" +
                       "  \"compute_memory_limit\": 0,\n" +
                       "  \"oltp_namespace\": \"oltp5\",\n" +
                       "  \"olap_namespace\": \"olap5\",\n" +
                       "  \"storage_namespace\": \"st5\",\n" +
                       "  \"operator_image_path\": \"aaa\",\n" +
                       "  \"internal_algorithm_image_url\": \"aaa\",\n" +
                       "  \"max_graph_number\": 100,\n" +
                       "  \"max_role_number\": 100,\n" +
                       "  \"auth\": false,\n" +
                       "  \"configs\": {}\n" +
                       "}";
        r = this.client().post(PATH, body2);
        assertResponseStatus(201, r);
    }

    @Test
    public void testGetSpace() {
        Response r = this.client().get(PATH + "/DEFAULT");
        assertResponseStatus(200, r);
    }

    @Test
    public void testGetSpaceProfile() {
        Response r = this.client().get(PATH + "/profile");
        assertResponseStatus(200, r);
    }

    @Test
    public void testGetStorageInfo() {
        Response r = this.client().get(PATH + "/profile");
        String result = assertResponseStatus(200, r);
        List<Map<String, Object>> resultMap =
                JsonUtil.fromJson(result, new TypeReference<List<Map<String,
                        Object>>>() {
                });
        Assert.assertNotNull(resultMap.get(0).get("storage_percent"));
    }
}
