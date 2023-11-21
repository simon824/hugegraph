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

package org.apache.hugegraph.api.traversers;

import java.util.Map;

import org.apache.hugegraph.api.BaseApiTest;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class PersonalRankApiTest extends BaseApiTest {

    final static String path = TRAVERSERS_API + "/personalrank";
    static final String PATH_NEW = TRAVERSERS_API + "/personalrank/batch";
    static final String PATH_REPLAY_WRITE = "/graphs/hugegraphapi/graph" +
                                            "/vertices/reply_write";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testPersonalRank() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String peterId = name2Ids.get("peter");
        String reqBody = String.format("{" +
                                       "\"source\":\"%s\"," +
                                       "\"max_depth\":\"%s\"," +
                                       "\"label\":\"%s\"," +
                                       "\"alpha\":\"%s\"}",
                                       markoId, 3, "created", 1);
        Response r = client().post(path, reqBody);
        String content = assertResponseStatus(200, r);
        Map<String, String> personalRank = assertJsonContains(content, "personal_rank");
        assertMapContains(personalRank, peterId);
    }

    @Test
    public void testPersonalRankNew() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String peterId = name2Ids.get("peter");
        String reqBody = String.format("{" +
                                       "\"source\":\"%s\"," +
                                       "\"max_depth\":\"%s\"," +
                                       "\"labels\":[\"%s\"]," +
                                       "\"alpha\":\"%s\"}",
                                       markoId, 3, "created", 1);
        Response r = client().post(PATH_NEW, reqBody);
        String content = assertResponseStatus(200, r);
    }

    @Test
    public void testReplyWrite() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String peterId = name2Ids.get("peter");
        String reqBody = String.format("{\n" +
                                       "    \"source\": \"%s\",\n" +
                                       "    \"max_depth\": \"%s\",\n" +
                                       "    \"labels\": [\"%s\"],\n" +
                                       "    \"reply_field\": \"rank\",\n" +
                                       "    \"with_label\": \"BOTH_LABEL\",\n" +
                                       "    \"max_degree\": 1000,\n" +
                                       "    \"limit\": 500,\n" +
                                       "    \"direction\": \"OUT\",\n" +
                                       "    \"write_vertex_flag\":false,\n" +
                                       "    \"a_filter_value_by_name" +
                                       "\":\"city\",\n" +
                                       "    \"b_filter_value_by_name" +
                                       "\":\"city\"\n" +
                                       "}",
                                       markoId, 3, "created");
        Response r = client().post(PATH_REPLAY_WRITE, reqBody);
        String content = assertResponseStatus(200, r);
    }

    @Test
    public void testReplyWriteIdNull() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String peterId = name2Ids.get("peter");
        String reqBody = String.format("{\n" +
                                       "    \"source\": \"%s\",\n" +
                                       "    \"max_depth\": \"%s\",\n" +
                                       "    \"labels\": [\"%s\"],\n" +
                                       "    \"reply_field\": \"rank\",\n" +
                                       "    \"with_label\": \"BOTH_LABEL\",\n" +
                                       "    \"max_degree\": 1000,\n" +
                                       "    \"limit\": 500,\n" +
                                       "    \"direction\": \"BOTH\",\n" +
                                       "    \"write_vertex_flag\":false,\n" +
                                       "    \"a_filter_value_by_name" +
                                       "\":\"city\",\n" +
                                       "    \"b_filter_value_by_name" +
                                       "\":\"city\"\n" +
                                       "}",
                                       null, 3, "created");
        Response r = client().post(PATH_REPLAY_WRITE, reqBody);
        String content = assertResponseStatus(200, r);
    }
}
