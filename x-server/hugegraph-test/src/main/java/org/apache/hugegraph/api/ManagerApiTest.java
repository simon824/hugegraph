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

import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class ManagerApiTest extends BaseApiTest {

    private static final String USER_PATH = "auth/users";
    private static final String PATH = "auth/managers";
    private static final int NO_LIMIT = -1;

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();
        deleteSpaceAdmins();
        deleteAdmins();
        deleteUsers();
    }

    public void deleteAdmins() {
        Response r = this.client().get(PATH, ImmutableMap.of("type",
                                                             HugePermission.ADMIN));
        String result = r.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> admins = (List<String>) resultMap.get("admins");
        for (String user : admins) {
            if ("admin".equals(user)) {
                continue;
            }
            this.client().delete(PATH, ImmutableMap.of("user", user, "type",
                                                       HugePermission.ADMIN));
        }
    }

    public void deleteSpaceAdmins() {
        Response r1 = this.client().get("/graphspaces");
        String result = r1.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> spaces = (List<String>) resultMap.get("graphSpaces");
        for (String space : spaces) {
            Response r = this.client().get(PATH,
                                           ImmutableMap.of("type",
                                                           HugePermission.SPACE,
                                                           "graphspace",
                                                           space));
            result = r.readEntity(String.class);
            resultMap = JsonUtil.fromJson(result, Map.class);
            List<String> spaceAdmins = (List<String>) resultMap.get("admins");
            for (String user : spaceAdmins) {
                this.client().delete(PATH, ImmutableMap.of("user", user, "type",
                                                           HugePermission.SPACE,
                                                           "graphspace", space));
            }
        }
    }

    public void deleteUsers() {
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            if (user.get("user_name").equals("admin")) {
                continue;
            }
            this.client().delete(USER_PATH, (String) user.get("id"));
        }
    }

    @Test
    public void testCreate() {
        this.createUser("create_user1");
        this.createUser("create_user2");

        String admin1 = "{\"user\":\"create_user1\"," +
                        "\"type\":\"ADMIN\"}";

        String space1 = "{\"user\":\"create_user2\"," +
                        "\"type\":\"SPACE\"," +
                        "\"graphspace\":\"DEFAULT\"}";

        Response r = client().post(PATH, admin1);
        assertResponseStatus(201, r);
        r = client().post(PATH, space1);
        assertResponseStatus(201, r);

        String admin2 = "{\"user\":\"create_user1\"," +
                        "\"type\":\"READ\"}";
        r = client().post(PATH, admin2);
        String result = assertResponseStatus(400, r);
        Map<String, String> resultMap = JsonUtil.fromJson(result, Map.class);
        Assert.assertTrue(resultMap.get("message").contains("The type could be"));

        String admin3 = "{\"user\":\"create_user1\"," +
                        "\"type\":\"ADMIN2\"}";
        r = client().post(PATH, admin3);
        result = assertResponseStatus(400, r);
        Assert.assertTrue(result.contains("Cannot deserialize value of type"));

        String admin4 = "{\"user\":\"create_user3\"," +
                        "\"type\":\"ADMIN\"}";
        r = client().post(PATH, admin4);
        result = assertResponseStatus(400, r);
        resultMap = JsonUtil.fromJson(result, Map.class);
        Assert.assertTrue(resultMap.get("message").contains("The user or group is not exist"));

        String space2 = "{\"user\":\"create_user2\"," +
                        "\"type\":\"SPACE\"," +
                        "\"graphspace\":\"DEFAULTD\"}";
        r = client().post(PATH, space2);
        result = assertResponseStatus(400, r);
        resultMap = JsonUtil.fromJson(result, Map.class);
        Assert.assertTrue(resultMap.get("message").contains("The graph space is not exist"));
    }

    protected void createUser(String name) {
        String user = "{\"user_name\":\"" + name + "\",\"user_password\":\"password1" +
                      "\", \"user_email\":\"user1@baidu.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";
        Response r = this.client().post(USER_PATH, user);
        assertResponseStatus(201, r);
    }

    protected List<Map<String, Object>> listUsers() {
        Response r = this.client().get(USER_PATH, ImmutableMap.of("limit",
                                                                  NO_LIMIT));
        String result = assertResponseStatus(200, r);

        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result, new TypeReference<Map<String,
                        List<Map<String, Object>>>>() {
                });
        return resultMap.get("users");
    }
}
