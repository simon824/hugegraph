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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class UserApiTest extends BaseApiTest {

    private static final String path = "auth/users";
    private static final int NO_LIMIT = -1;

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();
        Response r = this.client().get(path,
                                       ImmutableMap.of("limit", NO_LIMIT));
        String result = r.readEntity(String.class);
        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result,
                                  new TypeReference<Map<String,
                                          List<Map<String, Object>>>>() {
                                  });
        List<Map<String, Object>> users = resultMap.get("users");
        for (Map<String, Object> user : users) {
            if (user.get("user_name").equals("admin")) {
                continue;
            }
            this.client().delete(path, (String) user.get("id"));
        }
    }

    @Test
    public void testCreate() {
        String user1 = "{\"user_name\":\"create_user1\",\"user_password\":\"passwrod1\"," +
                       "\"user_email\":\"user1@baidu.com\",\"user_phone\":" +
                       "\"123456789\",\"user_avatar\":\"image1.jpg\"}";

        String user2 = "{\"user_name\":\"create_user2\",\"user_password\":\"passwrod2\"," +
                       "\"user_email\":\"user2@baidu.com\"," +
                       "\"user_phone\":\"1357924680\"," +
                       "\"user_avatar\":\"image2.jpg\"}";

        Response r = client().post(path, user1);
        String result = assertResponseStatus(201, r);
        Response r2 = client().post(path, user2);
        String result2 = assertResponseStatus(201, r2);

        assertJsonContains(result, "user_name");
        assertJsonContains(result, "user_password");
        assertJsonContains(result, "user_email");
        assertJsonContains(result, "user_phone");
        assertJsonContains(result, "user_avatar");

        assertJsonContains(result2, "user_name");
        assertJsonContains(result2, "user_password");
        assertJsonContains(result2, "user_email");
        assertJsonContains(result2, "user_phone");
        assertJsonContains(result2, "user_avatar");

        Response r3 = client().post(path, "{}");
        assertResponseStatus(400, r3);

        String user3 = "{\"user_name\":\"create_user1\",\"user_password\":\"passwrod3\"," +
                       "\"user_email\":\"user1@baidu.com\"," +
                       "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                       ".jpg\"}";
        Response r4 = client().post(path, user3);
        String result4 = assertResponseStatus(400, r4);
        Map<String, String> resultMap = JsonUtil.fromJson(result4, Map.class);
        Assert.assertTrue(resultMap.get("message").contains("has existed"));
    }

    @Test
    public void testCreateBatch() {
        Map<String, String> user1 = ImmutableMap.of("user_name", "create_user1",
                                                    "user_password", "password1");
        Map<String, String> user2 = ImmutableMap.of("user_name", "create_user1",
                                                    "user_password", "password1");

        Map<String, String> user3 = ImmutableMap.of("name", "create_user3",
                                                    "user_password",
                                                    "password3");

        Map<String, String> user4 =
                ImmutableMap.of("user_name", "create_user4",
                                "user_nickname", "create user4",
                                "user_password", "password");

        Map<String, String> user5 =
                ImmutableMap.of("user_name", "create_user5");


        List<Map<String, String>> data = new ArrayList<>(10);
        data.add(user1);
        data.add(user2);
        data.add(user3);
        data.add(user4);
        data.add(user5);

        Response r = client().post(path + "/batch", JsonUtil.toJson(data));
        String result = assertResponseStatus(201, r);
        List<Map<String, Object>> results = assertJsonContains(result,
                                                               "result");
        for (Map<String, Object> resMap : results) {
            String message = (String) resMap.get("result");
            Assert.assertTrue(message,
                              message.contains("successfully created") ||
                              message.contains("error: user_name existed") ||
                              message.contains("Invalid nickname") ||
                              message.contains("error: parameter 'user_name' " +
                                               "not found") ||
                              message.contains("The password is"));
        }
    }

    @Test
    public void testList() {
        createUser("test_list1");
        createUser("test_list2");
        createUser("test_list3");
        List<Map<String, Object>> users = listUsers();
        assert users.size() > 3 : users.size();
    }

    @Test
    public void testGetUser() {
        createUser("test_get1");
        createUser("test_get2");
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            Response r = client().get(path, (String) user.get("id"));
            String result = assertResponseStatus(200, r);
            assertJsonContains(result, "user_name");
        }
    }

    @Test
    public void testUpdate() {
        createUser("test_update1");
        createUser("test_update2");
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            if (user.get("user_name").equals("admin")) {
                continue;
            }
            String user1 = "{\"user_password\":\"password1\"," +
                           "\"user_email\":\"user1@baidu.com\"," +
                           "\"user_phone\":\"111111\"," +
                           "\"user_avatar\":\"image1" +
                           ".jpg\"}";
            Response r = client().put(path, (String) user.get("id"), user1,
                                      ImmutableMap.of());
            assertResponseStatus(200, r);
        }
    }

    @Test
    public void testDelete() {
        createUser("test_del1");
        createUser("test_del2");
        createUser("test_del3");

        client().delete(path, "test_del1");
        client().delete(path, "test_del2");
        client().delete(path, "test_del3");

        Response r = client().delete(path, "test_del1");
        String result = assertResponseStatus(400, r);
        Assert.assertThat(result,
                          CoreMatchers.containsString("not existed"));
    }

    protected void createUser(String name) {
        String user = "{\"user_name\":\"" + name + "\",\"user_password\":\"password1" +
                      "\", \"user_email\":\"user1@baidu.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";
        Response r = this.client().post(path, user);
        assertResponseStatus(201, r);
    }

    protected List<Map<String, Object>> listUsers() {
        Response r = this.client().get(path, ImmutableMap.of("limit",
                                                             NO_LIMIT));
        String result = assertResponseStatus(200, r);

        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result, new TypeReference<Map<String,
                        List<Map<String, Object>>>>() {
                });
        return resultMap.get("users");
    }

    @Test
    public void testAuthContext() {
        // Test org.apache.hugegraph.auth.HugeAuthenticator.User.USERCACHES

        HugeAuthenticator.User admin = HugeAuthenticator.User.ADMIN;
        admin.toJson();
        admin.toJson();

        HugeAuthenticator.User.fromJson(admin.toJson());
        HugeAuthenticator.User.fromJson(admin.toJson());
    }
}
