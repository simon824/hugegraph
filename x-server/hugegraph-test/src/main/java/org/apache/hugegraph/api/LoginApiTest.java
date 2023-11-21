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

import java.nio.file.Paths;
import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

public class LoginApiTest extends BaseApiTest {

    private static final String PATH = "auth";
    private static final String USER_PATH = "auth/users";

    @Before
    public void setup() {
    }

    @Override
    @After
    public void teardown() {
        clearUsers();
    }

    @Test
    public void testLogin() {
        Response r = this.createUser("logintest1", "logintest1");
        Map<String, Object> user = r.readEntity(
                new GenericType<>() {
                });
        String userId = (String) user.get("id");
        assert userId != null : userId;

        r = this.login("logintest1", "logintest1");
        String result = assertResponseStatus(200, r);
        assertJsonContains(result, "token");

        r = this.login("logintest1", "pass1");
        assertResponseStatus(401, r);

        r = this.login("pass1", "logintest1");
        assertResponseStatus(401, r);
    }

    @Test
    public void testLogout() {
        Response r = this.createUser("logouttest1", "logouttest1");
        Map<String, Object> user = r.readEntity(
                new GenericType<>() {
                });
        String userId = (String) user.get("id");
        assert userId != null : userId;

        String result;
        r = this.login("logouttest1", "logouttest1");
        result = assertResponseStatus(200, r);
        assertJsonContains(result, "token");

        String token = this.tokenFromResponse(result);

        String path = Paths.get(PATH, "logout").toString();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        r = client().delete(path, headers);
        assertResponseStatus(204, r);

        String invalidToken =
                "eyJhbGciOiJIUzI1NiJ9" +
                ".eyJ1caVyX25hbWUiOiJ0ZXN0IiwidXNlcl9pZCI6Ii02Mzp0ZXN0IiwiZXhwIjoxNjI0MzUzMjUyfQ" +
                ".kYot-3mSGlfSbEMzxrTs84q8YanhTTxtsKPPG25CNxA";
        headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + invalidToken);
        r = client().delete(path, headers);
        assertResponseStatus(401, r);
    }

    @Test
    public void testVerify() {
        Response r = this.createUser("verifytest1", "verifytest1");
        Map<String, Object> user = r.readEntity(
                new GenericType<>() {
                });
        String userId = (String) user.get("id");
        assert userId != null : userId;

        String result;
        r = this.login("verifytest1", "verifytest1");
        result = assertResponseStatus(200, r);
        assertJsonContains(result, "token");

        String token = this.tokenFromResponse(result);

        String path = Paths.get(PATH, "verify").toString();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        r = client().get(path, headers);

        result = assertResponseStatus(200, r);
        assertJsonContains(result, "user_id");
        assertJsonContains(result, "user_name");

        user = JsonUtil.fromJson(result, new TypeReference<Map<String,
                Object>>() {
        });
        Assert.assertEquals(userId, user.get("user_id"));
        Assert.assertEquals("verifytest1", user.get("user_name"));

        String invalidToken =
                "eyJhbGciOiJIUzI1NiJ9" +
                ".eyJ1caVyX25hbWUiOiJ0ZXN0IiwidXNlcl9pZCI6Ii02Mzp0ZXN0IiwiZXhwIjoxNjI0MzUzMjUyfQ" +
                ".kYot-3mSGlfSbEMzxrTs84q8YanhTTxtsKPPG25CNxA";
        headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + invalidToken);
        r = client().get(path, headers);
        assertResponseStatus(401, r);

        invalidToken = "123.ansfaf";
        headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + invalidToken);
        r = client().get(path, headers);
        assertResponseStatus(401, r);
    }

    @Test
    public void testVerifyUpdatePw() {
        String name = "verify_up_pw";
        Response r = this.createUser(name, name);
        Map<String, Object> user = r.readEntity(
                new GenericType<>() {
                });
        String userId = (String) user.get("id");
        assert userId != null : userId;

        String result;
        r = this.login(name, name);
        result = assertResponseStatus(200, r);
        assertJsonContains(result, "token");

        String token = this.tokenFromResponse(result);
        // update user with same password
        String user1 = "{\"user_password\":\"verify_up_pw\"," +
                       "\"user_email\":\"user1@baidu.com\"," +
                       "\"user_phone\":\"111111\"," +
                       "\"user_avatar\":\"image1" +
                       ".jpg\"}";
        r = client().put(USER_PATH, userId, user1, ImmutableMap.of());
        assertResponseStatus(200, r);

        String path = Paths.get(PATH, "verify").toString();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        r = client().get(path, headers);
        assertResponseStatus(200, r);

        // update user_password
        user1 = "{\"user_password\":\"password1\"," +
                "\"user_email\":\"user1@baidu.com\"," +
                "\"user_phone\":\"111111\"," +
                "\"user_avatar\":\"image1" +
                ".jpg\"}";
        r = client().put(USER_PATH, userId, user1, ImmutableMap.of());
        assertResponseStatus(200, r);

        headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        r = client().get(path, headers);
        assertResponseStatus(401, r);
    }

    private Response createUser(String name, String password) {
        String user = "{\"user_name\":\"%s\",\"user_password\":\"%s" +
                      "\",\"user_email\":\"user1@baidu.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";
        return this.client().post(USER_PATH,
                                  String.format(user, name, password));
    }

    private Response deleteUser(String id) {
        return this.client().delete(USER_PATH, id);
    }

    private Response login(String name, String password) {
        String login = Paths.get(PATH, "login").toString();
        String loginUser = "{\"user_name\":\"%s\"," +
                           "\"user_password\":\"%s\"," +
                           "\"token_expire\":10080}";

        return client().post(login, String.format(loginUser,
                                                  name, password));
    }

    private String tokenFromResponse(String content) {
        Map<String, Object> data = JsonUtil.fromJson(
                content,
                new TypeReference<Map<String, Object>>() {
                });
        return (String) data.get("token");
    }
}