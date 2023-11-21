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

import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.AuthApiUtils;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class AuthApiTest extends BaseApiTest {

    private static final String SPACE = "authed";
    // use authed as test space
    private static final String AUTH_PATH = "graphspaces/authed/auth";
    private static final String BELONG_PATH = AUTH_PATH + "/belongs";
    private static final String ROLE_PATH = AUTH_PATH + "/roles";
    private static final String ACCESS_PATH = AUTH_PATH + "/accesses";
    private static final String TARGET_PATH = AUTH_PATH + "/targets";
    private static final String SPACE_PATH = "graphspaces";
    private static final String USER_PATH = "auth/users";

    private static final int NO_LIMIT = -1;

    @BeforeClass
    public static void createAuthedSpace() {
        String body = "{\n" +
                      "  \"name\": \"authed\",\n" +
                      "  \"nickname\":\"权限测试空间\",\n" +
                      "  \"description\": \"authed\",\n" +
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
                      "  \"auth\": true,\n" +
                      "  \"configs\": {}\n" +
                      "}";
        try {
            RestClient client = newClient();
            client.post(SPACE_PATH, body);
            client.close();
        } catch (Exception e) {
            Assert.assertContains("has existed", e.getMessage());
        }
    }

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();
        clearUsers();
        clearRoles();
        clearTargets();

        // check no belongs and accesses
    }

    public void clearRoles() {
        Response r = this.client().get(ROLE_PATH);
        String result = r.readEntity(String.class);
        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result,
                                  new TypeReference<Map<String,
                                          List<Map<String, Object>>>>() {
                                  });
        List<Map<String, Object>> roles = resultMap.get("roles");
        for (Map<String, Object> role : roles) {
            if (HugeDefaultRole.isDefault((String) role.get("role_name"))) {
                continue;
            }
            this.client().delete(ROLE_PATH, (String) role.get("id"));
        }
    }

    public void clearTargets() {
        Response r = this.client().get(TARGET_PATH);
        String result = r.readEntity(String.class);
        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result,
                                  new TypeReference<Map<String,
                                          List<Map<String, Object>>>>() {
                                  });
        List<Map<String, Object>> targets = resultMap.get("targets");
        for (Map<String, Object> target : targets) {
            if (HugeDefaultRole.isDefaultTarget((String) target.get(
                    "target_name"))) {
                continue;
            }
            this.client().delete(TARGET_PATH, (String) target.get("id"));
        }
    }

    @Test
    public void testCreateBelongExisted() {
        String name = "cbetest";
        Response r = this.createUser(name, name);
        assertResponseStatus(201, r);

        r = this.createRole(name, name);
        assertResponseStatus(201, r);

        r = this.createBelong(name, name, "");
        assertResponseStatus(201, r);

        r = this.createBelong("wrong", name, "");
        assertErrorContains(r, "is not existed");

        r = this.createBelong(name, "wrong", "");
        assertErrorContains(r, "is not existed");

        r = this.createBelong("", name, "wrong", "gr");
        assertErrorContains(r, "is not existed");
    }

    @Test
    public void testCreateRoleBlankSpace() {
        Response r = this.createRole("role blank", "");
        assertErrorContains(r, "Invalid name");

        r = this.createRole("roleBlank", "role Blank");
        assertErrorContains(r, "Invalid nickname");

        r = this.createRole("roleBlank", "roleBlank");
        assertResponseStatus(201, r);
    }

    @Test
    public void testCreateRoleDefault() {
        String name = "crd_test";
        String forbidden = "Create role name or nickname like default role is " +
                           "forbidden";
        String space = HugeDefaultRole.SPACE.toString();
        String analyst = HugeDefaultRole.ANALYST.toString();
        String observer = "graph_" + HugeDefaultRole.OBSERVER;

        Response r = this.createRole(space, name);
        assertErrorContains(r, forbidden);

        r = this.createRole(name, HugeDefaultRole.getNickname(space));
        assertErrorContains(r, forbidden);

        r = this.createRole(analyst, name);
        assertErrorContains(r, forbidden);

        r = this.createRole(name, HugeDefaultRole.getNickname(analyst));
        assertErrorContains(r, forbidden);

        r = this.createRole(observer, name);
        assertErrorContains(r, forbidden);

        r = this.createRole(name, HugeDefaultRole.getNickname(observer));
        assertErrorContains(r, "Invalid nickname");
    }

    @Test
    public void testCreateRoleExisted() {
        String name = "cretest";
        Response r = this.createRole(name, "");
        assertResponseStatus(201, r);

        r = this.createRole(name, name);
        assertErrorContains(r, "has existed");
    }

    @Test
    public void testCreateAccessExisted() {
        String name = "caetest";
        Response r = this.createRole(name, name);
        assertResponseStatus(201, r);

        r = this.createTarget(name, "graph");
        assertResponseStatus(201, r);

        r = this.createAccess("wrong", name, "READ");
        assertErrorContains(r, "is not existed");

        r = this.createAccess(name, "wrong", "READ");
        assertErrorContains(r, "is not existed");
    }

    @Test
    public void testCreateAccessWrongPermission() {
        String name = "cawp";
        Response r = this.createAccess(name, name, "SPACE");
        assertErrorContains(r, "The access_permission could only be");

        r = this.createAccess(name, name, "ADMIN");
        assertErrorContains(r, "The access_permission could only be");

        r = this.createAccess(name, name, "format");
        //assertErrorContains(r, "The access_permission could only be");

        r = this.createRole(name, name);
        assertResponseStatus(201, r);

        r = this.createTarget(name, "graph");
        assertResponseStatus(201, r);

        r = this.createAccess(name, name, "NONE");
        assertResponseStatus(201, r);

        r = this.createAccess(name, name, "READ");
        assertResponseStatus(201, r);

        r = this.createAccess(name, name, "WRITE");
        assertResponseStatus(201, r);

        r = this.createAccess(name, name, "DELETE");
        assertResponseStatus(201, r);

        r = this.createAccess(name, name, "EXECUTE");
        assertResponseStatus(201, r);
    }

    @Test
    public void testCreateTargetBlankSpace() {
        Response r = this.createTarget("target blank", "graph");
        assertErrorContains(r, "Invalid name");

        r = this.createTarget("targetBlank", "graph");
        assertResponseStatus(201, r);
    }

    @Test
    public void testUpdateRole() {
        String name = "ur_test";
        Response r = this.createRole(name, name);
        assertResponseStatus(201, r);

        r = this.updateRole(name, "");
        assertResponseStatus(200, r);

        r = this.updateRole(name, name + "_update");
        assertResponseStatus(200, r);
    }

    private Response createUser(String name, String password) {
        return AuthApiUtils.createUser(this.client(), name, password);
    }

    private Response createBelong(String user, String role, String group) {
        return AuthApiUtils.createBelong(this.client(), SPACE, user, role,
                                         group);
    }

    private Response createBelong(String user, String role, String group,
                                  String link) {
        return AuthApiUtils.createBelong(this.client(), SPACE, user, role,
                                         group, link);

    }

    private Response createRole(String name, String nickname) {
        return AuthApiUtils.createRole(this.client(), SPACE, name, nickname);

    }

    private Response updateRole(String name, String nickname) {
        return AuthApiUtils.updateRole(this.client(), SPACE, name, nickname);
    }

    private Response createAccess(String role, String target,
                                  String permission) {
        return AuthApiUtils.createAccess(this.client(), SPACE, role, target,
                                         permission);
    }

    private Response createTarget(String name, String graph) {
        return AuthApiUtils.createTarget(this.client(), SPACE, name, graph);

    }
}
