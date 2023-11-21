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

import org.apache.hugegraph.testutil.AuthApiUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class GraphsApiTest extends BaseApiTest {

    private static final String TEMP_SPACE = "graph_test";
    private static final String TEMP_AUTH_SPACE = "graph_auth_test";
    private static final String PATH = "graphspaces/graph_test/graphs";
    private static final String PATH_AUTH = "graphspaces/graph_auth_test" +
                                            "/graphs";

    @BeforeClass
    public static void prepareSpace() {
        createSpace(TEMP_SPACE, false);
        createSpace(TEMP_AUTH_SPACE, true);
    }

    @AfterClass
    public static void tearDown() {
        clearSpaces();
    }

    @Test
    public void testUpdateNickname() {
        String name = "update_nickname";
        Response r = createGraph(TEMP_SPACE, name);
        assertResponseStatus(201, r);

        r = updateGraph("update", TEMP_SPACE, name, name);
        assertResponseStatus(200, r);
    }

    @Test
    public void testUpdateNicknameAuthed() {
        String name = "update_nickname_authed";
        Response r = createGraph(TEMP_AUTH_SPACE, name);
        assertResponseStatus(201, r);

        String userSpace = "user_space";
        String userAnalyst = "user_analyst";
        String userObserver = "user_observer";
        AuthApiUtils.createUser(client(), userSpace, userSpace);
        AuthApiUtils.createManager(client(), userSpace, "SPACE",
                                   TEMP_AUTH_SPACE);
        AuthApiUtils.createUser(client(), userAnalyst, userAnalyst);
        AuthApiUtils.createDefaultRole(client(), TEMP_AUTH_SPACE, userAnalyst,
                                       "analyst", null);
        AuthApiUtils.createUser(client(), userObserver, userObserver);
        AuthApiUtils.createDefaultRole(client(), TEMP_AUTH_SPACE, userObserver,
                                       "observer", name);

        r = updateGraph("update", TEMP_AUTH_SPACE, name, name + "1");
        assertResponseStatus(200, r);
    }

    @Test
    public void testWrongNickname() {
        String name = "wrong_nickname";
        Response r = createGraph(TEMP_SPACE, name);
        assertResponseStatus(201, r);

        r = updateGraph("update", TEMP_SPACE, name, "错误 名称");
        assertErrorContains(r, "Invalid nickname");
    }

    @Test
    public void testDeleteGraph() {
        Response r = createGraph(TEMP_SPACE, "delete");
        assertResponseStatus(201, r);

        r = client().delete(PATH, "delete");
        assertResponseStatus(204, r);
    }
}
