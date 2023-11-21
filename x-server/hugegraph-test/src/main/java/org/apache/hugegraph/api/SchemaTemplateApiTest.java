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

import org.apache.hugegraph.util.JsonUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class SchemaTemplateApiTest extends BaseApiTest {

    private static final String TEMP_SPACE = "templates";
    private static final String PATH = "graphspaces/templates/schematemplates";

    private static RestClient normal;
    private static RestClient space;
    private static RestClient analyst;

    @BeforeClass
    public static void prepareSpace() {
        createSpace(TEMP_SPACE, true);
        normal = userClient("template_normal");
        space = spaceManagerClient(TEMP_SPACE, "template_space");
        analyst = analystClient(TEMP_SPACE, "template_analyst");
    }

    @AfterClass
    public static void clearAll() {
        clearSpaces();
        clearUsers();
    }

    @After
    public void clearTemplates() {
        Response r = client().get(PATH);
        String result = r.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> templates = (List<String>) resultMap.get(
                "schema_templates");
        for (String template : templates) {
            client().delete(PATH, template);
        }
    }

    @Test
    public void updatePermission() {
        String name = "update";
        String body = "{\n" +
                      "  \"name\": \"%s\",\n" +
                      "  \"schema\": \"graph.schema().propertyKey('name')" +
                      ".asText().ifNotExist().create();\"\n" +
                      "}";
        Response r = normal.post(PATH, String.format(body, name));
        assertResponseStatus(201, r);

        r = normal.put(PATH, name, String.format(body, name), ImmutableMap.of());
        assertResponseStatus(200, r);

        r = analyst.put(PATH, name, String.format(body, name), ImmutableMap.of());
        assertErrorContains(r, "No permission to update schema template");

        r = space.put(PATH, name, String.format(body, name), ImmutableMap.of());
        assertResponseStatus(200, r);
    }

    @Test
    public void updateCheck() {
        String name = "update_check";
        String body = "{\n" +
                      "  \"name\": \"%s\",\n" +
                      "  \"schema\": \"graph.schema().propertyKey('name')" +
                      ".asText().ifNotExist().create();\"\n" +
                      "}";
        Response r = normal.post(PATH, String.format(body, name));
        assertResponseStatus(201, r);

        String bodyNull = "{\n" +
                          "  \"name\": null,\n" +
                          "  \"schema\": \"graph.schema().propertyKey('name')" +
                          ".asText().ifNotExist().create();\"\n" +
                          "}";

        r = normal.put(PATH, name, bodyNull, ImmutableMap.of());
        assertResponseStatus(200, r);

        r = normal.put(PATH, name, String.format(body, "changed"),
                       ImmutableMap.of());
        assertErrorContains(r, "The name of template can't be updated");
    }

    @Test
    public void deletePermission() {
        String name = "delete";
        String body = "{\n" +
                      "  \"name\": \"%s\",\n" +
                      "  \"schema\": \"graph.schema().propertyKey('name')" +
                      ".asText().ifNotExist().create();\"\n" +
                      "}";
        Response r = normal.post(PATH, String.format(body, name));
        assertResponseStatus(201, r);

        r = analyst.delete(PATH, name);
        assertErrorContains(r, "No permission to delete schema template");

        r = normal.delete(PATH, name);
        assertResponseStatus(204, r);

        normal.post(PATH, String.format(body, name));
        r = space.delete(PATH, name);
        assertResponseStatus(204, r);
    }

    public Response createTemplate(String name) {
        String body = "{\n" +
                      "  \"name\": \"%s\",\n" +
                      "  \"schema\": \"graph.schema().propertyKey('name')" +
                      ".asText().ifNotExist().create();\"\n" +
                      "}";
        Response r = client().post(PATH, String.format(body, name));
        assertResponseStatus(201, r);
        return r;
    }
}
