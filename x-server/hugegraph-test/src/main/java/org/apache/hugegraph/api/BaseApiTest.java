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

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

public class BaseApiTest {

    public static final String DELIMETER = "/";
    private static final String GRAPH = "hugegraphapi";
    protected static final String URL_PREFIX = "graphspaces/DEFAULT/graphs/" + GRAPH;
    protected static final String TRAVERSERS_API = URL_PREFIX + "/traversers";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";
    private static final int NO_LIMIT = -1;
    private static final String SCHEMA_PKS = "/schema/propertykeys";
    private static final String SCHEMA_VLS = "/schema/vertexlabels";
    private static final String SCHEMA_ELS = "/schema/edgelabels";
    private static final String SCHEMA_ILS = "/schema/indexlabels";
    private static final String GRAPH_VERTEX = "/graph/vertices";
    private static final String GRAPH_EDGE = "/graph/edges";
    private static final String BATCH = "/batch";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static String BASE_URL = "http://127.0.0.1:8080";
    private static RestClient client;

    @BeforeClass
    public static void init() {
        client = newClient();
        BaseApiTest.initOrClear();
    }

    @AfterClass
    public static void clear() throws Exception {
        client.close();
    }

    public static RestClient newClient() {
        return new RestClient(BASE_URL);
    }

    /**
     * Utils method to init some properties
     */
    protected static void initPropertyKey() {
        String path = URL_PREFIX + SCHEMA_PKS;

        createAndAssert(path, "{\n"
                              + "\"name\": \"name\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"age\",\n"
                              + "\"data_type\": \"INT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"city\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"lang\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"date\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"price\",\n"
                              + "\"data_type\": \"INT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"weight\",\n"
                              + "\"data_type\": \"DOUBLE\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
        createAndAssert(path, "{\n"
                              + "\"name\": \"rank\",\n"
                              + "\"data_type\": \"TEXT\",\n"
                              + "\"cardinality\": \"SINGLE\",\n"
                              + "\"check_exist\": false,\n"
                              + "\"properties\":[]\n"
                              + "}", 202);
    }

    protected static void initVertexLabel() {
        String path = URL_PREFIX + SCHEMA_VLS;

        createAndAssert(path, "{\n"
                              + "\"primary_keys\":[\"name\"],\n"
                              + "\"id_strategy\": \"PRIMARY_KEY\",\n"
                              + "\"name\": \"person\",\n"
                              + "\"properties\":[\"city\", \"name\", \"age\", \"rank\"],\n"
                              + "\"check_exist\": false,\n"
                              + "\"nullable_keys\":[\"rank\"]\n"
                              + "}");

        createAndAssert(path, "{\n"
                              + "\"primary_keys\":[\"name\"],\n"
                              + "\"id_strategy\": \"PRIMARY_KEY\",\n"
                              + "\"name\": \"software\",\n"
                              + "\"properties\":[\"price\", \"name\", \"lang\", \"rank\"],\n"
                              + "\"check_exist\": false,\n"
                              + "\"nullable_keys\":[\"rank\"]\n"
                              + "}");
    }

    protected static void initEdgeLabel() {
        String path = URL_PREFIX + SCHEMA_ELS;

        createAndAssert(path, "{\n"
                              + "\"name\": \"created\",\n"
                              + "\"source_label\": \"person\",\n"
                              + "\"target_label\": \"software\",\n"
                              + "\"frequency\": \"SINGLE\",\n"
                              + "\"properties\":[\"date\", \"weight\"],\n"
                              + "\"sort_keys\":[],\n"
                              + "\"check_exist\": false,\n"
                              + "\"nullable_keys\":[]\n"
                              + "}");
        createAndAssert(path, "{\n"
                              + "\"name\": \"knows\",\n"
                              + "\"source_label\": \"person\",\n"
                              + "\"target_label\": \"person\",\n"
                              + "\"frequency\": \"MULTIPLE\",\n"
                              + "\"properties\":[\"date\", \"weight\"],\n"
                              + "\"sort_keys\":[\"date\"],\n"
                              + "\"check_exist\": false,\n"
                              + "\"nullable_keys\":[]\n"
                              + "}");
    }

    protected static void initIndexLabel() {
        String path = URL_PREFIX + SCHEMA_ILS;

        Response r = client.post(path, "{\n"
                                       + "\"name\": \"personByCity\",\n"
                                       + "\"base_type\": \"VERTEX_LABEL\",\n"
                                       + "\"base_value\": \"person\",\n"
                                       + "\"index_type\": \"SECONDARY\",\n"
                                       + "\"check_exist\": false,\n"
                                       + "\"fields\": [\n"
                                       + "\"city\"\n"
                                       + "]\n"
                                       + "}");
        assertResponseStatus(202, r);
    }

    protected static void initEdge() {
        String path = URL_PREFIX + GRAPH_EDGE + BATCH;
        Map<String, String> ret = listAllVertexName2Ids();
        String markoId = ret.get("marko");
        String peterId = ret.get("peter");
        String joshId = ret.get("josh");
        String vadasId = ret.get("vadas");
        String rippleId = ret.get("ripple");

        String body = String.format("[{"
                                    + "\"label\": \"knows\","
                                    + "\"outV\": \"%s\","
                                    + "\"inV\": \"%s\","
                                    + "\"outVLabel\": \"person\","
                                    + "\"inVLabel\": \"person\","
                                    + "\"properties\": {"
                                    + " \"date\": \"2021-01-01\","
                                    + " \"weight\":0.5}},{"
                                    + "\"label\": \"knows\","
                                    + "\"outV\": \"%s\","
                                    + "\"inV\": \"%s\","
                                    + "\"outVLabel\": \"person\","
                                    + "\"inVLabel\": \"person\","
                                    + "\"properties\": {"
                                    + " \"date\": \"2021-01-01\","
                                    + " \"weight\":0.4}},{"
                                    + "\"label\": \"knows\","
                                    + "\"outV\": \"%s\","
                                    + "\"inV\": \"%s\","
                                    + "\"outVLabel\": \"person\","
                                    + "\"inVLabel\": \"person\","
                                    + "\"properties\": {"
                                    + " \"date\": \"2021-01-01\","
                                    + " \"weight\":0.3}},{"
                                    + "\"label\": \"created\","
                                    + "\"outV\": \"%s\","
                                    + "\"inV\": \"%s\","
                                    + "\"outVLabel\": \"person\","
                                    + "\"inVLabel\": \"software\","
                                    + "\"properties\": {"
                                    + " \"date\": \"2021-01-01\","
                                    + " \"weight\":0.2}"
                                    + "},{"
                                    + "\"label\": \"created\","
                                    + "\"outV\": \"%s\","
                                    + "\"inV\": \"%s\","
                                    + "\"outVLabel\": \"person\","
                                    + "\"inVLabel\": \"software\","
                                    + "\"properties\": {"
                                    + " \"date\": \"2021-01-01\","
                                    + " \"weight\":0.1}}]",
                                    markoId, peterId, peterId, joshId,
                                    joshId, vadasId, markoId, rippleId,
                                    peterId, rippleId);
        createAndAssert(path, body);
    }

    protected static void initVertex() {
        String path = URL_PREFIX + GRAPH_VERTEX;

        createAndAssert(path, "{\n"
                              + "\"label\": \"person\",\n"
                              + "\"type\": \"vertex\",\n"
                              + "\"properties\":{"
                              + "\"name\": \"marko\","
                              + "\"age\": 29,"
                              + "\"city\": \"Beijing\""
                              + "}\n"
                              + "}");
        createAndAssert(path, "{\n"
                              + "\"label\": \"person\",\n"
                              + "\"type\": \"vertex\",\n"
                              + "\"properties\":{"
                              + "\"name\": \"vadas\","
                              + "\"age\": 27,"
                              + "\"city\": \"HongKong\""
                              + "}\n"
                              + "}");
        createAndAssert(path, "{\n"
                              + "\"label\": \"person\",\n"
                              + "\"type\": \"vertex\",\n"
                              + "\"properties\":{"
                              + "\"name\": \"josh\","
                              + "\"age\": 32,"
                              + "\"city\": \"Beijing\""
                              + "}\n"
                              + "}");
        createAndAssert(path, "{\n"
                              + "\"label\": \"person\",\n"
                              + "\"type\": \"vertex\",\n"
                              + "\"properties\":{"
                              + "\"name\": \"peter\","
                              + "\"age\": 35,"
                              + "\"city\": \"Shanghai\""
                              + "}\n"
                              + "}");
        createAndAssert(path, "{\n"
                              + "\"label\": \"software\",\n"
                              + "\"type\": \"vertex\",\n"
                              + "\"properties\":{"
                              + "\"name\": \"ripple\","
                              + "\"lang\": \"java\","
                              + "\"price\": 199"
                              + "}\n"
                              + "}");
        createAndAssert(path, "{\n"
                              + "\"label\": \"software\",\n"
                              + "\"type\": \"vertex\",\n"
                              + "\"properties\":{"
                              + "\"name\": \"lop\","
                              + "\"lang\": \"java\","
                              + "\"price\": 328"
                              + "}\n"
                              + "}");
    }

    protected static Response createAndAssert(String path, String body) {
        return createAndAssert(path, body, 201);
    }

    protected static Response createAndAssert(String path, String body,
                                              int status) {
        Response r = client.post(path, body);
        assertResponseStatus(status, r);
        return r;
    }

    protected static Map<String, String> listAllVertexName2Ids() {
        Response r = client.get(URL_PREFIX + GRAPH_VERTEX);
        String content = assertResponseStatus(200, r);

        @SuppressWarnings("rawtypes")
        List<Map> vertices = readList(content, "vertices", Map.class);

        Map<String, String> vertextName2Ids = new HashMap<>();
        for (Map vertice : vertices) {
            Map properties = (Map) vertice.get("properties");
            if (properties == null ||
                !properties.containsKey("name") ||
                !vertice.containsKey("id")) {
                continue;
            }
            String name = (String) properties.get("name");
            if (StringUtils.isEmpty(name)) {
                continue;
            }

            String id = (String) vertice.get("id");
            if (StringUtils.isEmpty(id)) {
                continue;
            }

            vertextName2Ids.put(name, id);
        }

        return vertextName2Ids;
    }

    protected static String id2Json(String params) {
        return String.format("\"%s\"", params);
    }

    protected static String getVertexId(String label, String key, String value)
            throws IOException {
        String props = mapper.writeValueAsString(ImmutableMap.of(key, value));
        Map<String, Object> params = ImmutableMap.of(
                "label", label,
                "properties", URLEncoder.encode(props, "UTF-8")
        );
        Response r = client.get(URL_PREFIX + GRAPH_VERTEX, params);
        String content = assertResponseStatus(200, r);

        @SuppressWarnings("rawtypes")
        List<Map> list = readList(content, "vertices", Map.class);
        if (list.size() != 1) {
            throw new HugeException("Failed to get vertex id: %s", content);
        }
        return (String) list.get(0).get("id");
    }

    protected static void clearGraph() {
        Consumer<String> consumer = (urlSuffix) -> {
            String path = URL_PREFIX + urlSuffix;
            String type = urlSuffix.substring(urlSuffix.lastIndexOf('/') + 1);
            Response r = client.get(path);
            if (r.getStatus() != 200) {
                throw new HugeException("Failed to list: " + type + r.readEntity(String.class));
            }
            String content = r.readEntity(String.class);
            @SuppressWarnings("rawtypes")
            List<Map> list = readList(content, type, Map.class);
            List<Object> ids = list.stream().map(e -> e.get("id"))
                                   .collect(Collectors.toList());
            ids.forEach(id -> {
                if (id instanceof Integer) {
                    client.delete(path, String.format("%d", id));
                } else {
                    client.delete(path, (String) id);
                }
            });
        };

        consumer.accept(GRAPH_EDGE);
        consumer.accept(GRAPH_VERTEX);
    }

    protected static void clearSchema() {
        Consumer<String> consumer = (urlSuffix) -> {
            String path = URL_PREFIX + urlSuffix;
            String type = urlSuffix.substring(urlSuffix.lastIndexOf('/') + 1);
            Response r = client.get(path);
            if (r.getStatus() != 200) {
                throw new HugeException("Failed to list " + type);
            }
            String content = r.readEntity(String.class);
            @SuppressWarnings("rawtypes")
            List<Map> list = readList(content, type, Map.class);
            List<Object> names = list.stream().map(e -> e.get("name"))
                                     .collect(Collectors.toList());
            Set<Integer> tasks = new HashSet<>();
            names.forEach(name -> {
                Response response = client.delete(path, (String) name);
                if (urlSuffix.equals(SCHEMA_PKS)) {
                    return;
                }
                String result = assertResponseStatus(202, response);
                tasks.add(assertJsonContains(result, "task_id"));
            });
            for (Integer task : tasks) {
                waitTaskSuccess(task);
            }
        };

        consumer.accept(SCHEMA_ILS);
        consumer.accept(SCHEMA_ELS);
        consumer.accept(SCHEMA_VLS);
        consumer.accept(SCHEMA_PKS);
    }

    protected static void waitTaskSuccess(int task) {
        String status;
        do {
            Response r = client.get("/graphs/hugegraphapi/tasks/",
                                    String.valueOf(task));
            String content = assertResponseStatus(200, r);
            status = assertJsonContains(content, "task_status");
        } while (!"success".equals(status));
    }

    protected static String parseId(String content) throws IOException {
        Map<?, ?> map = mapper.readValue(content, Map.class);
        return (String) map.get("id");
    }

    protected static <T> List<T> readList(String content,
                                          String key,
                                          Class<T> clazz) {
        try {
            JsonNode root = mapper.readTree(content);
            JsonNode element = root.get(key);
            if (element == null) {
                throw new HugeException(String.format(
                        "Can't find value of the key: %s in json.", key));
            }
            JavaType type = mapper.getTypeFactory()
                                  .constructParametricType(List.class, clazz);
            return mapper.readValue(element.toString(), type);
        } catch (IOException e) {
            throw new HugeException(String.format(
                    "Failed to deserialize %s", content), e);
        }
    }

    protected static void initOrClear() {
        Response r = client.get(URL_PREFIX);
        if (r.getStatus() != 200) {
            String body = "{\n" +
                          "  \"backend\": \"hstore\",\n" +
                          "  \"serializer\": \"binary\",\n" +
                          "  \"store\": \"hugegraphapi\",\n" +
                          "  \"search.text_analyzer\": \"jieba\",\n" +
                          "  \"search.text_analyzer_mode\": \"INDEX\"\n" +
                          "}";
            r = client.post(URL_PREFIX, body);
            if (r.getStatus() != 201) {
                throw new HugeException("Failed to create graph: " + GRAPH +
                                        r.readEntity(String.class));
            }
        } else {
            BaseApiTest.clearData();
        }
    }

    protected static void clearData() {
        clearGraph();
        clearSchema();
    }

    protected static String assertResponseStatus(int status,
                                                 Response response) {
        String content = response.readEntity(String.class);
        String message = String.format("Response with status %s and content %s",
                                       response.getStatus(), content);
        Assert.assertEquals(message, status, response.getStatus());
        return content;
    }

    protected static String assertErrorContains(Response response,
                                                String message) {
        Assert.assertNotEquals("Fail to assert request failed", 200,
                               response.getStatus());
        String content = response.readEntity(String.class);
        Map<String, String> resultMap = JsonUtil.fromJson(content, Map.class);
        Assert.assertTrue(resultMap.get("message").contains(message));
        return content;
    }

    public static <T> T assertJsonContains(String response, String key) {
        Map<?, ?> json = JsonUtil.fromJson(response, Map.class);
        return assertMapContains(json, key);
    }

    @SuppressWarnings("unchecked")
    public static <T> T assertMapContains(Map<?, ?> map, String key) {
        String message = String.format("Expect contains key '%s' in %s",
                                       key, map);
        Assert.assertTrue(message, map.containsKey(key));
        return (T) map.get(key);
    }

    public static Map<?, ?> assertArrayContains(List<Map<?, ?>> list,
                                                String key, Object value) {
        String message = String.format("Expect contains {'%s':'%s'} in list %s",
                                       key, value, list);
        Map<?, ?> found = null;
        for (Map<?, ?> map : list) {
            if (map.get(key).equals(value)) {
                found = map;
                break;
            }
        }
        Assert.assertTrue(message, found != null);
        return found;
    }

    public static void clearUsers() {
        String path = "auth/users";
        Response r = client.get(path,
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
            client.delete(path, (String) user.get("id"));
        }
    }

    public static void createSpace(String name, boolean auth) {
        String body = "{\n" +
                      "  \"name\": \"%s\",\n" +
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
                      "  \"auth\": %s,\n" +
                      "  \"configs\": {}\n" +
                      "}";
        Response r = client.post("graphspaces", String.format(body, name,
                                                              auth));
        assertResponseStatus(201, r);
    }

    public static void clearSpaces() {
        Response r = client.get("graphspaces");
        String result = r.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> spaces = (List<String>) resultMap.get("graphSpaces");
        for (String space : spaces) {
            if (!"DEFAULT".equals(space)) {
                client.delete("graphspaces", space);
            }
        }
    }

    public static Response createGraph(String graphSpace, String name) {
        return createGraph(graphSpace, name, name);
    }

    public static Response createGraph(String graphSpace, String name,
                                       String nickname) {
        String config = "{\n" +
                        "  \"backend\": \"hstore\",\n" +
                        "  \"serializer\": \"binary\",\n" +
                        "  \"store\": \"%s\",\n" +
                        "  \"nickname\": \"%s\",\n" +
                        "  \"search.text_analyzer\": \"jieba\",\n" +
                        "  \"search.text_analyzer_mode\": \"INDEX\"\n" +
                        "}";
        String path = String.format("graphspaces/%s/graphs/%s", graphSpace,
                                    name);
        return client.post(path, String.format(config, name, nickname));
    }

    public static Response updateGraph(String action, String graphSpace,
                                       String name, String nickname) {
        String body = "{\n" +
                      "  \"action\": \"%s\",\n" +
                      "  \"update\": {\n" +
                      "    \"name\":\"%s\",\n" +
                      "    \"nickname\": \"%s\"\n" +
                      "  }\n" +
                      "}";
        String path = String.format("graphspaces/%s/graphs", graphSpace);
        return client.put(path, name,
                          String.format(body, action, name, nickname),
                          ImmutableMap.of());
    }

    public static RestClient userClient(String username) {
        String user1 = "{\"user_name\":\"%s\"," +
                       "\"user_password\":\"%s\"}";
        Response r = client.post("auth/users",
                                 String.format(user1, username, username));
        assertResponseStatus(201, r);

        RestClient client = new RestClient(BASE_URL, username, username);
        return client;
    }

    public static RestClient spaceManagerClient(String graphSpace,
                                                String username) {
        RestClient spaceClient = userClient(username);

        String spaceBody = "{\n" +
                           "  \"user\": \"%s\",\n" +
                           "  \"type\": \"SPACE\",\n" +
                           "  \"graphspace\": \"%s\"\n" +
                           "}";
        client.post("auth/managers", String.format(spaceBody, username,
                                                   graphSpace));
        return spaceClient;
    }

    public static RestClient analystClient(String graphSpace, String username) {
        RestClient analystClient = userClient(username);

        String body = "{\n" +
                      "  \"user\": \"%s\",\n" +
                      "  \"role\": \"analyst\",\n" +
                      "}";
        String path = String.format("graphspaces/%s/role", graphSpace);
        client.post(path, String.format(body, username));
        return analystClient;
    }

    public static RestClient observerClient(String graphSpace, String graph,
                                            String username) {
        RestClient observerClient = userClient(username);

        String body = "{\n" +
                      "  \"user\": \"%s\",\n" +
                      "  \"role\": \"observer\",\n" +
                      "  \"graph\": \"%s\"\n" +
                      "}";
        client.post("auth/managers", String.format(body, username,
                                                   graph));
        return observerClient;
    }

    @After
    public void teardown() throws Exception {
        BaseApiTest.clearData();
    }

    public RestClient client() {
        return client;
    }

    public static class RestClient {

        private Client client;
        private WebTarget target;

        public RestClient(String url) {
            this.client = ClientBuilder.newClient();
            this.client.register(EncodingFilter.class);
            this.client.register(GZipEncoder.class);
            this.client.register(HttpAuthenticationFeature.basic(USERNAME,
                                                                 PASSWORD));
            this.target = this.client.target(url);
        }

        public RestClient(String url, String username, String password) {
            this.client = ClientBuilder.newClient();
            this.client.register(EncodingFilter.class);
            this.client.register(GZipEncoder.class);
            this.client.register(HttpAuthenticationFeature.basic(username,
                                                                 password));
            this.target = this.client.target(url);
        }

        public void close() {
            this.client.close();
        }

        public WebTarget target() {
            return this.target;
        }

        public WebTarget target(String url) {
            return this.client.target(url);
        }

        public Response get(String path) {
            return this.target.path(path).request().get();
        }

        public Response get(String path, String id) {
            return this.target.path(path).path(id).request().get();
        }

        public Response get(String path,
                            MultivaluedMap<String, Object> headers) {
            return this.target.path(path).request().headers(headers).get();
        }

        public Response get(String path, Multimap<String, Object> params) {
            WebTarget target = this.target.path(path);
            for (Map.Entry<String, Object> entries : params.entries()) {
                target = target.queryParam(entries.getKey(), entries.getValue());
            }
            return target.request().get();
        }

        public Response get(String path, Map<String, Object> params) {
            WebTarget target = this.target.path(path);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                target = target.queryParam(i.getKey(), i.getValue());
            }
            return target.request().get();
        }

        public Response post(String path, String content) {
            return this.post(path, Entity.json(content));
        }

        public Response post(String path, Entity<?> entity) {
            return this.target.path(path).request().post(entity);
        }

        public Response put(String path, String id, String content,
                            Map<String, Object> params) {
            WebTarget target = this.target.path(path).path(id);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                target = target.queryParam(i.getKey(), i.getValue());
            }
            return target.request().put(Entity.json(content));
        }

        public Response delete(String path, String id) {
            return this.target.path(path).path(id).request().delete();
        }

        public Response delete(String path, Map<String, Object> params) {
            WebTarget target = this.target.path(path);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                target = target.queryParam(i.getKey(), i.getValue());
            }
            return target.request().delete();
        }

        public Response delete(String path,
                               MultivaluedMap<String, Object> headers) {
            WebTarget target = this.target.path(path);
            return target.request().headers(headers).delete();
        }

        public void registerBasic(String username, String password) {
            this.client.register(HttpAuthenticationFeature.basic(username,
                                                                 password));
        }
    }
}
