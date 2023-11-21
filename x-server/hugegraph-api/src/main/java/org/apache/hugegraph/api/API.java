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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.core.MediaType;

public class API {

    public static final String CHARSET = "UTF-8";
    public static final String APPLICATION_JSON = MediaType.APPLICATION_JSON;
    public static final String APPLICATION_JSON_WITH_CHARSET =
            APPLICATION_JSON + ";charset=" + CHARSET;
    public static final String APPLICATION_TEXT_WITH_CHARSET =
            MediaType.TEXT_PLAIN + ";charset=" + CHARSET;
    public static final String JSON = MediaType.APPLICATION_JSON_TYPE.getSubtype();
    public static final String ACTION_APPEND = "append";
    public static final String ACTION_ELIMINATE = "eliminate";
    public static final String ACTION_UPDATE = "update";
    public static final String ACTION_CLEAR = "clear";
    public static final String USER_NAME_PATTERN = "^[0-9a-zA-Z_]{4,16}$";
    public static final String USER_PASSWORD_PATTERN = "[a-zA-Z0-9~!@#$%^&*()" +
                                                       "_+|<>,.?/:;" +
                                                       "'`\"\\[\\]{}\\\\]{5," +
                                                       "16}";
    public static final String USER_NICKNAME_PATTERN = "^(?!_)(?!.*?_$)" +
                                                       "[a-zA-Z0-9\u4e00-\u9fa5~!@#$" +
                                                       "%^&*()_+|<>,.?/:;" +
                                                       "'`\"\\[\\]{}\\\\]{1,16}$";
    private static final Logger LOGGER = Log.logger(RestServer.class);
    private static final Meter succeedMeter =
            MetricsUtil.registerMeter(API.class, "commit-succeed");
    private static final Meter illegalArgErrorMeter =
            MetricsUtil.registerMeter(API.class, "illegal-arg");
    private static final Meter expectedErrorMeter =
            MetricsUtil.registerMeter(API.class, "expected-error");
    private static final Meter unknownErrorMeter =
            MetricsUtil.registerMeter(API.class, "unknown-error");

    public static HugeGraph graph(GraphManager manager, String graphSpace,
                                  String graph) {
        HugeGraph g = manager.graph(graphSpace, graph);
        if (g == null) {
            throw new NotFoundException(String.format(
                    "Graph '%s' does not exist", graph));
        }
        return g;
    }

    public static GraphSpace space(GraphManager manager, String space) {
        GraphSpace s = manager.graphSpace(space);
        if (s == null) {
            throw new NotFoundException(String.format(
                    "Graph space '%s' does not exist", space));
        }
        return s;
    }

    public static Service service(GraphManager manager, String graphSpace,
                                  String service) {
        Service s = manager.service(graphSpace, service);
        if (s == null) {
            throw new NotFoundException(String.format(
                    "Service '%s' does not exist", service));
        }
        return s;
    }

    public static SchemaTemplate schemaTemplate(GraphManager manager,
                                                String graphSpace,
                                                String schemaTemplate) {
        SchemaTemplate st = manager.schemaTemplate(graphSpace, schemaTemplate);
        if (st == null) {
            throw new NotFoundException(String.format(
                    "Schema template '%s' does not exist", schemaTemplate));
        }
        return st;
    }

    public static HugeGraph graph4admin(GraphManager manager, String graphSpace,
                                        String graph) {
        return graph(manager, graphSpace, graph).hugegraph();
    }

    public static <R> R commit(HugeGraph g, Callable<R> callable) {
        Consumer<Throwable> rollback = (error) -> {
            if (error != null) {
                LOGGER.error("[SERVER] Commit failed", error);
            }
            try {
                g.tx().rollback();
            } catch (Throwable e) {
                LOGGER.error("[SERVER] Rollback failed", e);
            }
        };

        try {
            R result = callable.call();
            g.tx().commit();
            succeedMeter.mark();
            return result;
        } catch (IllegalArgumentException | NotFoundException |
                 ForbiddenException e) {
            illegalArgErrorMeter.mark();
            rollback.accept(null);
            throw e;
        } catch (RuntimeException e) {
            expectedErrorMeter.mark();
            rollback.accept(e);
            throw e;
        } catch (Throwable e) {
            unknownErrorMeter.mark();
            rollback.accept(e);
            // TODO: throw the origin exception 'e'
            throw new HugeException("Failed to commit", e);
        }
    }

    public static void commit(HugeGraph g, Runnable runnable) {
        commit(g, () -> {
            runnable.run();
            return null;
        });
    }

    public static Object[] properties(Map<String, Object> properties) {
        Object[] list = new Object[properties.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            list[i++] = prop.getKey();
            list[i++] = prop.getValue();
        }
        return list;
    }

    protected static void checkCreatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkCreate(false);
    }

    protected static void checkUpdatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkUpdate();
    }

    protected static void checkCreatingBody(
            Collection<? extends Checkable> bodys) {
        E.checkArgumentNotNull(bodys, "The request body can't be empty");
        for (Checkable body : bodys) {
            E.checkArgument(body != null,
                            "The batch body can't contain null record");
            body.checkCreate(true);
        }
    }

    protected static void checkUpdatingBody(
            Collection<? extends Checkable> bodys) {
        E.checkArgumentNotNull(bodys, "The request body can't be empty");
        for (Checkable body : bodys) {
            E.checkArgumentNotNull(body,
                                   "The batch body can't contain null record");
            body.checkUpdate();
        }
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> parseProperties(String properties) {
        if (properties == null || properties.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<String, Object> props = null;
        try {
            props = JsonUtil.fromJson(properties, Map.class);
        } catch (Exception ignored) {
        }

        // If properties is the string "null", props will be null
        E.checkArgument(props != null,
                        "Invalid request with properties: %s", properties);
        return props;
    }

    public static boolean checkAndParseAction(String action) {
        E.checkArgumentNotNull(action, "The action param can't be empty");
        if (action.equals(ACTION_APPEND)) {
            return true;
        } else if (action.equals(ACTION_ELIMINATE)) {
            return false;
        } else {
            throw new NotSupportedException(
                    String.format("Not support action '%s'", action));
        }
    }

    public static class DebugMeasure {
        public static final String EDGE_ITER = "edge_iters";
        public static final String VERTICE_ITER = "vertice_iters";
        public static final String COST = "cost";

        protected long timeStart = System.currentTimeMillis();
        protected HashMap<String, Object> mapResult = new LinkedHashMap<>();

        public Map<String, Object> getResult() {
            mapResult.put(COST, System.currentTimeMillis() - timeStart);
            return mapResult;
        }

        public void put(String key, String value) {
            this.mapResult.put(key, value);
        }

        public void put(String key, long value) {
            this.mapResult.put(key, value);
        }

        public void put(String key, int value) {
            this.mapResult.put(key, value);
        }

        protected void addCount(String key, long value) {
            long cur = 0;
            if (this.mapResult.containsKey(key)) {
                cur = (long) this.mapResult.get(key);
            }
            this.mapResult.put(key, cur + value);
        }

        public void addIterCount(long verticeIters, long edgeIters) {
            this.addCount(EDGE_ITER, edgeIters);
            this.addCount(VERTICE_ITER, verticeIters);
        }
    }
}