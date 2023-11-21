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

package org.apache.hugegraph.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.StandardAuthenticator;
import org.apache.hugegraph.backend.store.BackendStoreSystemInfo;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

public class InitStore {

    private static final Logger LOG = Log.logger(InitStore.class);

    // 6~8 retries may be needed under high load for Cassandra backend
    private static final int RETRIES = 10;
    // Less than 5000 may cause mismatch exception with Cassandra backend
    private static final long RETRY_INTERVAL = 5000;

    private static final MultiValueMap exceptions = new MultiValueMap();

    static {
        exceptions.put("OperationTimedOutException",
                       "Timed out waiting for server response");
        exceptions.put("NoHostAvailableException",
                       "All host(s) tried for query failed");
        exceptions.put("InvalidQueryException", "does not exist unconfirmed table");
    }

    public static void main(String[] args) throws Exception {
        E.checkArgument(args.length == 12,
                        "HugeGraph init-store need to pass the config file " +
                        "of RestServer, like: conf/rest-server.properties");
        E.checkArgument(args[0].endsWith(".properties"),
                        "Expect the parameter is properties config file.");

        String restConf = args[0];
        HugeConfig restServerConfig = new HugeConfig(restConf);

        boolean loadFromLocalConfig;
        try {
            loadFromLocalConfig = restServerConfig.getBoolean(
                    ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name());
        } catch (NoSuchElementException e) {
            loadFromLocalConfig = false;
        }
        if (!loadFromLocalConfig) {
            LOG.info("Config '{}' is false, init-store do nothing and exit",
                     ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name());
            return;
        }

        int num;
        try {
            num = restServerConfig.getInt(ServerOptions.TASK_THREADS.name());
        } catch (NoSuchElementException e) {
            num = 4;
        }
        TaskManager.instance(num);

        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
        RegisterUtil.registerServer();

        String graphsDir = restServerConfig.get(ServerOptions.GRAPHS);
        Map<String, String> graphConfs = ConfigUtil.scanGraphsDir(graphsDir);
        List<String> sortedGraphNames = new ArrayList<>(graphConfs.keySet());
        sortedGraphNames.sort((t0, t1) -> {
            HugeConfig config = new HugeConfig(graphConfs.get(t0));
            String clazz = config.getString("gremlin.graph", null);
            if (clazz != null &&
                "org.apache.hugegraph.HugeFactory".equals(clazz.trim())) {
                return 1;
            }
            return -1;
        });

        List<String> metaEndpoints = Arrays.asList(args[5].split(","));
        Boolean withCa = "true".equals(args[8]);
        StandardAuthenticator.initAdminUserIfNeeded(restConf, metaEndpoints,
                                                    args[6], withCa, args[9],
                                                    args[10], args[11]);

        for (String graphName : sortedGraphNames) {
            initGraph(graphConfs.get(graphName));
        }

        HugeFactory.shutdown(30L);
    }

    private static void initGraph(String configPath) throws Exception {
        LOG.info("Init graph with config file: {}", configPath);
        HugeConfig config = new HugeConfig(configPath);
        HugeGraph graph = (HugeGraph) GraphFactory.open(config);

        try (graph) {
            BackendStoreSystemInfo sysInfo = graph.backendStoreSystemInfo();
            if (sysInfo.exists()) {
                LOG.info("Skip init-store due to the backend store of '{}' " +
                         "had been initialized", graph.name());
                sysInfo.checkVersion();
            } else {
                initBackend(graph);
            }
        }
    }

    private static void initBackend(final HugeGraph graph)
            throws InterruptedException {
        int retries = RETRIES;
        retry:
        do {
            try {
                graph.initBackend();
            } catch (Exception e) {
                String clz = e.getClass().getSimpleName();
                String message = e.getMessage();
                if (exceptions.containsKey(clz) && retries > 0) {
                    @SuppressWarnings("unchecked")
                    Collection<String> keywords = exceptions.getCollection(clz);
                    for (String keyword : keywords) {
                        if (message.contains(keyword)) {
                            LOG.info("Init failed with exception '{} : {}', " +
                                     "retry  {}...",
                                     clz, message, RETRIES - retries + 1);

                            Thread.sleep(RETRY_INTERVAL);
                            continue retry;
                        }
                    }
                }
                throw e;
            }
            break;
        } while (retries-- > 0);
    }
}
