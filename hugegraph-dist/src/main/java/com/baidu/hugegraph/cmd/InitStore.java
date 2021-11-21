/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.cmd;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.StandardAuthenticator;
import com.baidu.hugegraph.backend.store.BackendStoreSystemInfo;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.dist.RegisterUtil;
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

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
        exceptions.put("InvalidQueryException", "does not exist");
        exceptions.put("InvalidQueryException", "unconfigured table");
    }

    public static void main(String[] args) throws Exception {
        E.checkArgument(args.length == 1,
                        "HugeGraph init-store need to pass the config file " +
                        "of RestServer, like: conf/rest-server.properties");
        E.checkArgument(args[0].endsWith(".properties"),
                        "Expect the parameter is properties config file.");

        String restConf = args[0];

        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
        RegisterUtil.registerServer();

//        Parameters params = new Parameters();
//        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
//                new FileBasedConfigurationBuilder(YAMLConfiguration.class).
//                        configure(params.fileBased().setFileName(gremlinConfFile));
//        YAMLConfiguration config = (YAMLConfiguration) builder.getConfiguration();
//
//        List<HierarchicalConfiguration<ImmutableNode>> nodes =
//                config.childConfigurationsAt(GRAPHS);
//        E.checkArgument(nodes.size() == 1,
//                "Must contain one '%s' node in config file '%s'",
//                GRAPHS, gremlinConfFile);
//
//        HierarchicalConfiguration<ImmutableNode> node = nodes.get(0);
//        E.checkArgument(!node.isEmpty(),
//                "Must contain at least one graph");
//
//        for (Iterator<String> it = node.getKeys(); it.hasNext(); ) {
//            String graphName = it.next();
//            HugeFactory.checkGraphName(graphName, "gremlin-server.yaml");
//            String configPath = node.getProperty(graphName).toString();
//            initGraph(configPath);
//        }

        HugeConfig restServerConfig = new HugeConfig(restConf);
        String graphsDir = restServerConfig.get(ServerOptions.GRAPHS);
        Map<String, String> graphs = ConfigUtil.scanGraphsDir(graphsDir);

        for (Map.Entry<String, String> entry : graphs.entrySet()) {
            initGraph(entry.getValue());
        }

        StandardAuthenticator.initAdminUserIfNeeded(restConf);

        HugeFactory.shutdown(30L);
    }

    private static void initGraph(String configPath) throws Exception {
        LOG.info("Init graph with config file: {}", configPath);
        HugeConfig config = new HugeConfig(configPath);
        // Forced set RAFT_MODE to false when initializing backend
        config.setProperty(CoreOptions.RAFT_MODE.name(), "false");
        HugeGraph graph = (HugeGraph) GraphFactory.open(config);

        BackendStoreSystemInfo sysInfo = graph.backendStoreSystemInfo();
        try {
            if (sysInfo.exists()) {
                LOG.info("Skip init-store due to the backend store of '{}' " +
                         "had been initialized", graph.name());
                sysInfo.checkVersion();
            } else {
                initBackend(graph);
            }
        } finally {
            graph.close();
        }
    }

    private static void initBackend(final HugeGraph graph)
                                    throws InterruptedException {
        int retries = RETRIES;
        retry: do {
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
