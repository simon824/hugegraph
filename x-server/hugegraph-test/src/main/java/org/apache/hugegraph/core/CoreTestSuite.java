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

package org.apache.hugegraph.core;

import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.StandardAuthManager;
import org.apache.hugegraph.core.PropertyCoreTest.EdgePropertyCoreTest;
import org.apache.hugegraph.core.PropertyCoreTest.VertexPropertyCoreTest;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.util.Log;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        PropertyKeyCoreTest.class,
        VertexLabelCoreTest.class,
        EdgeLabelCoreTest.class,
        IndexLabelCoreTest.class,
        VertexCoreTest.class,
        EdgeCoreTest.class,
        ParentAndSubEdgeCoreTest.class,
        TraverserCoreTest.class,
        VertexPropertyCoreTest.class,
        EdgePropertyCoreTest.class,
        RestoreCoreTest.class,
        TaskCoreTest.class,
        AuthTest.class,
        MultiGraphsTest.class,
})
public class CoreTestSuite {

    private static final Logger LOG = Log.logger(BaseCoreTest.class);
    private static HugeGraph graph = null;

    private static MetaManager metaManager = MetaManager.instance();
    private static StandardAuthManager authManager = null;

    @BeforeClass
    public static void initEnv() {
        RegisterUtil.registerServer();
        RegisterUtil.registerBackends();
    }

    @BeforeClass
    public static void init() {
        List<String> endpoints = ImmutableList.of("127.0.0.1:8686");
        metaManager.connect("hg", MetaManager.MetaDriverType.PD,
                            null, null, null, endpoints);
        authManager = new StandardAuthManager(metaManager,
                                              "FXQXbJtbCLxODc6tGci732pkH1cyf8Qg");
        authManager.initAdmin();

        TaskManager.instance(4);
        graph = Utils.open();
        try {
            /*
             * clearBackend操作会等待task执行完成
             * 在执行任务查询过程中，如果图未初始化则会抛出异常
             */
            graph.clearBackend();
        } catch (Throwable t) {
            LOG.warn("Error when clearBackend", t);
        }
        graph.initBackend();
        graph.serverStarted();
    }

    @AfterClass
    public static void clear() {
        if (graph == null) {
            return;
        }

        try {
            graph.clearBackend();
        } finally {
            try {
                graph.close();
            } catch (Throwable e) {
                LOG.error("Error when close()", e);
            }
            graph = null;
        }
    }

    protected static HugeGraph graph() {
        Assert.assertNotNull(graph);
        // Assert.assertFalse(graph.closed());
        return graph;
    }

    protected static AuthManager authManager() {
        Assert.assertNotNull(authManager);
        return authManager;
    }
}
