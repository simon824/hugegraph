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

import java.io.File;
import java.util.List;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.traversers.TraversersApiTestSuite;
import org.apache.hugegraph.dist.HugeGraphServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.ImmutableList;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        PropertyKeyApiTest.class,
        VertexLabelApiTest.class,
        EdgeLabelApiTest.class,
        IndexLabelApiTest.class,
        SchemaApiTest.class,
        VertexApiTest.class,
        EdgeApiTest.class,
        TaskApiTest.class,
        GremlinApiTest.class,
        MetricsApiTest.class,
        PdMetricsApiTest.class,
        UserApiTest.class,
        AuthApiTest.class,
        ManagerApiTest.class,
        LoginApiTest.class,
        TraversersApiTestSuite.class,
        CypherApiTest.class,
        VermeerApiTest.class,
        GraphSpaceApiTest.class,
        GraphsApiTest.class,
        SchemaTemplateApiTest.class,
        SubgraphsApiTest.class,
        HstoreApiTest.class,
        CypherApi2Test.class
})
public class ApiTestSuite {
    private static HugeGraphServer hugeGraphServer = null;

    @BeforeClass
    public static void initEnv() {
        HugeGraphServer.register();
        String serverDir = "./";
        String gremlinServerConf = serverDir + "conf/gremlin-server.yaml";
        gremlinServerConf = new File(gremlinServerConf).getAbsolutePath();
        String restServerConf = serverDir + "conf/rest-server.properties";
        restServerConf = new File(restServerConf).getAbsolutePath();
        String graphSpace = "DEFAULT";
        String serviceId = "DEFAULT";
        List<String> metaEndpoints = ImmutableList.of("127.0.0.1:8686");

        String cluster = "hg";
        String pdPeers = "127.0.0.1:8686";
        boolean withCa = false;
        String caFile = serverDir + "/conf/ca.perm";
        String clientCaFile = serverDir + "/conf/client_ca.perm";
        String clientKeyFile = serverDir + "/conf/client.key";
        try {
            hugeGraphServer = new HugeGraphServer(gremlinServerConf,
                                                  restServerConf, graphSpace,
                                                  serviceId, metaEndpoints,
                                                  cluster, pdPeers, withCa,
                                                  caFile, clientCaFile, clientKeyFile);
        } catch (Exception e) {
            throw new HugeException("Fail to start server.", e);
        }
    }

    @AfterClass
    public static void clear() throws Exception {
        if (hugeGraphServer != null) {
            hugeGraphServer.stop();
        }
    }
}
