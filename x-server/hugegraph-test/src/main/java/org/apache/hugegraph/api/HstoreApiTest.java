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

import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class HstoreApiTest extends BaseApiTest {

    private static final String PATH = "hstore";

    @Test
    public void testGetGrpcAddress() {
        Response r = this.client().get(PATH);
        Map<String, Object> content = r.readEntity(Map.class);
        List<Long> nodes = (List<Long>) assertMapContains(content, "nodes");
        String node1 = nodes.get(0).toString();

        Response r2 = this.client().get(PATH + "/" + node1);
        Map<String, Object> content2 = r2.readEntity(Map.class);
        String address = (String) assertMapContains(content2, "address");
        Assert.assertContains(":", address);
    }
}
