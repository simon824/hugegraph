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

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class PdMetricsApiTest extends BaseApiTest {
    private static String path1 = "/metrics/graphspaces/DEFAULT";
    private static String path2 = "/metrics/graphspaces/DEFAULT/graphs" +
                                  "/hugegraphapi";

    @Test
    public void testListGraphSpaceMetrics() {
        Response r =
                this.client().get(path1, ImmutableMap.of("recentdata", "10"));
        String content = assertResponseStatus(200, r);

        r = this.client().get(path1, ImmutableMap.of("recentdata", "60"));
        content = assertResponseStatus(200, r);
    }

    @Test
    public void testListGraphMetrics() {
        Response r =
                this.client().get(path2, ImmutableMap.of("recentdata", "10"));
        String content = assertResponseStatus(200, r);

        r = this.client().get(path2, ImmutableMap.of("recentdata", "60"));
        content = assertResponseStatus(200, r);
    }


}
