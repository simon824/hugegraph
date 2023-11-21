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

public class VermeerApiTest extends BaseApiTest {

    private static String path = "/vermeer";

    @Test
    public void testLoad() {
        String json = "{\n" +
                      "  \"task_type\":\"load\",\n" +
                      "  \"graph\":\"DEFAULT-hugegraphapi\",\n" +
                      "  \"params\": {\n" +
                      "        \"load.parallel\": \"50\",\n" +
                      "        \"load.type\": \"hugegraph\",\n" +
                      "        \"load.use_outedge\":  \"1\",\n" +
                      "        \"load.use_out_degree\":  \"1\",\n" +
                      "        \"load.hg_pd_peers\": \"[\\\"127.0.0.1:" +
                      "8686\\\"]\",\n" +
                      "        \"load.hugegraph_name\":  " +
                      "\"DEFAULT/hugegraphapi/g\",\n" +
                      "        \"load.hugegraph_username\":\"admin\",\n" +
                      "        \"load.hugegraph_password\":\"admin\"\n" +
                      "    }\n" +
                      "}";
        client().post(path, json);
    }

    @Test
    public void testCompute() {
        String json = "{\n" +
                      "  \"task_type\":\"compute\",\n" +
                      "  \"graph\":\"DEFAULT-hugegraphapi\",\n" +
                      "  \"params\":{\n" +
                      "  \"compute.algorithm\": \"pagerank\",\n" +
                      "  \"compute.parallel\": \"1\",\n" +
                      "  \"compute.max_step\": \"10\",\n" +
                      "  \"pagerank.damping\": \"0.85\",\n" +
                      "  \"pagerank.diff_threshold\": \"0.00001\",\n" +
                      "  \"output.parallel\":\"10\",\n" +
                      "  \"output.type\":\"hugegraph\",\n" +
                      "  \"output.hg_pd_peers\":\"[\\\"127.0.0.1:" +
                      "8686\\\"]\",\n" +
                      "  \"output.hugegraph_username\":\"admin\", \n" +
                      "  \"output.hugegraph_password\":\"admin\",\n" +
                      "  \"output.hugegraph_name\":\"DEFAULT/hugegraphapi/g" +
                      "\",\n" +
                      "  \"output.hugegraph_property\": \"pagerank\"\n" +
                      "  }\n" +
                      "}";
        client().post(path, json);
    }
}
