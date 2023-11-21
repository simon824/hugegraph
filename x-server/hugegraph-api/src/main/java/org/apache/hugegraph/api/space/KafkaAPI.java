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

package org.apache.hugegraph.api.space;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.kafka.ClientFactory;
import org.apache.hugegraph.kafka.consumer.StandardConsumer;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.kafka.common.TopicPartition;

import com.codahale.metrics.annotation.Timed;
import com.google.gson.Gson;

import groovy.lang.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("kafka")
@Singleton
public class KafkaAPI extends API {

    @GET
    @Timed
    @Path("topics")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String getKafkaTopicInfo(@Context GraphManager manager) {

        if (!BrokerConfig.getInstance().isKafkaEnabled()) {
            return "{}";
        }

        StandardConsumer client = ClientFactory.getInstance().getStandardConsumer();

        Map<TopicPartition, Long> map = client.getStackMap();

        if (null == map) {
            return "{}";
        }

        Map<String, Map<Integer, Long>> stackMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : map.entrySet()) {
            String topic = entry.getKey().topic();
            Map<Integer, Long> sub =
                    stackMap.computeIfAbsent(topic, v -> new HashMap<>());
            Integer partition = entry.getKey().partition();
            sub.put(partition, entry.getValue());
        }

        Gson gson = new Gson();
        return gson.toJson(stackMap);
    }

    @GET
    @Timed
    @Path("graphspace/filter")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public List<String> getFilteredGraphspaces(@Context GraphManager manager) {
        MetaManager meta = MetaManager.instance();
        return meta.getKafkaFilteredGraphspace();
    }

    @POST
    @Timed
    @Path("graphspace/filter")
    @Consumes(APPLICATION_JSON_WITH_CHARSET)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public List<String> updateFilteredGraphspaces(@Context GraphManager manager,
                                                  List<String> graphSpaces) {
        MetaManager meta = MetaManager.instance();
        meta.updateKafkaFilteredGraphspace(graphSpaces);
        return graphSpaces;
    }

    @DELETE
    @Timed
    @Path("graphspace/filter/{graphspace}")
    @RolesAllowed({"admin"})
    public void removeFilteredGraphspaces(@Context GraphManager manager,
                                          @PathParam("graphspace") String graphSpace) {
        MetaManager meta = MetaManager.instance();
        List<String> graphspaces = meta.getKafkaFilteredGraphspace();
        Set<String> set = new HashSet<>(graphspaces);
        set.remove(graphSpace);
        List<String> next = new ArrayList<>(set);
        meta.updateKafkaFilteredGraphspace(next);
    }

    @GET
    @Timed
    @Path("graph/filter")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public List<String> getFilteredGraphs(@Context GraphManager manager) {
        MetaManager meta = MetaManager.instance();
        return meta.getKafkaFilteredGraph();
    }

    @POST
    @Timed
    @Path("graph/filter")
    @Consumes(APPLICATION_JSON_WITH_CHARSET)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public List<String> updateFilteredGraphs(@Context GraphManager manager,
                                             List<String> graphs) {

        MetaManager meta = MetaManager.instance();
        meta.updateKafkaFilteredGraph(graphs);
        return graphs;
    }

    @DELETE
    @Timed
    @Path("graph/filter/{graphname}")
    @RolesAllowed({"admin"})
    public void removeFilteredGraph(@Context GraphManager manager,
                                    @PathParam("graphname") String graphName) {
        MetaManager meta = MetaManager.instance();
        List<String> graphs = meta.getKafkaFilteredGraph();
        Set<String> set = new HashSet<>(graphs);
        set.remove(graphName);
        List<String> next = new ArrayList<>(set);
        meta.updateKafkaFilteredGraph(next);
    }
}
