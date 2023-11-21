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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.store.hstore.HstoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("hstore")
@Singleton
public class HStoreAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);
    private PDClient client;

    protected synchronized PDClient client(HugeConfig config) {
        if (this.client != null) {
            return this.client;
        }

        String pdPeers = config.get(HstoreOptions.PD_PEERS);

        E.checkArgument(StringUtils.isNotEmpty(pdPeers),
                        "Please set pd addrs use config: pd.peers");

        this.client = PDClient.create(PDConfig.of(pdPeers)
                                              .setEnablePDNotify(true));

        E.checkArgument(client != null,
                        "Get pd client error, The hstore api is not enable.");

        return this.client;
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context HugeConfig config,
                       @QueryParam("offlineExcluded")
                       @DefaultValue("false") boolean offlineExcluded) {

        LOG.debug("List all hstore node");

        List<Long> nodes = new ArrayList<>();

        List<Metapb.Store> stores;
        try {
            stores = client(config).getStoreStatus(offlineExcluded);
        } catch (PDException e) {
            throw new HugeException("Get hstore nodes error", e);
        }

        for (Metapb.Store store : stores) {
            // 节点id
            long id = store.getId();
            nodes.add(id);
        }

        return ImmutableMap.of("nodes", nodes);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context HugeConfig config,
                      @Context GraphManager manager,
                      @PathParam("id") long id) {

        Metapb.Store store;
        try {
            store = client(config).getStore(id);
        } catch (PDException e) {
            throw new HugeException("Get hstore node by id error", e);
        }

        E.checkArgument(store != null, "Get store by (%d) not exist", id);

        Metapb.StoreStats stats = store.getStats();
        // 总空间大小
        long capacity = stats.getCapacity();
        // 使用大小
        long used = stats.getUsedSize();
        // 状态
        Metapb.StoreState state = store.getState();
        // grpc ip:port
        String address = store.getAddress();
        // 分片数量

        List<Metapb.Partition> partitions = null;

        try {
            partitions = client(config).getPartitionsByStore(id);
        } catch (PDException e) {
            throw new HugeException("Get partitions by node id error", e);
        }

        List<Map> partitionInfos = new ArrayList<>();
        for (Metapb.Partition partition : partitions) {
            int pid = partition.getId();
            String graphName = partition.getGraphName();
            partitionInfos.add(ImmutableMap.of("id", pid,
                                               "graph_name", graphName));
        }

        HashMap<String, Object> storeInfo = new HashMap<>();
        storeInfo.put("id", id);
        storeInfo.put("capacity", capacity);
        storeInfo.put("used", used);
        storeInfo.put("state", state.name());
        storeInfo.put("partitions", partitionInfos);
        storeInfo.put("address", address);

        return storeInfo;
    }

    @GET
    @Timed
    @Path("status")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object status(@Context HugeConfig config) {

        LOG.debug("Get hstore cluster status");

        String status;
        try {
            status = client(config).getClusterStats().getState().name();
        } catch (PDException e) {
            throw new HugeException("Get store cluster status error", e);
        }

        return ImmutableMap.of("status", status);
    }

    @GET
    @Timed
    @Path("split")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object split(@Context HugeConfig config) {

        LOG.debug("Trigger the cluster to split...");

        try {
            client(config).splitData();
        } catch (PDException e) {
            throw new HugeException("split error: " + e.getMessage(), e);
        }

        return "success";
    }

    @GET
    @Timed
    @Path("{id}/startup")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object startup(@Context HugeConfig config,
                          @PathParam("id") long id) {

        LOG.debug("Query Hstore cluster status");
        Metapb.Store oldStore;
        try {
            oldStore = client(config).getStore(id);
        } catch (PDException e) {
            throw new HugeException(String.format(
                    "Get hstore node(%s) error", id), e);
        }

        Metapb.Store newStore = Metapb.Store.newBuilder(oldStore)
                                            .setState(Metapb.StoreState.Up)
                                            .build();
        try {
            client(config).updateStore(newStore);
        } catch (PDException e) {
            throw new HugeException("Startup error: " + e.getMessage(), e);
        }

        return "success";
    }

    @GET
    @Timed
    @Path("{id}/shutdown")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object shutdown(@Context HugeConfig config,
                           @PathParam("id") long id) {

        LOG.info("shutdown hstore node: %s");

        Metapb.Store oldStore;
        try {
            oldStore = client(config).getStore(id);
        } catch (PDException e) {
            throw new HugeException(String.format(
                    "Get hstore node(%s) error", id), e);
        }
        Metapb.Store newStore = Metapb.Store.newBuilder(oldStore)
                                            .setState(Metapb.StoreState.Tombstone)
                                            .build();
        try {
            client(config).updateStore(newStore);
        } catch (PDException e) {
            throw new HugeException("Shutdown error: " + e.getMessage(), e);
        }

        return "success";
    }
}
