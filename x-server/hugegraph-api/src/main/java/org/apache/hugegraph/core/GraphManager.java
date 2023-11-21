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

import static org.apache.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_DESCRIPTION;
import static org.apache.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_SERVICE_NAME;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeAuthenticator.User;
import org.apache.hugegraph.auth.HugeFactoryAuthProxy;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.cache.CachedGraphTransaction;
import org.apache.hugegraph.backend.cache.CachedSchemaTransaction;
import org.apache.hugegraph.backend.store.AbstractBackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStoreSystemInfo;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.io.HugeGraphSONModule;
import org.apache.hugegraph.k8s.K8sDriver;
import org.apache.hugegraph.k8s.K8sDriverProxy;
import org.apache.hugegraph.k8s.K8sManager;
import org.apache.hugegraph.k8s.K8sRegister;
import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.metrics.ServerReporter;
import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.serializer.JsonSerializer;
import org.apache.hugegraph.serializer.Serializer;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.space.Service.Status;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.optimize.HugeScriptTraversal;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.RegisterConfig;
import com.baidu.hugegraph.dto.ServiceDTO;
import com.baidu.hugegraph.registerimpl.PdRegister;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

import io.fabric8.kubernetes.api.model.Namespace;

public final class GraphManager {

    public static final String NAME_REGEX = "^[a-z][a-z0-9_]{0,47}$";
    // nickname should be compatible with all patterns of name
    public static final String NICKNAME_REGEX = "^[a-zA-Z\u4e00-\u9fa5]" +
                                                "[a-zA-Z0-9\u4e00-\u9fa5~!@#$" +
                                                "%^&*()_+|<>,.?/:;" +
                                                "'`\"\\[\\]{}\\\\]{0,47}$";
    public static final String AUTHID_REGEX = "^[a-zA-Z\u4e00-\u9fa5]" +
                                              "[a-zA-Z0-9\u4e00-\u9fa5~!@#$" +
                                              "%^&*()_+|<>,.?/:;" +
                                              "'`\"\\[\\]{}\\\\]{0,47}$";
    public static final int ID_MAX_LENGTH = 48;
    public static final int NICKNAME_MAX_LENGTH = 48;
    public static final String DELIMITER = "-";
    public static final String NAMESPACE_CREATE = "namespace_create";
    private static final Logger LOG = Log.logger(GraphManager.class);
    private final String cluster;
    private final String graphsDir;
    private final Boolean startIgnoreSingleGraphError;
    private final Boolean graphLoadFromLocalConfig;
    private final Boolean k8sApiEnabled;
    private final Map<String, GraphSpace> graphSpaces;
    private final Map<String, Service> services;
    private final Map<String, Graph> graphs;
    private final Set<String> localGraphs;
    private final Set<String> removingGraphs;
    private final Set<String> creatingGraphs;
    private final HugeAuthenticator authenticator;
    private final AuthManager authManager;
    private final MetaManager metaManager = MetaManager.instance();
    private final K8sManager k8sManager = K8sManager.instance();
    private final String serviceGraphSpace;
    private final String serviceID;
    private final String pdPeers;

    private final EventHub eventHub;

    private final String url;

    private final Set<String> serverUrlsToPd;
    private final Boolean serverDeployInK8s;

    private final HugeConfig config;

    private K8sDriver.CA ca;

    private String pdK8sServiceId;

    private DiscoveryClientImpl pdClient;

    private boolean licenseValid;

    public GraphManager(HugeConfig conf, EventHub hub) {

        LOG.info("Init graph manager");

        E.checkArgumentNotNull(conf, "The config can't be null");
        this.config = conf;
        this.url = conf.get(ServerOptions.REST_SERVER_URL);
        this.serverUrlsToPd = new HashSet<>(Arrays.asList(
                conf.get(ServerOptions.SERVER_URLS_TO_PD).split(",")));
        this.serverDeployInK8s =
                conf.get(ServerOptions.SERVER_DEPLOY_IN_K8S);
        this.startIgnoreSingleGraphError = conf.get(
                ServerOptions.SERVER_START_IGNORE_SINGLE_GRAPH_ERROR);
        this.graphsDir = conf.get(ServerOptions.GRAPHS);
        this.cluster = conf.get(ServerOptions.CLUSTER);
        this.graphSpaces = new ConcurrentHashMap<>();
        this.services = new ConcurrentHashMap<>();
        this.graphs = new ConcurrentHashMap<>();
        this.removingGraphs = ConcurrentHashMap.newKeySet();
        this.creatingGraphs = ConcurrentHashMap.newKeySet();
        this.authenticator = HugeAuthenticator.loadAuthenticator(conf);
        this.serviceGraphSpace = conf.get(ServerOptions.SERVICE_GRAPH_SPACE);
        this.serviceID = conf.get(ServerOptions.SERVICE_ID);
        this.pdPeers = conf.getString(ServerOptions.PD_PEERS.name());
        this.eventHub = hub;
        this.k8sApiEnabled = conf.get(ServerOptions.K8S_API_ENABLE);
        this.licenseValid = true;

        BrokerConfig.setPdPeers(this.pdPeers);

        try {
            this.pdClient = DiscoveryClientImpl
                    .newBuilder()
                    .setCenterAddress(this.pdPeers) // pd grpc端口
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.listenChanges();

        this.initMetaManager(conf);
        this.initK8sManagerIfNeeded(conf);

        this.createDefaultGraphSpaceIfNeeded(conf);

        this.loadGraphSpaces();

        this.loadServices();

        if (this.authenticator != null) {
            this.authManager = this.authenticator.authManager();
        } else {
            this.authManager = null;
        }

        Object y = conf.get(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG);
        this.graphLoadFromLocalConfig = Boolean.valueOf(y.toString());

        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            Map<String, String> graphConfigs =
                    ConfigUtil.scanGraphsDir(this.graphsDir);
            this.localGraphs = graphConfigs.keySet();
            this.loadGraphs(graphConfigs);
        } else {
            this.localGraphs = ImmutableSet.of();
        }

        // Load graphs configured in etcd
        this.loadGraphsFromMeta(this.graphConfigs());

        // this.installLicense(conf, "");
        this.waitGraphsStarted();
        this.checkBackendVersionOrExit(conf);
        this.serverStarted();
        this.addMetrics(conf);
        // listen meta changes, e.g. watch dynamically graph add/remove
        this.listenMetaChanges();
    }

    private static String serviceId(String graphSpace, Service.ServiceType type,
                                    String serviceName) {
        return String.join(DELIMITER, graphSpace, type.name(), serviceName)
                     .replace("_", "-").toLowerCase();
    }

    public static void prepareSchema(HugeGraph graph, String gremlin) {
        Map<String, Object> bindings = ImmutableMap.of(
                "graph", graph,
                "schema", graph.schema());
        HugeScriptTraversal<?, ?> traversal = new HugeScriptTraversal<>(
                graph.traversal(),
                "gremlin-groovy", gremlin,
                bindings, ImmutableMap.of());
        while (traversal.hasNext()) {
            traversal.next();
        }
        try {
            traversal.close();
        } catch (Exception e) {
            throw new HugeException("Failed to init schema", e);
        }
    }

    private static void registerCacheMetrics(Map<String, Cache<?, ?>> caches) {
        Set<String> names = MetricManager.INSTANCE.getRegistry().getNames();
        for (Map.Entry<String, Cache<?, ?>> entry : caches.entrySet()) {
            String key = entry.getKey();
            Cache<?, ?> cache = entry.getValue();

            String hits = String.format("%s.%s", key, "hits");
            String miss = String.format("%s.%s", key, "miss");
            String exp = String.format("%s.%s", key, "expire");
            String size = String.format("%s.%s", key, "size");
            String cap = String.format("%s.%s", key, "capacity");

            // Avoid registering multiple times
            if (names.stream().anyMatch(name -> name.endsWith(hits))) {
                continue;
            }

            MetricsUtil.registerGauge(Cache.class, hits, () -> cache.hits());
            MetricsUtil.registerGauge(Cache.class, miss, () -> cache.miss());
            MetricsUtil.registerGauge(Cache.class, exp, () -> cache.expire());
            MetricsUtil.registerGauge(Cache.class, size, () -> cache.size());
            MetricsUtil.registerGauge(Cache.class, cap, () -> cache.capacity());
        }
    }

    private static void sleep1s() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static String serviceName(String graphSpace, String service) {
        return String.join(DELIMITER, graphSpace, service);
    }

    private static String graphName(String graphSpace, String graph) {
        return String.join(DELIMITER, graphSpace, graph);
    }

    private static void checkGraphSpaceName(String name) {
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(name)) {
            return;
        }
        checkName(name, "graph space");
    }

    private static void checkServiceName(String name) {
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(name)) {
            return;
        }
        checkName(name, "service");
    }

    private static void checkGraphName(String name) {
        checkName(name, "graph");
    }

    private static void checkSchemaTemplateName(String name) {
        checkName(name, "schema template");
    }

    private static void checkName(String name, String type) {
        E.checkArgument(name.matches(NAME_REGEX),
                        "Invalid id or name '%s' for %s, valid name is up to " +
                        "48 alpha-numeric characters and underscores and only" +
                        "letters are supported as first letter. " +
                        "Note: letter is lower case", name, type);
    }

    public static void checkNickname(String nickname) {
        E.checkArgument(nickname.matches(NICKNAME_REGEX),
                        "Invalid nickname '%s' for %s, valid name is up " +
                        "to %s letters, Chinese or special " +
                        "characters, and can only start with a " +
                        "letter or Chinese", nickname, "graph",
                        NICKNAME_MAX_LENGTH);
    }

    public static void checkAuthName(String name) {
        E.checkArgumentNotNull(name, "The name or id can't be null");
        E.checkArgument(!name.contains("->"),
                        "The name or id cannot contain illegal phrase" +
                        "'->'");
        E.checkArgument(name.matches(AUTHID_REGEX),
                        "Invalid name '%s' for %s, valid name is up " +
                        "to %s letters, Chinese or special " +
                        "characters, and can only start with a " +
                        "letter or Chinese", name, "graph space",
                        ID_MAX_LENGTH);
    }

    public MetaManager meta() {
        return this.metaManager;
    }

    public void reload() {
        // Remove graphs from GraphManager
        for (String graph : this.graphs.keySet()) {
            String[] parts = graph.split(DELIMITER);
            this.dropGraph(parts[0], parts[1], false);
        }
        int count = 0;
        while (!this.graphs.isEmpty() && count++ < 10) {
            sleep1s();
        }
        if (!this.graphs.isEmpty()) {
            throw new HugeException("Failed to reload grahps, try later");
        }
        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            this.loadGraphs(ConfigUtil.scanGraphsDir(this.graphsDir));
        }
        // Load graphs configured in etcd
        this.loadGraphsFromMeta(this.graphConfigs());
    }

    public void reload(String graphSpace, String name) {
        if (!this.graphs.containsKey(name)) {
            return;
        }
        // Remove graphs from GraphManager
        this.dropGraph(graphSpace, name, false);
        int count = 0;
        while (this.graphs.containsKey(name) && count++ < 10) {
            sleep1s();
        }
        if (this.graphs.containsKey(name)) {
            throw new HugeException("Failed to reload '%s', try later", name);
        }
        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            Map<String, String> configs =
                    ConfigUtil.scanGraphsDir(this.graphsDir);
            if (configs.containsKey(name)) {
                this.loadGraphs(ImmutableMap.of(name, configs.get(name)));
            }
        }

        // Load graphs configured in etcd
        Map<String, Map<String, Object>> configs = this.graphConfigs();
        String graphName = graphName(graphSpace, name);
        if (configs.containsKey(graphName)) {
            this.loadGraphsFromMeta(ImmutableMap.of(graphName,
                                                    configs.get(graphName)));
        }
    }

    public void destroy() {
        this.unlistenChanges();
    }

    private void initMetaManager(HugeConfig conf) {
        List<String> endpoints = conf.get(ServerOptions.META_ENDPOINTS);
        boolean useCa = conf.get(ServerOptions.META_USE_CA);
        String ca = null;
        String clientCa = null;
        String clientKey = null;
        if (useCa) {
            ca = conf.get(ServerOptions.META_CA);
            clientCa = conf.get(ServerOptions.META_CLIENT_CA);
            clientKey = conf.get(ServerOptions.META_CLIENT_KEY);
            this.ca = new K8sDriver.CA(ca, clientCa, clientKey);
        }
        this.metaManager.connect(this.cluster, MetaManager.MetaDriverType.ETCD,
                                 ca, clientCa, clientKey, endpoints);
    }

    private void initK8sManagerIfNeeded(HugeConfig conf) {
        boolean useK8s = conf.get(ServerOptions.SERVER_USE_K8S);
        if (useK8s) {
            String oltpImage = conf.get(ServerOptions.SERVER_K8S_OLTP_IMAGE);
            String olapImage = conf.get(ServerOptions.SERVER_K8S_OLAP_IMAGE);
            String storageImage =
                    conf.get(ServerOptions.SERVER_K8S_STORAGE_IMAGE);
            this.k8sManager.connect(oltpImage, olapImage, storageImage, this.ca);
        }
    }

    private void createDefaultGraphSpaceIfNeeded(HugeConfig config) {
        Map<String, GraphSpace> graphSpaceConfigs =
                this.metaManager.graphSpaceConfigs();
        GraphSpace graphSpace;
        if (graphSpaceConfigs.containsKey(DEFAULT_GRAPH_SPACE_SERVICE_NAME)) {
            return;
        }
        String oltpNs = config.get(
                ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE);
        String olapNs = config.get(
                ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE);
        graphSpace = this.createGraphSpace(DEFAULT_GRAPH_SPACE_SERVICE_NAME,
                                           GraphSpace.DEFAULT_NICKNAME,
                                           DEFAULT_GRAPH_SPACE_DESCRIPTION,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, oltpNs, olapNs,
                                           false, User.ADMIN.getName(),
                                           ImmutableMap.of());
        boolean useK8s = config.get(ServerOptions.SERVER_USE_K8S);
        if (!useK8s) {
            return;
        }
        String oltp = config.get(ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE);
        // oltp namespace
        Namespace oltpNamespace = this.k8sManager.namespace(oltp);
        if (oltpNamespace == null) {
            throw new HugeException(
                    "The config option: %s, value: %s does not exist",
                    ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE.name(),
                    oltp);
        }
        graphSpace.oltpNamespace(oltp);
        // olap namespace
        String olap = config.get(ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE);
        Namespace olapNamespace = this.k8sManager.namespace(olap);
        if (olapNamespace == null) {
            throw new HugeException(
                    "The config option: %s, value: %s does not exist",
                    ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE.name(),
                    olap);
        }
        graphSpace.olapNamespace(olap);
        // storage is same as oltp
        graphSpace.storageNamespace(oltp);
        this.updateGraphSpace(graphSpace);
    }

    /**
     * Force overwrite internalAlgorithmImageUrl
     */
    public void overwriteAlgorithmImageUrl(String imageUrl) {
        if (StringUtils.isNotBlank(imageUrl) && this.k8sApiEnabled) {

            ServerOptions.K8S_INTERNAL_ALGORITHM_IMAGE_URL = new ConfigOption<>(
                    "k8s.internal_algorithm_image_url",
                    "K8s internal algorithm image url",
                    null,
                    imageUrl
            );

            String enableInternalAlgorithm = K8sDriverProxy.getEnableInternalAlgorithm();
            String internalAlgorithm = K8sDriverProxy.getInternalAlgorithm();
            Map<String, String> algorithms = K8sDriverProxy.getAlgorithms();
            try {
                K8sDriverProxy.setConfig(
                        enableInternalAlgorithm,
                        imageUrl,
                        internalAlgorithm,
                        algorithms
                );
            } catch (IOException e) {
                LOG.error("Overwrite internal_algorithm_image_url failed! {}", e);
            }
        }
    }

    private void loadGraphSpaces() {
        Map<String, GraphSpace> graphSpaceConfigs =
                this.metaManager.graphSpaceConfigs();
        this.graphSpaces.putAll(graphSpaceConfigs);
        for (Map.Entry<String, GraphSpace> entry : graphSpaceConfigs.entrySet()) {
            if (this.serviceGraphSpace.equals(entry.getKey())) {
                overwriteAlgorithmImageUrl(entry.getValue().internalAlgorithmImageUrl());
            }
        }
    }

    private void loadServices() {
        for (String graphSpace : this.graphSpaces.keySet()) {
            Map<String, Service> services = this.metaManager
                    .serviceConfigs(graphSpace);
            for (Map.Entry<String, Service> entry : services.entrySet()) {
                this.services.put(serviceName(graphSpace, entry.getKey()),
                                  entry.getValue());
            }
        }
        Service service = new Service(this.serviceID, User.ADMIN.getName(),
                                      Service.ServiceType.OLTP,
                                      Service.DeploymentType.MANUAL);
        service.description(service.name());

        if (this.serverDeployInK8s) {
            // 支持saas化仅在k8s中启动server,将正确server服务的urls注册到pd
            service.urls(this.serverUrlsToPd);
        } else {
            service.url(this.url);
        }

        service.serviceId(serviceId(this.serviceGraphSpace,
                                    Service.ServiceType.OLTP,
                                    this.serviceID));

        String serviceName = serviceName(this.serviceGraphSpace, this.serviceID);
        Boolean newAdded = false;
        if (!this.services.containsKey(serviceName)) {
            newAdded = true;
            // add to local cache
            this.services.put(serviceName, service);
        }
        Service self = this.services.get(serviceName);
        if (!self.sameService(service)) {
            /*
             * update service if it has been changed(e.g. for manual service,
             * url may change)
             */
            newAdded = true;
            self = service;
        }
        if (null != self) {
            // register self to pd, should prior to etcd due to pdServiceId info
            this.registerServiceToPd(this.serviceGraphSpace, self);
            if (self.k8s()) {
                try {
                    this.registerK8StoPd(self);
                } catch (Exception e) {
                    LOG.error("Register K8s info to PD failed: {}", e);
                }
            }
            if (newAdded) {
                // Register to etcd since even-handler has not been registered now
                this.metaManager.addServiceConfig(this.serviceGraphSpace, self);
                this.metaManager.notifyServiceAdd(this.serviceGraphSpace,
                                                  this.serviceID);
            }
        }
    }

    public boolean isAuth() {
        return this.graphSpace(this.serviceGraphSpace).auth();
    }

    private synchronized Map<String, Map<String, Object>> graphConfigs() {
        Map<String, Map<String, Object>> configs =
                CollectionFactory.newMap(CollectionType.EC);
        for (String graphSpace : this.graphSpaces.keySet()) {
            configs.putAll(this.metaManager.graphConfigs(graphSpace));
        }
        return configs;
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            HugeGraphAuthProxy.setAdmin();
            LOG.info("RestServer accepts event 'graph.create'");
            event.checkArgs(String.class, HugeGraph.class);
            String name = (String) event.args()[0];
            HugeGraph graph = (HugeGraph) event.args()[1];
            graph.switchAuthManager(this.authManager);
            this.graphs.putIfAbsent(name, graph);
            HugeGraphAuthProxy.resetContext();
            return null;
        });
        this.eventHub.listen(Events.GRAPH_DROP, event -> {
            HugeGraphAuthProxy.setAdmin();
            LOG.info("RestServer accepts event 'graph.drop'");
            event.checkArgs(String.class);
            String name = (String) event.args()[0];
            HugeGraph graph = (HugeGraph) this.graphs.remove(name);
            if (graph == null) {
                HugeGraphAuthProxy.resetContext();
                return null;
            }
            try {
                graph.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
            HugeGraphAuthProxy.resetContext();
            return null;
        });
    }

    private void unlistenChanges() {
        this.eventHub.unlisten(Events.GRAPH_CREATE);
        this.eventHub.unlisten(Events.GRAPH_DROP);
    }

    private void listenMetaChanges() {
        this.metaManager.listenGraphSpaceAdd(this::graphSpaceAddHandler);
        this.metaManager.listenGraphSpaceRemove(this::graphSpaceRemoveHandler);
        this.metaManager.listenGraphSpaceUpdate(this::graphSpaceUpdateHandler);

        this.metaManager.listenServiceAdd(this::serviceAddHandler);
        this.metaManager.listenServiceRemove(this::serviceRemoveHandler);
        this.metaManager.listenServiceUpdate(this::serviceUpdateHandler);

        this.metaManager.listenGraphAdd(this::graphAddHandler);
        this.metaManager.listenGraphRemove(this::graphRemoveHandler);
        this.metaManager.listenGraphUpdate(this::graphUpdateHandler);
        this.metaManager.listenGraphClear(this::graphClearHandler);

        this.metaManager.listenSchemaCacheClear(this::schemaCacheClearHandler);
        this.metaManager.listenGraphCacheClear(this::graphCacheClearHandler);
        this.metaManager.listenGraphVertexCacheClear(this::graphVertexCacheClearHandler);
        this.metaManager.listenGraphEdgeCacheClear(this::graphEdgeCacheClearHandler);
        this.metaManager.listenRestPropertiesUpdate(
                this.serviceGraphSpace, this.serviceID,
                this::restPropertiesHandler);
        this.metaManager.listenGremlinYamlUpdate(
                this.serviceGraphSpace, this.serviceID,
                this::gremlinYamlHandler);
        this.metaManager.listenAuthEvent(this::authHandler);
    }

    private void loadGraphs(final Map<String, String> graphConfigs) {
        for (Map.Entry<String, String> conf : graphConfigs.entrySet()) {
            String name = conf.getKey();
            String path = conf.getValue();
            HugeFactory.checkGraphName(name, "rest-server.properties");
            try {
                this.loadGraph(name, path);
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format("Failed to load graph '%s' from local",
                                        name), e);
            }
        }
    }

    private Date parseDate(Object o) {
        if (null == o) {
            return null;
        }
        String timeStr = String.valueOf(o);
        try {
            return HugeGraphSONModule.DATE_FORMAT.parse(timeStr);
        } catch (ParseException exc) {
            return null;
        }
    }

    private void loadGraphsFromMeta(
            Map<String, Map<String, Object>> graphConfigs) {
        for (Map.Entry<String, Map<String, Object>> conf :
                graphConfigs.entrySet()) {
            String[] parts = conf.getKey().split(DELIMITER);
            Map<String, Object> config = conf.getValue();

            String creator = String.valueOf(config.get("creator"));
            Date createTime = parseDate(config.get("create_time"));
            Date updateTime = parseDate(config.get("update_time"));


            HugeFactory.checkGraphName(parts[1], "meta server");
            try {
                HugeGraph graph = this.createGraph(parts[0], parts[1],
                                                   creator, config, false);
                graph.createTime(createTime);
                graph.updateTime(updateTime);
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format("Failed to load graph '%s' from " +
                                        "meta server", parts[1]), e);
            }
        }
    }

    private void waitGraphsStarted() {
        this.graphs.values().forEach(g -> {
            try {
                HugeGraph graph = (HugeGraph) g;
                graph.switchAuthManager(this.authManager);
                graph.waitStarted();
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error("Failed to wait graph started", e);
            }
        });
    }

    private GraphSpace createGraphSpace(String name, String nickname,
                                        String description,
                                        int cpuLimit, int memoryLimit,
                                        int storageLimit,
                                        int maxGraphNumber,
                                        int maxRoleNumber,
                                        String oltpNamespace,
                                        String olapNamespace,
                                        boolean auth, String creator,
                                        Map<String, Object> configs) {
        GraphSpace space = new GraphSpace(name, nickname, description,
                                          cpuLimit,
                                          memoryLimit, storageLimit,
                                          maxGraphNumber, maxRoleNumber,
                                          auth, creator, configs);
        space.oltpNamespace(oltpNamespace);
        space.olapNamespace(olapNamespace);
        return this.createGraphSpace(space);
    }

    @SuppressWarnings("unused")
    private GraphSpace createGraphSpace(String name,
                                        String nickname, String description,
                                        int cpuLimit, int memoryLimit,
                                        int storageLimit,
                                        int maxGraphNumber,
                                        int maxRoleNumber,
                                        boolean auth, String creator,
                                        Map<String, Object> configs) {
        GraphSpace space = new GraphSpace(name, nickname, description, cpuLimit,
                                          memoryLimit, storageLimit,
                                          maxGraphNumber, maxRoleNumber,
                                          auth, creator, configs);
        return this.createGraphSpace(space);
    }

    private GraphSpace updateGraphSpace(GraphSpace space) {
        String name = space.name();
        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.notifyGraphSpaceUpdate(name);
        this.graphSpaces.put(name, space);
        return space;
    }

    private void makeResourceQuota(String namespace, int cpuLimit,
                                   int memoryLimit) {
        k8sManager.loadResourceQuota(namespace, cpuLimit, memoryLimit);
    }

    /**
     * Create or get new namespaces
     *
     * @param namespace
     * @return isNewCreated
     */
    private boolean attachK8sNamespace(String namespace, String olapOperatorImage, Boolean isOlap) {
        boolean isNewCreated = false;
        try {
            if (!Strings.isNullOrEmpty(namespace)) {
                Namespace current = k8sManager.namespace(namespace);
                if (null == current) {
                    LockResult lock = this.metaManager.lock(this.cluster,
                                                            NAMESPACE_CREATE,
                                                            namespace);
                    try {
                        current = k8sManager.namespace(namespace);
                        if (null != current) {
                            return false;
                        }
                        current = k8sManager.createNamespace(namespace,
                                                             ImmutableMap.of());
                        if (null == current) {
                            throw new HugeException(
                                    "Cannot attach k8s namespace {}",
                                    namespace);
                        }
                        isNewCreated = true;
                        // start operator pod
                        // read from computer-system or default ?
                        // read from "hugegraph-computer-system"
                        // String containerName = "hugegraph-operator";
                        // String imageName = "";
                        if (isOlap) {
                            LOG.info("Try to create operator pod for k8s " +
                                     "namespace {} with operator image {}",
                                     namespace, olapOperatorImage);
                            k8sManager.createOperatorPod(namespace,
                                                         olapOperatorImage);
                        }
                    } finally {
                        this.metaManager.unlock(lock, this.cluster,
                                                NAMESPACE_CREATE, namespace);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Attach k8s namespace meet error {}", e);
        }
        return isNewCreated;
    }

    /*
     * 1.create DEFAULT space when init service
     * 2.Direct request server, create space with name and nickname
     * */
    public GraphSpace createGraphSpace(GraphSpace space) {
        String name = space.name();
        checkGraphSpaceName(name);
        String nickname = space.nickname();
        if (StringUtils.isNotEmpty(nickname)) {
            checkNickname(nickname);
        } else {
            nickname = name;
        }

        E.checkArgument(!isExistedSpaceNickname(name, nickname),
                        "Space nickname '%s' existed",
                        nickname);
        space.name(name);
        space.nickname(nickname);
        this.limitStorage(space, space.storageLimit);

        boolean useK8s = config.get(ServerOptions.SERVER_USE_K8S);

        if (useK8s) {
            E.checkArgument(!space.oltpNamespace().isEmpty() &&
                            !space.olapNamespace().isEmpty(),
                            "Oltp and olap namespace of space for " +
                            "k8s-enabled server must be set",
                            nickname);

            boolean notDefault = !DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(name);
            int cpuLimit = space.cpuLimit();
            int memoryLimit = space.memoryLimit();

            int computeCpuLimit = space.computeCpuLimit() == 0 ?
                                  space.cpuLimit() : space.computeCpuLimit();
            int computeMemoryLimit = space.computeMemoryLimit() == 0 ?
                                     space.memoryLimit() : space.computeMemoryLimit();
            boolean sameNamespace = space.oltpNamespace().equals(space.olapNamespace());
            attachK8sNamespace(space.oltpNamespace(),
                               space.operatorImagePath(), sameNamespace);
            if (notDefault) {
                if (sameNamespace) {
                    this.makeResourceQuota(space.oltpNamespace(),
                                           cpuLimit + computeCpuLimit,
                                           memoryLimit + computeMemoryLimit);
                } else {
                    this.makeResourceQuota(space.oltpNamespace(), cpuLimit,
                                           memoryLimit);
                }
            }
            if (!sameNamespace) {
                attachK8sNamespace(space.olapNamespace(),
                                   space.operatorImagePath(), true);
                if (notDefault) {
                    this.makeResourceQuota(space.olapNamespace(),
                                           computeCpuLimit, computeMemoryLimit);
                }
            }
        }

        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.appendGraphSpaceList(name);
        this.metaManager.notifyGraphSpaceAdd(name);
        this.graphSpaces.put(name, space);
        return space;
    }

    private void limitStorage(GraphSpace space, int storageLimit) {
        PDClient pdClient = PDClient.create(PDConfig.of(this.pdPeers)
                                                    .setEnablePDNotify(true));
        try {
            pdClient.setGraphSpace(space.name(), storageLimit);
        } catch (Exception e) {
            LOG.error("Exception occur when set storage limit!", e);
        }
    }

    public void getStorageInfo(String space) {
        GraphSpace gs = this.graphSpace(space);
        PDClient pdClient = PDClient.create(PDConfig.of(this.pdPeers)
                                                    .setEnablePDNotify(true));
        try {
            Metapb.GraphSpace spaceMeta = pdClient.getGraphSpace(space).get(0);
            Long usedGb = (spaceMeta.getUsedSize() / (1024 * 1024));
            gs.setStorageUsed(usedGb.intValue());
        } catch (PDException e) {
            LOG.error("Get storage information meet error {}", e);
        }
    }

    public void clearGraphSpace(String name) {
        // Clear all roles
        this.metaManager.clearGraphAuth(name);

        // Clear all schemaTemplate
        this.metaManager.clearSchemaTemplate(name);

        // Clear all graphs
        for (String key : this.graphs.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMITER);
                this.dropGraph(parts[0], parts[1], true);
            }
        }

        // Clear all services
        for (String key : this.services.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMITER);
                this.dropService(parts[0], parts[1]);
            }
        }
    }

    public void dropGraphSpace(String name) {
        if (this.serviceGraphSpace.equals(name)) {
            throw new HugeException("cannot delete service graph space %s",
                                    this.serviceGraphSpace);
        }
        this.clearGraphSpace(name);
        this.metaManager.removeGraphSpaceConfig(name);
        this.metaManager.clearGraphSpaceList(name);
        this.metaManager.notifyGraphSpaceRemove(name);
        this.graphSpaces.remove(name);
    }

    private void registerServiceToPd(String graphSpace, Service service) {
        try {
            PdRegister register = PdRegister.getInstance();
            RegisterConfig config = new RegisterConfig()
                    .setAppName(this.cluster)
                    .setGrpcAddress(this.pdPeers)
                    .setUrls(service.urls())
                    .setConsumer((Consumer<RegisterInfo>) registerInfo -> {
                        if (registerInfo.hasHeader()) {
                            Pdpb.ResponseHeader header = registerInfo.getHeader();
                            if (header.hasError()) {
                                Pdpb.ErrorType errorType = header.getError().getType();
                                if (errorType == Pdpb.ErrorType.LICENSE_ERROR
                                    || errorType == Pdpb.ErrorType.LICENSE_VERIFY_ERROR) {
                                    if (licenseValid) {
                                        LOG.warn("License check failure. {}",
                                                 header.getError().getMessage());
                                        licenseValid = false;
                                    }
                                    return;
                                } else {
                                    LOG.warn("RegisterServiceToPd Error. {}",
                                             header.getError().getMessage());
                                }
                            }
                        }
                        if (!licenseValid) {
                            LOG.warn("License is valid.");
                            licenseValid = true;
                        }
                    })
                    .setLabelMap(ImmutableMap.of(
                            PdRegisterLabel.REGISTER_TYPE.name(),
                            PdRegisterType.NODE_PORT.name(),
                            PdRegisterLabel.GRAPHSPACE.name(), graphSpace,
                            PdRegisterLabel.SERVICE_NAME.name(), service.name(),
                            PdRegisterLabel.SERVICE_ID.name(), service.serviceId(),
                            PdRegisterLabel.cores.name(),
                            String.valueOf(Runtime.getRuntime().availableProcessors())
                    ));

            String pdServiceId = register.registerService(config);
            service.pdServiceId(pdServiceId);
            LOG.info("Success to register service to pd");

        } catch (Exception e) {
            LOG.error("Failed to register service to pd", e);
        }
    }

    public void registerK8StoPd(Service service) throws Exception {
        try {
            PdRegister pdRegister = PdRegister.getInstance();
            K8sRegister k8sRegister = K8sRegister.instance();

            k8sRegister.initHttpClient();
            String rawConfig = k8sRegister.loadConfigStr();

            Gson gson = new Gson();
            ServiceDTO serviceDTO = gson.fromJson(rawConfig, ServiceDTO.class);
            RegisterConfig config = new RegisterConfig();

            String nodeName = System.getenv("MY_NODE_NAME");
            if (Strings.isNullOrEmpty(nodeName)) {
                nodeName = serviceDTO.getSpec().getClusterIP();
            }

            config
                    .setNodePort(serviceDTO.getSpec().getPorts()
                                           .get(0).getNodePort().toString())
                    .setNodeName(nodeName)
                    .setAppName(this.cluster)
                    .setGrpcAddress(this.pdPeers)
                    .setVersion(serviceDTO.getMetadata().getResourceVersion())
                    .setLabelMap(ImmutableMap.of(
                            PdRegisterLabel.REGISTER_TYPE.name(), PdRegisterType.NODE_PORT.name(),
                            PdRegisterLabel.GRAPHSPACE.name(), this.serviceGraphSpace,
                            PdRegisterLabel.SERVICE_NAME.name(), service.name(),
                            PdRegisterLabel.SERVICE_ID.name(), service.serviceId()
                    ));

            String ddsHost = this.metaManager.getDDSHost();
            if (!Strings.isNullOrEmpty(ddsHost)) {
                config.setDdsHost(ddsHost);
                config.setDdsSlave(BrokerConfig.getInstance().isSlave());
            }
            this.pdK8sServiceId = pdRegister.registerService(config);
        } catch (Exception e) {
            LOG.error("Register service k8s external info to pd failed!", e);
            throw e;
        }
    }

    public Service createService(String graphSpace, Service service) {
        String name = service.name();
        checkServiceName(name);

        if (null != service.urls() && service.urls().contains(this.url)) {
            throw new HugeException("url cannot be same as current url %s",
                                    this.url);
        }


        GraphSpace gs = this.metaManager.graphSpace(graphSpace);

        LockResult lock = this.metaManager.lock(this.cluster, graphSpace);
        try {
            if (gs.tryOfferResourceFor(service)) {
                this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
                this.metaManager.notifyGraphSpaceUpdate(graphSpace);
            } else {
                throw new HugeException("Not enough resources for service '%s'",
                                        service);
            }
        } finally {
            this.metaManager.unlock(lock, this.cluster, graphSpace);
        }

        lock = this.metaManager.lock(this.cluster, graphSpace, name);
        try {
            if (service.k8s()) {
                List<String> endpoints = this.config.get(
                        ServerOptions.META_ENDPOINTS);
                Set<String> urls = this.k8sManager.createService(
                        gs, service, endpoints, this.cluster);
                if (!urls.isEmpty()) {
                    String url = urls.iterator().next();
                    String[] parts = url.split(",");
                    service.port(Integer.valueOf(parts[parts.length - 1]));
                    service.urls().add(parts[0]);
                } else {
                    service.urls(urls);
                }
                service.status(Service.Status.STARTING);
            }
            service.serviceId(serviceId(graphSpace, service.type(),
                                        service.name()));

            // Persist to etcd
            this.metaManager.addServiceConfig(graphSpace, service);
            this.metaManager.notifyServiceAdd(graphSpace, name);
            this.services.put(serviceName(graphSpace, name), service);
        } catch (Exception e) {
            LOG.error("Create service failed {}", e);
            throw e;
        } finally {
            this.metaManager.unlock(lock, this.cluster, graphSpace, name);
        }

        return service;
    }

    public void startService(String graphSpace, Service service) {
        service.status(Status.STARTING);
        List<String> endpoints = this.config.get(ServerOptions.META_ENDPOINTS);
        GraphSpace gs = this.graphSpace(graphSpace);
        Set<String> urls = this.k8sManager.startService(gs, service, endpoints,
                                                        this.cluster);
        if (!urls.isEmpty()) {
            String url = urls.iterator().next();
            String[] parts = url.split(",");
            service.port(Integer.valueOf(parts[parts.length - 1]));
            service.urls().add(parts[0]);
        } else {
            service.urls(urls);
        }
        service.status(Service.Status.STARTING);
        this.metaManager.updateServiceConfig(graphSpace, service);
        this.metaManager.notifyServiceUpdate(graphSpace, service.name());
    }

    public void stopService(String graphSpace, String name) {
        Service service = this.service(graphSpace, name);
        if (null != service && service.k8s()) {
            GraphSpace gs = this.graphSpace(graphSpace);
            k8sManager.stopService(gs, service);
            service.running(0);
            service.status(Service.Status.STOPPED);
            this.metaManager.updateServiceConfig(graphSpace, service);
            this.metaManager.notifyServiceUpdate(graphSpace, service.name());
            if (!Strings.isNullOrEmpty(service.pdServiceId())) {
                PdRegister.getInstance().unregister(service.pdServiceId());
            }
        }
    }

    public void dropService(String graphSpace, String name) {
        GraphSpace gs = this.graphSpace(graphSpace);
        Service service = this.metaManager.service(graphSpace, name);
        if (null == service) {
            return;
        }
        if (service.k8s()) {
            this.k8sManager.deleteService(gs, service);
        }
        LockResult lock = this.metaManager.lock(this.cluster, graphSpace, name);
        this.metaManager.removeServiceConfig(graphSpace, name);
        this.metaManager.notifyServiceRemove(graphSpace, name);
        this.services.remove(serviceName(graphSpace, name));
        this.metaManager.unlock(lock, this.cluster, graphSpace, name);

        lock = this.metaManager.lock(this.cluster, graphSpace);
        gs.recycleResourceFor(service);
        this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        this.metaManager.notifyGraphSpaceUpdate(graphSpace);
        this.metaManager.unlock(lock, this.cluster, graphSpace);

        String pdServiceId = service.pdServiceId();
        LOG.debug("Going to unregister service {} from Pd", pdServiceId);
        if (StringUtils.isNotEmpty(pdServiceId)) {
            PdRegister register = PdRegister.getInstance();
            register.unregister(service.pdServiceId());
            LOG.debug("Service {} has been withdrew from Pd", pdServiceId);
        }
    }

    public HugeGraph createGraph(String graphSpace, String name, String creator,
                                 Map<String, Object> configs, boolean init) {
        String key = String.join(DELIMITER, graphSpace, name);
        if (this.graphs.containsKey(key)) {
            throw new ExistedException("graph", key);
        }
        boolean grpcThread = Thread.currentThread().getName().contains("grpc");
        if (grpcThread) {
            HugeGraphAuthProxy.setAdmin();
        }
        E.checkArgumentNotNull(name, "The graph name can't be null");
        checkGraphName(name);
        String nickname;
        if (configs.get("nickname") != null) {
            nickname = configs.get("nickname").toString();
            checkNickname(nickname);
        } else {
            nickname = name;
        }

        // init = false means load graph from meta
        E.checkArgument(!init || !isExistedGraphNickname(graphSpace, nickname),
                        "Graph nickname '%s' for %s has existed",
                        nickname, graphSpace);

        GraphSpace gs = this.graphSpace(graphSpace);
        E.checkArgumentNotNull(gs, "Invalid graph space: '%s'", graphSpace);
        if (!grpcThread && init) {
            // 从pd获取所有的图
            Set<String> allGraphs = this.graphs(graphSpace);
            gs.graphNumberUsed(allGraphs.size());
            if (gs.tryOfferGraph()) {
                LOG.info("The graph_number_used successfully increased to {} " +
                         "of graph space: {} for graph: {}",
                         gs.graphNumberUsed(), gs.name(), name);
            } else {
                throw new HugeException("Failed create graph due to reach " +
                                        "graph limit for graph space '%s'",
                                        graphSpace);
            }
        }

        configs.put(ServerOptions.PD_PEERS.name(), this.pdPeers);
        configs.put(CoreOptions.GRAPH_SPACE.name(), graphSpace);
        boolean auth = this.metaManager.graphSpace(graphSpace).auth();
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(graphSpace) || !auth) {
            configs.put("gremlin.graph", "org.apache.hugegraph.HugeFactory");
        } else {
            configs.put("gremlin.graph", "org.apache.hugegraph.auth.HugeFactoryAuthProxy");
        }

        configs.put("graphSpace", graphSpace);

        Date timeStamp = new Date();

        configs.putIfAbsent("nickname", nickname);
        configs.putIfAbsent("creator", creator);
        configs.putIfAbsent("create_time", timeStamp);
        configs.putIfAbsent("update_time", timeStamp);

        Configuration propConfig = this.buildConfig(configs);
        String storeName = propConfig.getString(CoreOptions.STORE.name());
        E.checkArgument(name.equals(storeName),
                        "The store name '%s' not match url name '%s'",
                        storeName, name);

        HugeConfig config = new HugeConfig(propConfig);
        this.checkOptions(graphSpace, config);
        HugeGraph graph = this.createGraph(graphSpace, config,
                                           this.authManager, init);
        graph.graphSpace(graphSpace);

        graph.nickname(nickname);
        graph.creator(creator);
        graph.createTime(timeStamp);
        graph.updateTime(timeStamp);

        String graphName = graphName(graphSpace, name);
        if (init) {
            this.creatingGraphs.add(graphName);
            this.metaManager.addGraphConfig(graphSpace, name, configs);
            this.metaManager.notifyGraphAdd(graphSpace, name);
        }
        this.graphs.put(graphName, graph);
        if (!grpcThread) {
            this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        }
        // Let gremlin server and rest server context add graph
        this.eventHub.notify(Events.GRAPH_CREATE, graphName, graph);

        if (init) {
            String schema = propConfig.getString(
                    CoreOptions.SCHEMA_INIT_TEMPLATE.name());
            if (schema == null || schema.isEmpty()) {
                return graph;
            }
            String schemas = this.schemaTemplate(graphSpace, schema).schema();
            prepareSchema(graph, schemas);
        }
        if (grpcThread) {
            HugeGraphAuthProxy.resetContext();
        }
        return graph;
    }

    public boolean isExistedSpaceNickname(String space, String nickname) {
        if (StringUtils.isEmpty(nickname)) {
            return false;
        }
        Set<String> graphSpaces = this.graphSpaces();
        for (String graphSpace : graphSpaces) {
            GraphSpace gs = this.graphSpace(graphSpace);
            // when update space, return true if nickname exists in other space
            if (nickname.equals(gs.nickname()) && !graphSpace.equals(space)) {
                return true;
            }
        }
        return false;
    }

    public boolean isExistedGraphNickname(String graphSpace, String nickname) {
        if (StringUtils.isEmpty(nickname)) {
            return false;
        }
        for (Map<String, Object> graphConfig :
                this.metaManager.graphConfigs(graphSpace).values()) {
            if (nickname.equals(graphConfig.get("nickname").toString())) {
                return true;
            }
        }
        return false;
    }

    private HugeGraph createGraph(String graphSpace, HugeConfig config,
                                  AuthManager authManager, boolean init) {
        // open succeed will fill graph instance into HugeFactory graphs(map)
        HugeGraph graph;
        try {
            graph = (HugeGraph) GraphFactory.open(config);
        } catch (Throwable e) {
            LOG.error("Exception occur when open graph", e);
            throw e;
        }
        graph.switchAuthManager(authManager);
        graph.graphSpace(graphSpace);
        graph.nickname(config.getString("nickname"));
        if (this.requireAuthentication()) {
            /*
             * The main purpose is to call method
             * verifyPermission(HugePermission.WRITE, ResourceType.STATUS)
             * that is private
             */
            graph.mode(GraphMode.NONE);
        }
        if (init) {
            try {
                graph.initBackend();
                graph.serverStarted();
            } catch (BackendException e) {
                try {
                    graph.close();
                } catch (Exception e1) {
                    if (graph instanceof StandardHugeGraph) {
                        ((StandardHugeGraph) graph).clearSchedulerAndLock();
                    }
                }
                HugeFactory.remove(graph);
                throw e;
            }
        }
        return graph;
    }

    private MapConfiguration buildConfig(Map<String, Object> configs) {
        return new MapConfiguration(configs);
    }

    private PropertiesConfiguration buildConfig(String configText) {
        E.checkArgument(StringUtils.isNotEmpty(configText),
                        "The config text can't be null or empty");
        PropertiesConfiguration propConfig = new PropertiesConfiguration();
        try {
            Reader in = new StringReader(configText);
            propConfig.read(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read config options", e);
        }
        return propConfig;
    }

    private void checkOptions(String graphSpace, HugeConfig config) {
        // The store cannot be the same as the existing graph
        this.checkOptionsUnique(graphSpace, config, CoreOptions.STORE);
        // NOTE: rocksdb can't use same data path for different graph,
        // but it's not easy to check here
        String backend = config.get(CoreOptions.BACKEND);
        if (backend.equalsIgnoreCase("rocksdb")) {
            // TODO: should check data path...
        }
    }

    public void dropGraph(String graphSpace, String name, boolean clear) {
        boolean grpcThread = Thread.currentThread().getName().contains("grpc");
        HugeGraph g = this.graph(graphSpace, name);
        E.checkArgumentNotNull(g, "The graph '%s' doesn't exist", name);
        if (this.localGraphs.contains(name)) {
            throw new HugeException("Can't delete graph '%s' loaded from " +
                                    "local config. Please delete config file " +
                                    "and restart HugeGraphServer if really " +
                                    "want to delete it.", name);
        }

        String graphName = graphName(graphSpace, name);
        if (clear) {
            this.removingGraphs.add(graphName);
            try {
                this.metaManager.removeGraphConfig(graphSpace, name);
                this.metaManager.notifyGraphRemove(graphSpace, name);
            } catch (Exception e) {
                throw new HugeException(
                        "Failed to remove graph config of '%s'", name, e);
            }

            /**
             * close task scheduler before clear data,
             * because taskinfo stored in backend in
             * {@link org.apache.hugegraph.task.DistributedTaskScheduler}
             */
            try {
                g.taskScheduler().close();
            } catch (Throwable t) {
                LOG.warn(String.format("Error when close TaskScheduler of %s",
                                       graphName),
                         t);
            }

            g.clearBackend();
            try {
                g.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
        }
        GraphSpace gs = this.graphSpace(graphSpace);
        if (!grpcThread) {
            gs.recycleGraph();
            LOG.info("The graph_number_used successfully decreased to {} " +
                     "of graph space: {} for graph: {}",
                     gs.graphNumberUsed(), gs.name(), name);
            this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        }
        // Let gremlin server and rest server context remove graph
        LOG.info("Notify remove graph {} by GRAPH_DROP event", name);
        Graph graph = this.graphs.remove(graphName);
        if (graph != null) {
            try {
                graph.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
        }
        this.eventHub.notify(Events.GRAPH_DROP, graphName);
    }

    public Set<String> graphSpaces() {
        return Collections.unmodifiableSet(this.graphSpaces.keySet());
    }

    public Set<String> services(String graphSpace) {
        Set<String> result = new HashSet<>();
        for (String key : this.services.keySet()) {
            String[] parts = key.split(DELIMITER);
            if (parts[0].equals(graphSpace)) {
                result.add(parts[1]);
            }
        }
        return result;
    }

    public Service service(String graphSpace, String name) {
        String key = String.join(DELIMITER, graphSpace, name);
        Service service = this.services.get(key);
        if (service == null) {
            service = this.metaManager.service(graphSpace, name);
        }
        if (service.manual()) {
            return service;
        }
        GraphSpace gs = this.graphSpace(graphSpace);
        int running = this.k8sManager.podsRunning(gs, service);
        if (service.running() != running) {
            service.running(running);
            this.metaManager.updateServiceConfig(graphSpace, service);
        }
        if (service.running() != 0) {
            service.status(Service.Status.RUNNING);
            this.metaManager.updateServiceConfig(graphSpace, service);
        }
        return service;
    }

    public Set<String> getServiceDdsUrls(String graphSpace, String service) {
        return this.getServiceUrls(graphSpace, service, PdRegisterType.DDS);
    }

    public Set<String> getServiceNodePortUrls(String graphSpace,
                                              String service) {
        return this.getServiceUrls(graphSpace, service,
                                   PdRegisterType.NODE_PORT);
    }

    public Set<String> getServiceUrls(String graphSpace, String service,
                                      PdRegisterType registerType) {
        Map<String, String> configs = new HashMap<>();
        if (StringUtils.isNotEmpty(graphSpace)) {
            configs.put(PdRegisterLabel.REGISTER_TYPE.name(), graphSpace);
        }
        if (StringUtils.isNotEmpty(service)) {
            configs.put(PdRegisterLabel.SERVICE_NAME.name(), service);
        }
        configs.put(PdRegisterLabel.REGISTER_TYPE.name(), registerType.name());
        Query query = Query.newBuilder().setAppName(cluster)
                           .putAllLabels(configs)
                           .build();
        NodeInfos nodeInfos = this.pdClient.getNodeInfos(query);
        for (NodeInfo nodeInfo : nodeInfos.getInfoList()) {
            LOG.info("node app name {}, node address: {}",
                     nodeInfo.getAppName(), nodeInfo.getAddress());
        }
        return nodeInfos.getInfoList().stream()
                        .map(nodeInfo -> nodeInfo.getAddress())
                        .collect(Collectors.toSet());
    }

    public Set<HugeGraph> graphs() {
        Set<HugeGraph> graphs = new HashSet<>();
        for (Graph g : this.graphs.values()) {
            graphs.add((HugeGraph) g);
        }
        return graphs;
    }

    public Set<String> graphs(String graphSpace) {
        Set<String> graphs = new HashSet<>();
        for (String key : this.metaManager.graphConfigs(graphSpace).keySet()) {
            graphs.add(key.split(DELIMITER)[1]);
        }
        return graphs;
    }

    public HugeGraph graph(String graphSpace, String name) {
        String key = String.join(DELIMITER, graphSpace, name);
        Graph graph = this.graphs.get(key);
        if (graph == null) {
            Map<String, Map<String, Object>> configs =
                    this.metaManager.graphConfigs(graphSpace);
            if (!configs.containsKey(key)) {
                return null;
            }
            Map<String, Object> config = configs.get(key);
            String creator = String.valueOf(config.get("creator"));
            Date createTime = parseDate(config.get("create_time"));
            Date updateTime = parseDate(config.get("update_time"));
            HugeGraph graph1 = this.createGraph(graphSpace, name,
                                                creator, config, false);
            graph1.createTime(createTime);
            graph1.updateTime(updateTime);
            this.graphs.put(key, graph1);
            return graph1;
        } else if (graph instanceof HugeGraph) {
            return (HugeGraph) graph;
        }
        throw new NotSupportException("graph instance of %s", graph.getClass());
    }

    public GraphSpace graphSpace(String name) {
        GraphSpace space = this.graphSpaces.get(name);
        if (space == null) {
            space = this.metaManager.graphSpace(name);
        }
        return space;
    }

    public Serializer serializer() {
        return JsonSerializer.instance();
    }

    public Serializer serializer(Graph g, Map<String, Object> debugMeasure) {
        return JsonSerializer.instance(debugMeasure);
    }

    public void rollbackAll() {
        this.graphs.values().forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.ROLLBACK);
    }

    public void commitAll() {
        this.graphs.values().forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().commit();
            }
        });
    }

    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.COMMIT);
    }

    public boolean requireAuthentication() {
        if (this.authenticator == null) {
            return false;
        }
        return this.authenticator.requireAuthentication();
    }

    public HugeAuthenticator.User authenticate(Map<String, String> credentials)
            throws AuthenticationException {
        return this.authenticator().authenticate(credentials);
    }

    public AuthManager authManager() {
        return this.authenticator().authManager();
    }

    public void close() {
    }

    private HugeAuthenticator authenticator() {
        E.checkState(this.authenticator != null,
                     "Unconfigured authenticator");
        return this.authenticator;
    }

    public boolean isAuthRequired() {
        return this.authenticator != null;
    }

    public boolean isLicenseValid() {
        return licenseValid;
    }

    private void closeTx(final Set<String> graphSourceNamesToCloseTxOn,
                         final Transaction.Status tx) {
        final Set<Graph> graphsToCloseTxOn = new HashSet<>();

        graphSourceNamesToCloseTxOn.forEach(name -> {
            if (this.graphs.containsKey(name)) {
                graphsToCloseTxOn.add(this.graphs.get(name));
            }
        });

        graphsToCloseTxOn.forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                if (tx == Transaction.Status.COMMIT) {
                    graph.tx().commit();
                } else {
                    graph.tx().rollback();
                }
            }
        });
    }

    private void loadGraph(String name, String path) {
        final HugeGraph graph = (HugeGraph) GraphFactory.open(path);
        String graphName = graphName(DEFAULT_GRAPH_SPACE_SERVICE_NAME, name);
        graph.graphSpace(DEFAULT_GRAPH_SPACE_SERVICE_NAME);
        graph.switchAuthManager(this.authManager);
        this.graphs.put(graphName, graph);
        LOG.info("Graph '{}' was successfully configured via '{}'", name, path);

        if (this.requireAuthentication() &&
            !(graph instanceof HugeGraphAuthProxy)) {
            LOG.warn("You may need to support access control for '{}' with {}",
                     path, HugeFactoryAuthProxy.GRAPH_FACTORY);
        }
    }

    private void checkBackendVersionOrExit(HugeConfig config) {
        for (Graph g : this.graphs.values()) {
            try {
                HugeGraph hugegraph = (HugeGraph) g;
                if (!hugegraph.backendStoreFeatures().supportsPersistence()) {
                    hugegraph.initBackend();
                }
                BackendStoreSystemInfo info = hugegraph.backendStoreSystemInfo();
                if (!info.exists()) {
                    throw new BackendException(
                            "The backend store of '%s' has not been " +
                            "initialized", hugegraph.name());
                }
                if (!info.checkVersion()) {
                    throw new BackendException(
                            "The backend store version is inconsistent");
                }
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format(
                        "Failed to check backend version for graph '%s'",
                        ((HugeGraph) g).name()), e);
            } finally {
                // close tx from main thread
                if (g.tx().isOpen()) {
                    g.tx().close();
                }
            }
        }
    }

    private void serverStarted() {
        for (Graph graph : this.graphs.values()) {
            try {
                HugeGraph hugegraph = (HugeGraph) graph;
                assert hugegraph != null;
                hugegraph.serverStarted();
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format(
                        "Failed to server started for graph '%s'", graph), e);
            }
        }
    }

    private <T> void graphSpaceAddHandler(T response) {
        List<String> names = this.metaManager
                .extractGraphSpacesFromResponse(response);

        for (String gs : names) {
            GraphSpace graphSpace = this.metaManager.getGraphSpaceConfig(gs);
            this.graphSpaces.put(gs, graphSpace);
            if (this.serviceGraphSpace.equals(gs)) {
                overwriteAlgorithmImageUrl(graphSpace.internalAlgorithmImageUrl());
            }
        }
    }

    private <T> void graphSpaceRemoveHandler(T response) {
        List<String> names = this.metaManager
                .extractGraphSpacesFromResponse(response);
        for (String gs : names) {
            this.graphSpaces.remove(gs);
        }
    }

    private <T> void graphSpaceUpdateHandler(T response) {
        List<String> names = this.metaManager
                .extractGraphSpacesFromResponse(response);
        for (String gs : names) {
            GraphSpace graphSpace = this.metaManager.getGraphSpaceConfig(gs);
            this.graphSpaces.put(gs, graphSpace);
            if (this.serviceGraphSpace.equals(gs)) {
                overwriteAlgorithmImageUrl(graphSpace.internalAlgorithmImageUrl());
            }
        }
    }

    private <T> void serviceAddHandler(T response) {
        List<String> names = this.metaManager
                .extractServicesFromResponse(response);
        Service service;
        for (String s : names) {
            String[] parts = s.split(DELIMITER);
            String graphSpace = parts[0];
            String serviceName = parts[1];
            String serviceRawConf = this.metaManager.getServiceRawConfig(
                    graphSpace, serviceName);

            service = this.metaManager.parseServiceRawConfig(serviceRawConf);
            this.services.put(s, service);
        }
    }

    private <T> void serviceRemoveHandler(T response) {
        List<String> names = this.metaManager
                .extractServicesFromResponse(response);
        for (String s : names) {
            this.services.remove(s);
        }
    }

    private <T> void serviceUpdateHandler(T response) {
        List<String> names = this.metaManager
                .extractServicesFromResponse(response);
        Service service;
        for (String s : names) {
            String[] parts = s.split(DELIMITER);
            String graphSpace = parts[0];
            String serviceName = parts[1];
            String serviceRawConf = this.metaManager.getServiceRawConfig(
                    graphSpace, serviceName);
            service = this.metaManager.parseServiceRawConfig(serviceRawConf);
            this.services.put(s, service);
        }
    }

    private <T> void graphAddHandler(T response) {
        List<String> names = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : names) {
            LOG.info("Accept graph add signal from etcd for {}", graphName);
            if (this.graphs.containsKey(graphName) ||
                this.creatingGraphs.contains(graphName)) {
                this.creatingGraphs.remove(graphName);
                continue;
            }

            LOG.info("Not exist in cache, Starting construct graph {}",
                     graphName);
            String[] parts = graphName.split(DELIMITER);
            Map<String, Object> config =
                    this.metaManager.getGraphConfig(parts[0], parts[1]);
            Object objc = config.get("creator");
            String creator = null == objc ?
                             GraphSpace.DEFAULT_CREATOR_NAME :
                             String.valueOf(objc);

            // Create graph without init
            try {
                HugeGraph graph = this.createGraph(parts[0], parts[1], creator,
                                                   config, false);
                graph.started(true);
                if (graph.tx().isOpen()) {
                    graph.tx().close();
                }
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format(
                        "Failed to create graph '%s'", graphName), e);
            }
        }
    }

    private <T> void graphRemoveHandler(T response) {
        List<String> graphNames = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (!this.graphs.containsKey(graphName) ||
                this.removingGraphs.contains(graphName)) {
                this.removingGraphs.remove(graphName);
                continue;
            }

            // Remove graph without clear
            String[] parts = graphName.split(DELIMITER);
            try {
                this.dropGraph(parts[0], parts[1], false);
            } catch (HugeException e) {
                LOG.error(String.format(
                        "Failed to drop graph '%s'", graphName), e);
            }
        }
    }

    private <T> void graphUpdateHandler(T response) {
        List<String> graphNames = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (this.graphs.containsKey(graphName)) {
                Graph graph = this.graphs.get(graphName);
                if (graph instanceof HugeGraph) {
                    HugeGraph hugeGraph = (HugeGraph) graph;
                    String[] values =
                            graphName.split(MetaManager.META_PATH_JOIN);
                    Map<String, Object> configs =
                            this.metaManager.getGraphConfig(values[0],
                                                            values[1]);
                    String readMode = configs.get(
                            CoreOptions.GRAPH_READ_MODE.name()).toString();
                    hugeGraph.readMode(GraphReadMode.valueOf(readMode));
                }
            }
        }
    }

    private <T> void graphClearHandler(T response) {
        List<String> graphNames = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (this.graphs.containsKey(graphName)) {
                Graph graph = this.graphs.get(graphName);
                if (graph instanceof HugeGraph) {
                    HugeGraph hugeGraph = (HugeGraph) graph;
                    ((AbstractBackendStoreProvider) hugeGraph.storeProvider())
                            .notifyAndWaitEvent(Events.STORE_CLEAR);
                }
            }
        }
    }

    private <T> void schemaCacheClearHandler(T response) {
        boolean grpcThread = false;
        try {
            grpcThread = Thread.currentThread().getName().contains("grpc");
            if (grpcThread) {
                HugeGraphAuthProxy.setAdmin();
            }

            List<String> graphNames = this.metaManager
                    .extractGraphsFromResponse(response);
            for (String graphName : graphNames) {
                LOG.info("Handle schema clear, graph name: {}", graphName);
                if (this.graphs.containsKey(graphName)) {
                    Graph graph = this.graphs.get(graphName);
                    if (graph instanceof HugeGraphAuthProxy) {
                        LOG.debug("graph instanceof HugeGraphAuthProxy: {}",
                                  graphName);
                        graph = ((HugeGraphAuthProxy) graph).hugegraph();
                    }
                    if (graph instanceof StandardHugeGraph) {
                        StandardHugeGraph hugeGraph = (StandardHugeGraph) graph;
                        CachedSchemaTransaction schemaTransaction =
                                (CachedSchemaTransaction) hugeGraph.schemaTransaction();
                        schemaTransaction.clearCache(true);
                        LOG.debug("schema cache clear done");
                    } else {
                        LOG.info("graph not instanceof StandardHugeGraph: {}",
                                 graph.getClass().getName());
                    }
                } else {
                    LOG.info("graphs not contains graph name: {}", graphName);
                }
            }
        } catch (Throwable e) {
            LOG.error("schemaClearHandler occur error", e);
            throw e;
        } finally {
            if (grpcThread) {
                HugeGraphAuthProxy.resetContext();
            }
        }
    }

    /**
     * 清空点 边 以及图的其他缓存信息
     *
     * @param response
     * @param <T>
     */
    private <T> void graphCacheClearHandler(T response) {
        this.clearGraphCache(response, null, true);
    }

    /**
     * 清空点的缓存信息
     *
     * @param response
     * @param <T>
     */
    private <T> void graphVertexCacheClearHandler(T response) {
        this.clearGraphCache(response, HugeType.VERTEX, false);
    }

    /**
     * 清空边的缓存信息
     *
     * @param response
     * @param <T>
     */
    private <T> void graphEdgeCacheClearHandler(T response) {
        this.clearGraphCache(response, HugeType.EDGE, false);
    }

    private <T> void clearGraphCache(T response, HugeType hugeType,
                                     boolean notify) {
        boolean grpcThread = false;
        try {
            grpcThread = Thread.currentThread().getName().contains("grpc");
            if (grpcThread) {
                HugeGraphAuthProxy.setAdmin();
            }
            List<String> graphNames = this.metaManager
                    .extractGraphsFromResponse(response);
            for (String graphName : graphNames) {
                LOG.info("Handle schema clear, graph name: {}", graphName);
                if (this.graphs.containsKey(graphName)) {
                    Graph graph = this.graphs.get(graphName);
                    if (graph instanceof HugeGraphAuthProxy) {
                        LOG.debug("graph instanceof HugeGraphAuthProxy: {}",
                                  graphName);
                        graph = ((HugeGraphAuthProxy) graph).hugegraph();
                    }
                    if (graph instanceof HugeGraph) {
                        HugeGraph hugeGraph = (HugeGraph) graph;
                        CachedGraphTransaction graphTransaction = Whitebox.invoke(
                                hugeGraph.getClass(),
                                "graphTransaction", hugeGraph);
                        graphTransaction.clearCache(hugeType, notify);
                        LOG.info("schema cache clear done, type: {} notify:{}",
                                 hugeType, notify);
                    } else {
                        LOG.info("graph not instanceof StandardHugeGraph: {}",
                                 graph.getClass().getName());
                    }
                } else {
                    LOG.info("graphs not contains graph name: {}", graphName);
                }
            }
        } catch (Throwable e) {
            LOG.error("schemaClearHandler occur error", e);
            throw e;
        } finally {
            if (grpcThread) {
                HugeGraphAuthProxy.resetContext();
            }
        }
    }

    private void addMetrics(HugeConfig config) {
        final MetricManager metric = MetricManager.INSTANCE;
        // Force to add server reporter
        int metricsDataTtl = config.get(ServerOptions.METRICS_DATA_TTL);
        boolean metricsDataToPd = config.get(ServerOptions.METRICS_DATA_TO_PD);
        ServerReporter reporter =
                ServerReporter.instance(metric.getRegistry(),
                                        (PdMetaDriver) this.meta().metaDriver(),
                                        metricsDataTtl, this.cluster,
                                        metricsDataToPd);
        // default reportTime = 60s
        int reportTime = config.get(ServerOptions.METRICS_DATA_REPORT_TIME);
        reporter.start(reportTime, TimeUnit.SECONDS);

        // Add metrics for MAX_WRITE_THREADS
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        MetricsUtil.registerGauge(RestServer.class, "max-write-threads",
                                  () -> maxWriteThreads);

        // Add metrics for caches
        @SuppressWarnings({"rawtypes", "unchecked"})
        Map<String, Cache<?, ?>> caches = (Map) CacheManager.instance()
                                                            .caches();
        registerCacheMetrics(caches);
        final AtomicInteger lastCachesSize = new AtomicInteger(caches.size());
        MetricsUtil.registerGauge(Cache.class, "instances", () -> {
            int count = caches.size();
            if (count != lastCachesSize.get()) {
                // Update if caches changed (effect in the next report period)
                registerCacheMetrics(caches);
                lastCachesSize.set(count);
            }
            return count;
        });

        // Add metrics for task
        MetricsUtil.registerGauge(TaskManager.class, "workers",
                                  () -> TaskManager.instance().workerPoolSize());
        MetricsUtil.registerGauge(TaskManager.class, "pending-tasks",
                                  () -> TaskManager.instance().pendingTasks());
    }

    private void checkOptionsUnique(String graphSpace,
                                    HugeConfig config,
                                    TypedOption<?, ?> option) {
        Object incomingValue = config.get(option);
        for (Map.Entry<String, Graph> entry : this.graphs.entrySet()) {
            String[] parts = entry.getKey().split(DELIMITER);
            if (!Objects.equals(graphSpace, parts[0]) ||
                !Objects.equals(incomingValue, parts[1])) {
                continue;
            }
            Object existedValue = ((HugeGraph) entry.getValue()).option(option);
            E.checkArgument(!incomingValue.equals(existedValue),
                            "The option '%s' conflict with existed",
                            option.name());
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void restPropertiesHandler(T response) {
        List<String> events = this.metaManager
                .extractGraphsFromResponse(response);
        try {
            for (String event : events) {
                if (StringUtils.isNotEmpty(event)) {
                    Map<String, Object> properties = JsonUtil.fromJson(event, Map.class);
                    HugeConfig conf = new HugeConfig(new MapConfiguration(properties));
                    if (k8sApiEnabled) {
                        GraphSpace gs = this.metaManager.graphSpace(this.serviceGraphSpace);
                        // conf.get(
                        //ServerOptions.K8S_KUBE_CONFIG);
                        String enableInternalAlgorithm = conf.get(
                                ServerOptions.K8S_ENABLE_INTERNAL_ALGORITHM);
                        String internalAlgorithmImageUrl = conf.get(
                                ServerOptions.K8S_INTERNAL_ALGORITHM_IMAGE_URL);
                        String internalAlgorithm = conf.get(
                                ServerOptions.K8S_INTERNAL_ALGORITHM);
                        Map<String, String> algorithms = conf.getMap(
                                ServerOptions.K8S_ALGORITHMS);
                        K8sDriverProxy.setConfig(enableInternalAlgorithm,
                                                 internalAlgorithmImageUrl,
                                                 internalAlgorithm,
                                                 algorithms);
                    } else {
                        K8sDriverProxy.disable();
                    }
                }
            }
        } catch (IOException e) {
            LOG.warn(e.toString());
        }
    }

    private <T> void gremlinYamlHandler(T response) {
        List<String> events = this.metaManager
                .extractGraphsFromResponse(response);
        for (String event : events) {
            // TODO: Restart gremlin server
        }
    }

    private <T> void authHandler(T response) {
        List<String> events = this.metaManager
                .extractGraphsFromResponse(response);
        for (String event : events) {
            Map<String, Object> properties =
                    JsonUtil.fromJson(event, Map.class);
            MetaManager.AuthEvent authEvent = new MetaManager.AuthEvent(properties);
            if (this.authManager != null) {
                this.authManager.processEvent(authEvent);
            }
        }
    }

    public Map<String, Object> restProperties(String graphSpace) {
        Map<String, Object> map;
        map = this.metaManager.restProperties(graphSpace, this.serviceID);
        return map == null ? new HashMap<>() : map;
    }

    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceName) {
        Map<String, Object> map;
        map = this.metaManager.restProperties(graphSpace, serviceName);
        return map == null ? new HashMap<>() : map;
    }

    public Map<String, Object> restProperties(String graphSpace,
                                              Map<String, Object> properties) {
        return this.metaManager.restProperties(graphSpace,
                                               this.serviceID,
                                               properties);
    }

    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceName,
                                              Map<String, Object> properties) {
        return this.metaManager.restProperties(graphSpace,
                                               serviceName,
                                               properties);
    }

    public Map<String, Object> deleteRestProperties(String graphSpace,
                                                    String key) {
        Map<String, Object> map;
        map = this.metaManager.deleteRestProperties(graphSpace,
                                                    this.serviceID,
                                                    key);
        return map == null ? new HashMap<>() : map;
    }

    public Map<String, Object> deleteRestProperties(String graphSpace,
                                                    String serviceName,
                                                    String key) {
        Map<String, Object> map;
        map = this.metaManager.deleteRestProperties(graphSpace,
                                                    serviceName,
                                                    key);
        return map == null ? new HashMap<>() : map;
    }

    public Map<String, Object> clearRestProperties(String graphSpace,
                                                   String serviceName) {
        Map<String, Object> map;
        map = this.metaManager.clearRestProperties(graphSpace, serviceName);
        return map == null ? new HashMap<>() : map;
    }

    public String gremlinYaml(String graphSpace) {
        return this.metaManager.gremlinYaml(graphSpace, this.serviceID);
    }

    public String gremlinYaml(String graphSpace, String yaml) {
        return this.metaManager.gremlinYaml(graphSpace, this.serviceID, yaml);
    }

    public Set<String> schemaTemplates(String graphSpace) {
        return this.metaManager.schemaTemplates(graphSpace);
    }

    public SchemaTemplate schemaTemplate(String graphSpace,
                                         String schemaTemplate) {

        return this.metaManager.schemaTemplate(graphSpace, schemaTemplate);
    }

    public void createSchemaTemplate(String graphSpace,
                                     SchemaTemplate schemaTemplate) {
        checkSchemaTemplateName(schemaTemplate.name());
        this.metaManager.addSchemaTemplate(graphSpace, schemaTemplate);
    }

    public void updateSchemaTemplate(String graphSpace,
                                     SchemaTemplate schemaTemplate) {
        this.metaManager.updateSchemaTemplate(graphSpace, schemaTemplate);
    }

    public void dropSchemaTemplate(String graphSpace, String name) {
        this.metaManager.removeSchemaTemplate(graphSpace, name);
    }

    public void updateGraphNickname(String graphSpace, String graphName,
                                    String nickname) {
        HugeGraph graph = graph(graphSpace, graphName);
        String originNickname = graph.nickname();
        try {
            Map<String, Object> configs =
                    this.metaManager.getGraphConfig(graphSpace, graphName);
            configs.put("nickname", nickname);
            this.metaManager.updateGraphConfig(graphSpace, graphName, configs);
            this.metaManager.notifyGraphUpdate(graphSpace, graphName);
        } catch (Exception e) {
            LOG.warn("The graph not exist or local graph");
        }

        // migrate permissions whether the space is authenticated
        if (!originNickname.equals(nickname)) {
            boolean auth = graphSpace(graphSpace).auth();
            this.authManager.updateNicknamePermission(graphSpace, originNickname,
                                                      nickname, auth);
        }
    }

    public void graphReadMode(String graphSpace, String graphName,
                              GraphReadMode readMode) {
        try {
            Map<String, Object> configs =
                    this.metaManager.getGraphConfig(graphSpace, graphName);
            configs.put(CoreOptions.GRAPH_READ_MODE.name(), readMode);
            this.metaManager.updateGraphConfig(graphSpace, graphName, configs);
            this.metaManager.notifyGraphUpdate(graphSpace, graphName);
        } catch (Exception e) {
            LOG.warn("The graph not exist or local graph");
        }
    }

    public Map<String, Object> graphConfig(String graphSpace,
                                           String graphName) {
        return this.metaManager.getGraphConfig(graphSpace, graphName);
    }

    public String pdPeers() {
        return this.pdPeers;
    }

    public String cluster() {
        return this.cluster;
    }

    private enum PdRegisterType {
        NODE_PORT,
        DDS
    }

    private enum PdRegisterLabel {
        REGISTER_TYPE,
        GRAPHSPACE,
        SERVICE_NAME,
        SERVICE_ID,
        cores
    }
}
