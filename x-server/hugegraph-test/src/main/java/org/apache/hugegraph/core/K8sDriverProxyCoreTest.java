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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.computer.driver.DefaultJobState;
import org.apache.hugegraph.computer.driver.JobObserver;
import org.apache.hugegraph.computer.driver.JobStatus;
import org.apache.hugegraph.k8s.K8sDriverProxy;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.util.ExecutorUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class K8sDriverProxyCoreTest extends BaseCoreTest {

    private static final Map<String, String> ALGORITHM_PARAMS =
            new HashMap<>() {{
                put("page-rank", "org.apache.hugegraph.computer.algorithm.centrality" +
                                 ".pagerank.PageRankParams");
                put("degree-centrality", "org.apache.hugegraph.computer.algorithm" +
                                         ".centrality.degree.DegreeCentralityParams");
                put("wcc", "org.apache.hugegraph.computer.algorithm.community.wcc" +
                           ".WccParams");
                put("triangle-count", "org.apache.hugegraph.computer.algorithm" +
                                      ".community.trianglecount.TriangleCountParams");
                put("rings", "org.apache.hugegraph.computer.algorithm.path.rings" +
                             ".RingsDetectionParams");
                put("rings-with-filter", "org.apache.hugegraph.computer.algorithm" +
                                         ".path.rings.filter.RingsDetectionWithFilterParams");
                put("betweenness-centrality", "org.apache.hugegraph.computer" +
                                              ".algorithm.centrality.betweenness" +
                                              ".BetweennessCentralityParams");
                put("closeness-centrality", "org.apache.hugegraph.computer.algorithm" +
                                            ".centrality.closeness.ClosenessCentralityParams");
                put("lpa", "org.apache.hugegraph.computer.algorithm.community.lpa" +
                           ".LpaParams");
                put("links", "org.apache.hugegraph.computer.algorithm.path.links" +
                             ".LinksParams");
                put("kcore", "org.apache.hugegraph.computer.algorithm.community" +
                             ".kcore.KCoreParams");
                put("louvain", "org.apache.hugegraph.computer.algorithm.community" +
                               ".louvain.LouvainParams");
                put("clustering-coefficient", "org.apache.hugegraph.computer" +
                                              ".algorithm.community.cc" +
                                              ".ClusteringCoefficientParams");
            }};
    private static final String NAMESPACE = "hugegraph-computer-system";
    private static final String KUBE_CONFIG = "conf/kube.kubeconfig";
    private static final String ENABLE_INTERNAL_ALGORITHM = "true";
    private static final String INTERNAL_ALGORITHM_IMAGE_URL = "hugegraph/" +
                                                               "hugegraph-computer-based" +
                                                               "-algorithm:beta1";
    private static final String PARAMS_CLASS = "org.apache.hugegraph.computer." +
                                               "algorithm.rank.pagerank." +
                                               "PageRankParams";
    private static final String INTERNAL_ALGORITHM = "[page-rank, " +
                                                     "degree-centrality, wcc, triangle-count, " +
                                                     "rings, " +
                                                     "rings-with-filter, betweenness-centrality, " +
                                                     "closeness-centrality, lpa, links, kcore, " +
                                                     "louvain, clustering-coefficient]";

    private static final String COMPUTER = "page-rank";

    private static ExecutorService POOL;

    @BeforeClass
    public static void init() {
        POOL = ExecutorUtil.newFixedThreadPool(1, "k8s-driver-test-pool");
    }

    @Before
    @Override
    public void setup() {
        super.setup();

        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();
        Iterator<HugeTask<Object>> iter = scheduler.tasks(null, -1, null);
        while (iter.hasNext()) {
            scheduler.delete(iter.next().id());
        }

        try {
            K8sDriverProxy.setConfig(ENABLE_INTERNAL_ALGORITHM,
                                     INTERNAL_ALGORITHM_IMAGE_URL,
                                     INTERNAL_ALGORITHM,
                                     ALGORITHM_PARAMS);
        } catch (IOException e) {
            // ignore
        }
    }

    @Test
    public void testK8sTask() throws TimeoutException {
        Map<String, String> params = new HashMap<>();
        params.put("k8s.worker_instances", "2");
        String namespace = NAMESPACE;
        K8sDriverProxy k8sDriverProxy = new K8sDriverProxy("2", COMPUTER);
        String jobId = k8sDriverProxy.getK8sDriver(namespace)
                                     .submitJob(COMPUTER, params);

        JobObserver jobObserver = Mockito.mock(JobObserver.class);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            k8sDriverProxy.getK8sDriver(namespace)
                          .waitJobAsync(jobId, params, jobObserver);
        }, POOL);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState));

        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.SUCCEEDED);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
               .onJobStateChanged(Mockito.eq(jobState2));

        future.getNow(null);
        k8sDriverProxy.close(namespace);
    }
}
