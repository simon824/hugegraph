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

package org.apache.hugegraph.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_METRICS;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_FAILED_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_RESPONSE_TIME_HISTOGRAM;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_SUCCESS_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_TOTAL_COUNTER;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSortedMap;

public class ServerReporter extends ScheduledReporter {
    private static final Logger LOG = Log.logger(ServerReporter.class);
    /* MINUTE = 1Min
     * */
    private static final long MINUTE = 60 * 1000L;
    private static volatile ServerReporter instance = null;
    private SortedMap<String, Gauge<?>> gauges;
    private SortedMap<String, Counter> counters;
    private SortedMap<String, Histogram> histograms;
    private SortedMap<String, Meter> meters;
    private SortedMap<String, Timer> timers;
    private int ttl;
    private PdMetaDriver pdMetaDriver;
    private String cluster;
    private boolean metricsDataToPd;

    private ServerReporter(MetricRegistry registry, PdMetaDriver pdMetaDriver,
                           int ttl, String cluster, boolean metricsDataToPd) {
        this(registry, SECONDS, MILLISECONDS, MetricFilter.ALL);
        this.ttl = ttl;
        this.pdMetaDriver = pdMetaDriver;
        this.cluster = cluster;
        this.metricsDataToPd = metricsDataToPd;
    }

    private ServerReporter(MetricRegistry registry, TimeUnit rateUnit,
                           TimeUnit durationUnit, MetricFilter filter) {
        super(registry, "server-reporter", filter, rateUnit, durationUnit);
        this.gauges = ImmutableSortedMap.of();
        this.counters = ImmutableSortedMap.of();
        this.histograms = ImmutableSortedMap.of();
        this.meters = ImmutableSortedMap.of();
        this.timers = ImmutableSortedMap.of();
    }

    public static synchronized ServerReporter instance(MetricRegistry registry,
                                                       PdMetaDriver pdMetaDriver,
                                                       int ttl, String cluster,
                                                       boolean metricsDataToPd) {
        if (instance == null) {
            synchronized (ServerReporter.class) {
                if (instance == null) {
                    instance = new ServerReporter(registry, pdMetaDriver, ttl,
                                                  cluster, metricsDataToPd);
                }
            }
        }
        return instance;
    }

    public static ServerReporter instance() {
        E.checkNotNull(instance, "Must instantiate ServerReporter before get");
        return instance;
    }

    public static void main(String[] args) {
        Date dNow = new Date();
        System.out.println(dNow);
        SimpleDateFormat ft = new SimpleDateFormat("HHmm");
        System.out.println("当前时间为: " + ft.format(dNow));

        System.out.println(true);
        HashMap<Object, Object> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("s", 23);
        System.out.println(JsonUtil.toJson(objectObjectHashMap));

        String s = "123/456/333455";
        String[] split = s.split("/");
        String lastWord = split[split.length - 1];
        System.out.println(s.substring(0, s.length() - lastWord.length()));
        System.out.println(Arrays.toString(split));
    }

    public Map<String, Timer> timers() {
        return Collections.unmodifiableMap(this.timers);
    }

    public Map<String, Gauge<?>> gauges() {
        return Collections.unmodifiableMap(this.gauges);
    }

    public Map<String, Counter> counters() {
        return Collections.unmodifiableMap(this.counters);
    }

    public Map<String, Histogram> histograms() {
        return Collections.unmodifiableMap(this.histograms);
    }

    public Map<String, Meter> meters() {
        return Collections.unmodifiableMap(this.meters);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        this.gauges = (SortedMap) gauges;
        this.counters = counters;
        this.histograms = histograms;
        this.meters = meters;
        this.timers = timers;

        if (metricsDataToPd) {
            reportMetricsDataToPd();
        }
    }

    private void reportMetricsDataToPd() {
        // 在这里将数据写入pd中
        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("HHmm");

        for (Map.Entry<String, Histogram> entry : this.histograms.entrySet()) {
            // k = path/method/responseTimeHistogram
            String k = entry.getKey();
            String[] split = k.split("/");
            String lastWord = split[split.length - 1];
            if (!lastWord.equals(METRICS_PATH_RESPONSE_TIME_HISTOGRAM)) {
                // 原有的metrics不上报
                continue;
            }
            // metricsName = path/method
            String metricsName =
                    k.substring(0, k.length() - lastWord.length() - 1);

            Counter totalCounter = this.counters.get(
                    join(metricsName, METRICS_PATH_TOTAL_COUNTER));
            Counter failedCounter = this.counters.get(
                    join(metricsName, METRICS_PATH_FAILED_COUNTER));
            Counter successCounter = this.counters.get(
                    join(metricsName, METRICS_PATH_SUCCESS_COUNTER));

            String key = metricsKey(metricsName, ft.format(dNow));
            Histogram histogram = entry.getValue();
            HashMap<String, Object> metricsMap = new HashMap<>();
            metricsMap.put(MetricsKeys.MAX_RESPONSE_TIME.name(),
                           histogram.getSnapshot().getMax());
            metricsMap.put(MetricsKeys.MEAN_RESPONSE_TIME.name(),
                           histogram.getSnapshot().getMean());

            metricsMap.put(MetricsKeys.TOTAL_REQUEST.name(),
                           totalCounter.getCount());

            if (failedCounter == null) {
                metricsMap.put(MetricsKeys.FAILED_REQUEST.name(), 0);
            } else {
                metricsMap.put(MetricsKeys.FAILED_REQUEST.name(),
                               failedCounter.getCount());
            }

            if (successCounter == null) {
                metricsMap.put(MetricsKeys.SUCCESS_REQUEST.name(), 0);
            } else {
                metricsMap.put(MetricsKeys.SUCCESS_REQUEST.name(),
                               successCounter.getCount());
            }

            this.pdMetaDriver.putTTL(key, JsonUtil.toJson(metricsMap),
                                     ttl * MINUTE);

            // 完成数据上报到pd后，删除register容器里的已有的counter 和 histogram
            MetricsUtil.removeMetrics(join(metricsName,
                                           METRICS_PATH_RESPONSE_TIME_HISTOGRAM));
            MetricsUtil.removeMetrics(join(metricsName,
                                           METRICS_PATH_SUCCESS_COUNTER));
            MetricsUtil.removeMetrics(join(metricsName,
                                           METRICS_PATH_FAILED_COUNTER));
            MetricsUtil.removeMetrics(join(metricsName,
                                           METRICS_PATH_TOTAL_COUNTER));
        }
        LOG.info("{} Complete metrics data reporting pd, TTL of data: {} " +
                 "min", dNow, ttl);
    }

    private String metricsKey(String key, String time) {
        return String.join("/",
                           META_PATH_HUGEGRAPH,
                           META_PATH_METRICS,
                           key,
                           this.cluster,
                           time);
    }

    private String join(String path1, String path2) {
        return String.join("/", path1, path2);
    }

}
