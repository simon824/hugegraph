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

package org.apache.hugegraph.api.metrics;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;

import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("metrics/graphspaces/{graphspace}")
@Singleton
public class PdMetricsAPI extends API {
    private static final Logger LOG = Log.logger(PdMetricsAPI.class);

    public static void main(String[] args) {
        // TODO: 2022/12/16 待删除 替换成单测
        List<String> list =
                new PdMetricsAPI().getStartAndEndTime("01", "00", "60");
        System.out.println(list.get(0));
        System.out.println(list.get(1));
        String path = "graphspaces/DEFAULT";
        PdMetricsAPI metricsAPI2 = new PdMetricsAPI();
        Map<String, Object> map =
                metricsAPI2.calMetricsData(new PdMetaDriver("127.0.0.1:8686"), "0000", "2350",
                                           path);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    @GET
    @Timed
    @Path("/graphs/{graph}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> listGraphMetrics(@Context GraphManager manager,
                                                @PathParam("graphspace") String graphSpace,
                                                @PathParam("graph") String graph,
                                                @QueryParam("starttime") String startTime,
                                                @QueryParam("endtime") String endTime,
                                                @QueryParam("recentdata") String recentData) {
        checkParams(startTime, endTime, recentData);
        String path =
                String.join("/", "graphspaces", graphSpace, "graphs", graph);
        MetaDriver pdMetaDriver = manager.meta().metaDriver();
        if (startTime != null && endTime != null) {
            HashMap<String, Object> results = new HashMap<>();
            results.put("request_time_interval", startTime + "-" + endTime);
            results.put("time_interval", startTime + "-" + endTime);
            results.put("data", calMetricsData(pdMetaDriver, startTime, endTime, path));
            return results;
        }

        Date dNow = new Date();
        String hour = new SimpleDateFormat("HH").format(dNow);
        String min = new SimpleDateFormat("mm").format(dNow);
        List<String> list = getStartAndEndTime(hour, min, recentData);

        HashMap<String, Object> results = new HashMap<>();
        results.put("request_time", hour + min);
        results.put("time_interval", list.get(0) + "-" + list.get(1));
        results.put("data", calMetricsData(pdMetaDriver, list.get(0), list.get(1), path));
        return results;
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> listGraphSpaceMetrics(
            @Context GraphManager manager,
            @PathParam("graphspace") String graphSpace,
            @QueryParam("starttime") String startTime,
            @QueryParam("endtime") String endTime,
            @QueryParam("recentdata") String recentData) {

        checkParams(startTime, endTime, recentData);
        String path = String.join("/", "graphspaces", graphSpace);
        MetaDriver pdMetaDriver = manager.meta().metaDriver();
        if (startTime != null && endTime != null) {
            HashMap<String, Object> results = new HashMap<>();
            results.put("request_time_interval", startTime + "-" + endTime);
            results.put("time_interval", startTime + "-" + endTime);
            results.put("data", calMetricsData(pdMetaDriver, startTime, endTime, path));
            return results;
        }

        Date dNow = new Date();
        String hour = new SimpleDateFormat("HH").format(dNow);
        String min = new SimpleDateFormat("mm").format(dNow);
        List<String> list = getStartAndEndTime(hour, min, recentData);

        HashMap<String, Object> results = new HashMap<>();
        results.put("request_time", hour + min);
        results.put("time_interval", list.get(0) + "-" + list.get(1));
        results.put("data", calMetricsData(pdMetaDriver, list.get(0), list.get(1), path));
        return results;
    }

    private List<String> getStartAndEndTime(String hh, String mm,
                                            String recentData) {
        int hour = Integer.parseInt(hh);
        int min = Integer.parseInt(mm);
        ArrayList<String> list = new ArrayList<>();
        String startTime = "00";
        String endTime = "00";

        if ("60".equals(recentData)) {
            if (hour >= 1 && hour <= 23) {
                startTime = hour - 1 + "00";
                endTime = hour + "00";
            } else {
                startTime = "2300";
                endTime = "2400";
            }
        } else if ("10".equals(recentData)) {
            if (min >= 0 && min <= 9) {
                if (hour >= 1) {
                    startTime = hour - 1 + "50";
                    endTime = hour - 1 + "60";
                } else {
                    startTime = "2350";
                    endTime = "2360";
                }
            } else {
                int startMin = (int) (Math.floor(min / 10.0) - 1) * 10;
                int endMin = (int) Math.floor(min / 10.0) * 10;
                startTime = hour + String.valueOf(startMin);
                endTime = hour + String.valueOf(endMin);
            }
        }

        startTime = correctTime(startTime);
        endTime = correctTime(endTime);
        list.add(startTime);
        list.add(endTime);
        return list;
    }

    private String correctTime(String time) {
        if (time.length() < 4) {
            StringBuilder timeBuilder = new StringBuilder(time);
            for (int i = 0; i <= 4 - timeBuilder.length(); i++) {
                timeBuilder.insert(0, "0");
            }
            time = timeBuilder.toString();
        }
        return time;
    }

    private void checkParams(String startTime, String endTime, String recentData) {
        E.checkArgument(
                recentData != null || (startTime != null && endTime != null),
                "Please enter the interface request data that takes the " +
                "nearest () minutes (eg: recentdata=10)" +
                "Or enter a reasonable time period (eg:starttime=1300 " +
                "&&endtime=1400)");

        if (recentData != null) {
            E.checkArgument("10".equals(recentData) || "60".equals(recentData),
                            String.format(
                                    "Currently only metrics data for the last" +
                                    " 10 or 60 minutes are supported, but recentdata=%s",
                                    recentData));
            return;
        }
        checkTime(startTime);
        checkTime(endTime);
        E.checkArgument(
                Integer.parseInt(startTime) <= Integer.parseInt(endTime),
                String.format("Please enter the correct start time, but get " +
                              "starttime = $s > endtime = $s",
                              startTime, endTime));
    }

    private void checkTime(String time) {
        E.checkArgument(time.length() == 4, String.format(
                "The correct 24-hour time (eg:1300) is " +
                "required, but the time obtained is%s",
                time));

        int hour = Integer.parseInt(time.substring(0, 2));
        int min = Integer.parseInt(time.substring(2, 4));
        E.checkArgument(hour >= 0 && hour <= 23,
                        String.format("The correct 24-hour time (eg:1300) is " +
                                      "required, but the time obtained is%s",
                                      time));
        E.checkArgument(min >= 0 && min <= 59,
                        String.format("The correct 24-hour time (eg:1300) is " +
                                      "required, but the time obtained is%s",
                                      time));
    }

    private Map<String, Object> calMetricsData(MetaDriver pdMetaDriver, String startTime,
                                               String endTime,
                                               String path) {
        String prefix = String.join("/", "HUGEGRAPH", "METRICS", path) + "/";
        Map<String, String> kvMap =
                pdMetaDriver.scanWithPrefix(prefix);
        System.out.println("kvMap size:" + kvMap.size());
        return filterByTime(kvMap, startTime, endTime);
    }

    private Map<String, Object> filterByTime(Map<String, String> kvMap,
                                             String startTime,
                                             String endTime) {
        // kvMap.entrySet().stream().filter(a->a.getKey().substring(a.length))
        Map<String, Object> results = new HashMap<>();

        for (Map.Entry<String, String> entry : kvMap.entrySet()) {
            String key = entry.getKey();
            // 裁剪出时间信息
            // 时间的格式：HHmm（小时加分钟）
            String time = key.substring(key.length() - 4);
            String noTimeKey = key.substring(0, key.length() - 4);
            if (legalTime(startTime, endTime, time)) {
                String value = entry.getValue();
                Map<String, Object> valueMap1 =
                        JsonUtil.fromJson(value, Map.class);

                if (results.get(noTimeKey) == null) {
                    results.put(noTimeKey, valueMap1);
                } else {
                    Map<String, Object> valueMap2 =
                            (Map) results.get(noTimeKey);
                    results.put(noTimeKey, mergeData(valueMap1, valueMap2));
                }
            }
        }
        return results;
    }

    private Map<String, Object> mergeData(Map<String, Object> valueMap1,
                                          Map<String, Object> valueMap2) {
        /* 将两个map的数据进行合并
         * 最大响应时间 Maximum response time： 取两者最大
         * 平均响应时间 Average response time： 通过请求总数进行计算得到
         * 请求总数 Total Requests ： 取两者之和
         * 失败数   Failed Requests：取两者之和
         * 成功数   Success Requests： 取两者之和
         * */
        for (Map.Entry<String, Object> entry : valueMap1.entrySet()) {
            String key = entry.getKey();
            switch (key) {
                case P.TOTAL_REQUEST:
                    valueMap1.put(key, (int) entry.getValue() +
                                       (int) valueMap2.get(P.TOTAL_REQUEST));
                    break;

                case P.FAILED_REQUEST:
                    valueMap1.put(key, (int) entry.getValue() +
                                       (int) valueMap2.get(P.FAILED_REQUEST));
                    break;

                case P.SUCCESS_REQUEST:
                    valueMap1.put(key, (int) entry.getValue() +
                                       (int) valueMap2.get(P.SUCCESS_REQUEST));
                    break;
                case P.MAX_RESPONSE_TIME:
                    valueMap1.put(key,
                                  Math.max((Integer) entry.getValue(),
                                           (Integer) valueMap2.get(P.MAX_RESPONSE_TIME)));
                    break;
                case P.MEAN_RESPONSE_TIME:
                    double mean1 = (double) entry.getValue();
                    double mean2 = (double) valueMap2.get(P.MEAN_RESPONSE_TIME);
                    int total1 = (int) valueMap1.get(P.TOTAL_REQUEST);
                    int total2 = (int) valueMap2.get(P.TOTAL_REQUEST);
                    valueMap1.put(key, (mean1 * total1 + mean2 * total2) /
                                       (total1 + total2));
                    break;
                default:
                    throw new IllegalStateException("Unexpected key: " + key);
            }
        }
        return valueMap1;
    }

    private boolean legalTime(String startTime, String endTime, String time) {
        int start = Integer.parseInt(startTime);
        int end = Integer.parseInt(endTime);
        int actualTime = Integer.parseInt(time);
        return actualTime >= start && actualTime < end;
    }

    public static final class P {
        public static final String MAX_RESPONSE_TIME = "MAX_RESPONSE_TIME";
        public static final String MEAN_RESPONSE_TIME = "MEAN_RESPONSE_TIME";
        public static final String TOTAL_REQUEST = "TOTAL_REQUEST";
        public static final String FAILED_REQUEST = "FAILED_REQUEST";
        public static final String SUCCESS_REQUEST = "SUCCESS_REQUEST";
    }

}
