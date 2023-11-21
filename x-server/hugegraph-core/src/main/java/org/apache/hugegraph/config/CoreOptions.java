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

package org.apache.hugegraph.config;

import static org.apache.hugegraph.backend.tx.GraphTransaction.COMMIT_BATCH;
import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeDouble;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.Bytes;

public class CoreOptions extends OptionHolder {

    public static final int CPUS = Runtime.getRuntime().availableProcessors();
    public static final ConfigOption<String> GRAPH_SPACE =
            new ConfigOption<>(
                    "graphspace",
                    "The graph space name.",
                    null,
                    "DEFAULT"
            );
    public static final ConfigOption<String> BACKEND =
            new ConfigOption<>(
                    "backend",
                    "The data store type.",
                    disallowEmpty(),
                    "hstore"
            );
    public static final ConfigOption<String> STORE =
            new ConfigOption<>(
                    "store",
                    "The database name.",
                    disallowEmpty(),
                    "hugegraph"
            );
    public static final ConfigOption<String> STORE_GRAPH =
            new ConfigOption<>(
                    "store.graph",
                    "The graph table name, which store vertex, edge and property.",
                    disallowEmpty(),
                    "g"
            );
    public static final ConfigOption<String> GRAPH_READ_MODE =
            new ConfigOption<>(
                    "graph.read_mode",
                    "The graph read mode, which could be ALL | OLTP_ONLY | OLAP_ONLY.",
                    disallowEmpty(),
                    "OLTP_ONLY"
            );
    public static final ConfigOption<String> SERIALIZER =
            new ConfigOption<>(
                    "serializer",
                    "The serializer for backend store, like: text/binary.",
                    disallowEmpty(),
                    "binary"
            );
    public static final ConfigOption<Integer> RATE_LIMIT_WRITE =
            new ConfigOption<>(
                    "rate_limit.write",
                    "The max rate(items/s) to add/update/delete vertices/edges.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );
    public static final ConfigOption<Integer> RATE_LIMIT_READ =
            new ConfigOption<>(
                    "rate_limit.read",
                    "The max rate(times/s) to execute query of vertices/edges.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );
    public static final ConfigOption<Long> TASK_WAIT_TIMEOUT =
            new ConfigOption<>(
                    "task.wait_timeout",
                    "Timeout in seconds for waiting for the task to " +
                    "complete, such as when truncating or clearing the " +
                    "backend.",
                    rangeInt(0L, Long.MAX_VALUE),
                    10L
            );
    public static final ConfigOption<Long> TASK_INPUT_SIZE_LIMIT =
            new ConfigOption<>(
                    "task.input_size_limit",
                    "The job input size limit in bytes.",
                    rangeInt(0L, Bytes.GB),
                    16 * Bytes.MB
            );
    public static final ConfigOption<Long> TASK_RESULT_SIZE_LIMIT =
            new ConfigOption<>(
                    "task.result_size_limit",
                    "The job result size limit in bytes.",
                    rangeInt(0L, Bytes.GB),
                    16 * Bytes.MB
            );
    public static final ConfigOption<Integer> TASK_TTL_DELETE_BATCH =
            new ConfigOption<>(
                    "task.ttl_delete_batch",
                    "The batch size used to delete expired data.",
                    rangeInt(1, 500),
                    1
            );
    public static final ConfigOption<String> SCHEDULER_TYPE =
            new ConfigOption<>(
                    "task.scheduler_type",
                    "use which type of scheduler" +
                    "used in distribution system",
                    allowValues("local", "distributed"),
                    "distributed"
            );
    public static final ConfigOption<Boolean> TASK_SYNC_DELETION =
            new ConfigOption<>(
                    "task.sync_deletion",
                    "Whether to delete schema or expired data synchronously.",
                    disallowEmpty(),
                    false
            );
    public static final ConfigOption<Integer> TASK_RETRY =
            new ConfigOption<>(
                    "task.retry",
                    "task retry times",
                    rangeInt(0, 3),
                    0
            );
    public static final ConfigOption<Long> STORE_CONN_DETECT_INTERVAL =
            new ConfigOption<>(
                    "store.connection_detect_interval",
                    "The interval in seconds for detecting connections, " +
                    "if the idle time of a connection exceeds this value, " +
                    "detect it and reconnect if needed before using, " +
                    "value 0 means detecting every time.",
                    rangeInt(0L, Long.MAX_VALUE),
                    600L
            );
    public static final ConfigOption<String> VERTEX_DEFAULT_LABEL =
            new ConfigOption<>(
                    "vertex.default_label",
                    "The default vertex label.",
                    disallowEmpty(),
                    "vertex"
            );
    public static final ConfigOption<Boolean> VERTEX_CHECK_CUSTOMIZED_ID_EXIST =
            new ConfigOption<>(
                    "vertex.check_customized_id_exist",
                    "Whether to check the vertices exist for those using " +
                    "customized id strategy.",
                    disallowEmpty(),
                    false
            );
    public static final ConfigOption<Boolean> VERTEX_REMOVE_LEFT_INDEX =
            new ConfigOption<>(
                    "vertex.remove_left_index_at_overwrite",
                    "Whether remove left index at overwrite.",
                    disallowEmpty(),
                    false
            );
    public static final ConfigOption<Boolean> VERTEX_ADJACENT_VERTEX_EXIST =
            new ConfigOption<>(
                    "vertex.check_adjacent_vertex_exist",
                    "Whether to check the adjacent vertices of edges exist.",
                    disallowEmpty(),
                    false
            );
    public static final ConfigOption<Boolean> VERTEX_ADJACENT_VERTEX_LAZY =
            new ConfigOption<>(
                    "vertex.lazy_load_adjacent_vertex",
                    "Whether to lazy load adjacent vertices of edges.",
                    disallowEmpty(),
                    true
            );
    public static final ConfigOption<Integer> VERTEX_PART_EDGE_COMMIT_SIZE =
            new ConfigOption<>(
                    "vertex.part_edge_commit_size",
                    "Whether to enable the mode to commit part of edges of " +
                    "vertex, enabled if commit size > 0, 0 meas disabled.",
                    rangeInt(0, (int) HugeTraverser.DEF_CAPACITY),
                    5000
            );
    public static final ConfigOption<Integer> VERTEX_TX_CAPACITY =
            new ConfigOption<>(
                    "vertex.tx_capacity",
                    "The max size(items) of vertices(uncommitted) in " +
                    "transaction.",
                    rangeInt(COMMIT_BATCH, 1000000),
                    10000
            );
    public static final ConfigOption<Integer> EDGE_TX_CAPACITY =
            new ConfigOption<>(
                    "edge.tx_capacity",
                    "The max size(items) of edges(uncommitted) in " +
                    "transaction.",
                    rangeInt(COMMIT_BATCH, 1000000),
                    10000
            );
    public static final ConfigOption<Boolean> QUERY_IGNORE_INVALID_DATA =
            new ConfigOption<>(
                    "query.ignore_invalid_data",
                    "Whether to ignore invalid data of vertex or edge.",
                    disallowEmpty(),
                    true
            );
    public static final ConfigOption<Boolean> QUERY_OPTIMIZE_AGGR_BY_INDEX =
            new ConfigOption<>(
                    "query.optimize_aggregate_by_index",
                    "Whether to optimize aggregate query(like count) by index.",
                    disallowEmpty(),
                    false
            );
    public static final ConfigOption<Integer> QUERY_BATCH_SIZE =
            new ConfigOption<>(
                    "query.batch_size",
                    "The size of each batch when querying by batch.",
                    rangeInt(1, (int) HugeTraverser.DEF_CAPACITY),
                    1000
            );
    public static final ConfigOption<Integer> QUERY_PAGE_SIZE =
            new ConfigOption<>(
                    "query.page_size",
                    "The size of each page when querying by paging.",
                    rangeInt(1, (int) HugeTraverser.DEF_CAPACITY),
                    500
            );
    public static final ConfigOption<Integer> QUERY_INDEX_INTERSECT_THRESHOLD =
            new ConfigOption<>(
                    "query.index_intersect_threshold",
                    "The maximum number of intermediate results to " +
                    "intersect indexes when querying by multiple single " +
                    "index properties.",
                    rangeInt(1, (int) HugeTraverser.DEF_CAPACITY),
                    1000
            );
    public static final ConfigOption<String> SCHEMA_INIT_TEMPLATE =
            new ConfigOption<>(
                    "schema.init_template",
                    "The template schema used to init graph",
                    null,
                    ""
            );
    /**
     * The schema name rule:
     * 1、Not allowed end with spaces
     * 2、Not allowed start with '~'
     */
    public static final ConfigOption<String> SCHEMA_ILLEGAL_NAME_REGEX =
            new ConfigOption<>(
                    "schema.illegal_name_regex",
                    "The regex specified the illegal format for schema name.",
                    disallowEmpty(),
                    ".*\\s+$|~.*"
            );
    public static final ConfigOption<Long> SCHEMA_CACHE_CAPACITY =
            new ConfigOption<>(
                    "schema.cache_capacity",
                    "The max cache size(items) of schema cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    10000L
            );
    public static final ConfigOption<String> VERTEX_CACHE_TYPE =
            new ConfigOption<>(
                    "vertex.cache_type",
                    "The type of vertex cache, allowed values are [l1, l2].",
                    allowValues("l1", "l2"),
                    "l2"
            );
    public static final ConfigOption<Long> VERTEX_CACHE_CAPACITY =
            new ConfigOption<>(
                    "vertex.cache_capacity",
                    "The max cache size(items) of vertex cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (5000 * 1000L) // 5 million
            );
    public static final ConfigOption<Integer> VERTEX_CACHE_EXPIRE =
            new ConfigOption<>(
                    "vertex.cache_expire",
                    "The expiration time in seconds of vertex cache.",
                    rangeInt(0, Integer.MAX_VALUE),
                    (60 * 60) // 1 hour
            );
    public static final ConfigOption<String> EDGE_CACHE_TYPE =
            new ConfigOption<>(
                    "edge.cache_type",
                    "The type of edge cache, allowed values are [l1, l2].",
                    allowValues("l1", "l2"),
                    "l2"
            );
    public static final ConfigOption<Long> EDGE_CACHE_CAPACITY =
            new ConfigOption<>(
                    "edge.cache_capacity",
                    "The max cache size(items) of edge cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    ((long) 5000 * 1000)
            );
    public static final ConfigOption<Integer> EDGE_CACHE_EXPIRE =
            new ConfigOption<>(
                    "edge.cache_expire",
                    "The expiration time in seconds of edge cache.",
                    rangeInt(0, Integer.MAX_VALUE),
                (60 * 60) // 1 hour
            );
    public static final ConfigOption<Long> SNOWFLAKE_WORKER_ID =
            new ConfigOption<>(
                    "snowflake.worker_id",
                    "The worker id of snowflake id generator.",
                    disallowEmpty(),
                    0L
            );
    public static final ConfigOption<Long> SNOWFLAKE_DATACENTER_ID =
            new ConfigOption<>(
                    "snowflake.datecenter_id",
                    "The datacenter id of snowflake id generator.",
                    disallowEmpty(),
                    0L
            );
    public static final ConfigOption<Boolean> SNOWFLAKE_FORCE_STRING =
            new ConfigOption<>(
                    "snowflake.force_string",
                    "Whether to force the snowflake long id to be a string.",
                    disallowEmpty(),
                    false
            );
    public static final ConfigOption<String> TEXT_ANALYZER =
            new ConfigOption<>(
                    "search.text_analyzer",
                    "Choose a text analyzer for searching the " +
                    "vertex/edge properties, available type are " +
                    "[word, ansj, hanlp, smartcn, jieba, jcseg, " +
                    "mmseg4j, ikanalyzer].",
                    disallowEmpty(),
                    "ikanalyzer"
            );
    public static final ConfigOption<String> TEXT_ANALYZER_MODE =
            new ConfigOption<>(
                    "search.text_analyzer_mode",
                    "Specify the mode for the text analyzer, " +
                    "the available mode of analyzer are " +
                    "{word: [MaximumMatching, ReverseMaximumMatching, " +
                    "MinimumMatching, ReverseMinimumMatching, " +
                    "BidirectionalMaximumMatching, " +
                    "BidirectionalMinimumMatching, " +
                    "BidirectionalMaximumMinimumMatching, " +
                    "FullSegmentation, MinimalWordCount, " +
                    "MaxNgramScore, PureEnglish], " +
                    "ansj: [BaseAnalysis, IndexAnalysis, ToAnalysis, " +
                    "NlpAnalysis], " +
                    "hanlp: [standard, nlp, index, nShort, shortest, speed], " +
                    "smartcn: [], " +
                    "jieba: [SEARCH, INDEX], " +
                    "jcseg: [Simple, Complex], " +
                    "mmseg4j: [Simple, Complex, MaxWord], " +
                    "ikanalyzer: [smart, max_word]" +
                    "}.",
                    disallowEmpty(),
                    "smart"
            );
    public static final ConfigOption<String> COMPUTER_CONFIG =
            new ConfigOption<>(
                    "computer.config",
                    "The config file path of computer job.",
                    disallowEmpty(),
                    "./conf/computer.yaml"
            );
    public static final ConfigOption<String> K8S_OPERATOR_TEMPLATE =
            new ConfigOption<>(
                    "k8s.operator_template",
                    "the path of operator container template.",
                    disallowEmpty(),
                    "./conf/operator-template.yaml"
            );
    public static final ConfigOption<String> K8S_QUOTA_TEMPLATE =
            new ConfigOption<>(
                    "k8s.quota_template",
                    "the path of resource quota template.",
                    disallowEmpty(),
                    "./conf/resource-quota-template.yaml"
            );
    public static final ConfigOption<Integer> OLTP_CONCURRENT_THREADS =
            new ConfigOption<>(
                    "oltp.concurrent_threads",
                    "Thread number to concurrently execute oltp algorithm.",
                    rangeInt(1, Math.max(10, CoreOptions.CPUS * 2)),
                    Math.max(10, CoreOptions.CPUS / 2)
            );
    public static final ConfigOption<Integer> OLTP_CONCURRENT_DEPTH =
            new ConfigOption<>(
                    "oltp.concurrent_depth",
                    "The min depth to enable concurrent oltp algorithm.",
                    rangeInt(0, 65535),
                    10
            );
    public static final ConfigConvOption<String, CollectionType> OLTP_COLLECTION_TYPE =
            new ConfigConvOption<>(
                    "oltp.collection_type",
                    "The implementation type of collections " +
                    "used in oltp algorithm.",
                    allowValues("JCF", "EC", "FU"),
                    CollectionType::valueOf,
                    "EC"
            );
    public static final ConfigOption<Integer> OLTP_QUERY_BATCH_SIZE =
            new ConfigOption<>(
                    "oltp.query_batch_size",
                    "The size of each batch when executing oltp algorithm.",
                    rangeInt(0, 65535),
                    10000
            );
    public static final ConfigOption<Double> OLTP_QUERY_BATCH_AVG_DEGREE_RATIO =
            new ConfigOption<>(
                    "oltp.query_batch_avg_degree_ratio",
                    "The ratio of exponential approximation for " +
                    "average degree of iterator when executing oltp algorithm.",
                    rangeDouble(0D, 1D),
                    0.95D
            );
    public static final ConfigOption<Long> OLTP_QUERY_BATCH_EXPECT_DEGREE =
            new ConfigOption<>(
                    "oltp.query_batch_expect_degree",
                    "The expect sum of degree in each batch when executing oltp algorithm.",
                    rangeInt(10 * 1000L, 1000 * 1000 * 1000L),
                    100 * 1000 * 1000L
            );
    public static final ConfigOption<Boolean> VIRTUAL_GRAPH_ENABLE =
            new ConfigOption<>(
                    "graph.virtual_graph_enable",
                    "Whether to enable the Virtual Graph.",
                    false
            );
    public static final ConfigOption<Integer> VIRTUAL_GRAPH_BATCH_BUFFER_SIZE =
            new ConfigOption<>(
                    "graph.virtual_graph_batch_buffer_size",
                    "The size of buffer for batch load in Virtual Graph.",
                    rangeInt(0, 65535),
                    0
            );
    public static final ConfigOption<Integer> VIRTUAL_GRAPH_BATCH_SIZE =
            new ConfigOption<>(
                    "graph.virtual_graph_batch_size",
                    "The size of each batch when batch loading in Virtual Graph.",
                    rangeInt(0, 65535),
                    50
            );
    public static final ConfigOption<Integer> VIRTUAL_GRAPH_BATCH_TIME_MS =
            new ConfigOption<>(
                    "graph.virtual_graph_batch_time_ms",
                    "Interval in milliseconds to batch load queries in buffer of Virtual Graph.",
                    rangeInt(0, Integer.MAX_VALUE),
                    100
            );
    public static final ConfigOption<Integer> VIRTUAL_GRAPH_VERTEX_INIT_CAPACITY =
            new ConfigOption<>(
                    "graph.virtual_graph_vertex_init_capacity",
                    "The minimum number of vertices cached in Virtual Graph.",
                    rangeInt(0, Integer.MAX_VALUE),
                    (1000 * 1000)
            );
    public static final ConfigOption<Long> VIRTUAL_GRAPH_VERTEX_MAX_SIZE =
            new ConfigOption<>(
                    "graph.virtual_graph_vertex_max_size",
                    "The maximum number of vertices cached in Virtual Graph.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (100 * 1000 * 1000L)
            );
    public static final ConfigOption<Long> VIRTUAL_GRAPH_VERTEX_EXPIRE =
            new ConfigOption<>(
                    "graph.virtual_graph_vertex_expire",
                    "The expiration time in seconds of vertex cache in Virtual Graph.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (60 * 100L)
            );
    public static final ConfigOption<Integer> VIRTUAL_GRAPH_EDGE_INIT_CAPACITY =
            new ConfigOption<>(
                    "graph.virtual_graph_edge_init_capacity",
                    "The minimum number of edges cached in Virtual Graph.",
                    rangeInt(0, Integer.MAX_VALUE),
                    10000
            );
    public static final ConfigOption<Long> VIRTUAL_GRAPH_EDGE_MAX_SIZE =
            new ConfigOption<>(
                    "graph.virtual_graph_edge_max_size",
                    "The maximum number of edges cached in Virtual Graph.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (1000 * 1000L)
            );
    public static final ConfigOption<Long> VIRTUAL_GRAPH_EDGE_EXPIRE =
            new ConfigOption<>(
                    "graph.virtual_graph_edge_expire",
                    "The expiration time in seconds of edge cache in Virtual Graph.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (60 * 100L)
            );
    public static final ConfigOption<Integer> VIRTUAL_GRAPH_BATCHER_TASK_THREADS =
            new ConfigOption<>(
                    "graph.virtual_graph_batcher_task_threads",
                    "The task threads of virtual graph batcher.",
                    rangeInt(1, Math.max(4, CoreOptions.CPUS * 2)),
                    Math.max(4, CoreOptions.CPUS / 2)
            );
    private static volatile CoreOptions instance;

    private CoreOptions() {
        super();
    }

    public static synchronized CoreOptions instance() {
        if (instance == null) {
            instance = new CoreOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
