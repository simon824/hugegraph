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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBOptions;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public class MultiGraphsTest {

    protected static final Logger LOG = Log.logger(MultiGraphsTest.class);
    private static final String NAME48 =
            "g12345678901234567890123456789012345678901234567";

    private static List<HugeGraph> openGraphs(String... graphNames) {
        List<HugeGraph> graphs = new ArrayList<>(graphNames.length);
        for (String graphName : graphNames) {
            Configuration config = buildConfig(graphName);
            graphs.add((HugeGraph) GraphFactory.open(config));
        }
        return graphs;
    }

    private static void destoryGraphs(List<HugeGraph> graphs) {
        for (HugeGraph graph : graphs) {
            try {
                graph.close();
            } catch (Exception e) {
                Assert.fail(e.toString());
            }
        }
    }

    private static HugeGraph openGraphWithBackend(String graph, String backend,
                                                  String serializer,
                                                  String... configs) {
        Configuration config = buildConfig(graph);
        config.setProperty(CoreOptions.BACKEND.name(), backend);
        config.setProperty(CoreOptions.SERIALIZER.name(), serializer);
        for (int i = 0; i < configs.length; ) {
            config.setProperty(configs[i++], configs[i++]);
        }
        return ((HugeGraph) GraphFactory.open(config));
    }

    private static Configuration buildConfig(String graphName) {
        PropertiesConfiguration conf = Utils.getConf();
        Configuration config = new BaseConfiguration();
        for (Iterator<String> keys = conf.getKeys(); keys.hasNext(); ) {
            String key = keys.next();
            config.setProperty(key, conf.getProperty(key));
        }
        // ((BaseConfiguration) config).setDelimiterParsingDisabled(true);

        config.setProperty(CoreOptions.STORE.name(), graphName);
        String dataPath = config.getString(RocksDBOptions.DATA_PATH.name());
        config.setProperty(RocksDBOptions.DATA_PATH.name(),
                           Paths.get(dataPath, graphName).toString());
        String walPath = config.getString(RocksDBOptions.WAL_PATH.name());
        config.setProperty(RocksDBOptions.WAL_PATH.name(),
                           Paths.get(walPath, graphName).toString());

        return config;
    }

    @Test
    public void testCreateMultiGraphs() {
        List<HugeGraph> graphs = openGraphs("g_1", NAME48);
        for (HugeGraph graph : graphs) {
            graph.initBackend();
            graph.clearBackend();
        }
        destoryGraphs(graphs);
    }

    @Test
    public void testCopySchemaWithMultiGraphs() {
        List<HugeGraph> graphs = openGraphs("schema_g1", "schema_g2");
        for (HugeGraph graph : graphs) {
            graph.initBackend();
        }
        HugeGraph g1 = graphs.get(0);
        g1.serverStarted();
        HugeGraph g2 = graphs.get(1);
        g2.serverStarted();

        SchemaManager schema = g1.schema();

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("weight").asDouble().valueList().create();
        schema.propertyKey("born").asDate().ifNotExist().create();
        schema.propertyKey("time").asDate().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("id", "name", "age", "city", "weight", "born")
              .primaryKeys("id").create();
        schema.vertexLabel("person2")
              .properties("id", "name", "age", "city")
              .primaryKeys("id").create();
        schema.edgeLabel("friend")
              .sourceLabel("person")
              .targetLabel("person")
              .properties("time").create();

        schema.indexLabel("personByName").onV("person").secondary()
              .by("name").create();
        schema.indexLabel("personByCity").onV("person").search()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        schema.indexLabel("friendByTime").onE("friend").range()
              .by("time").create();

        Assert.assertFalse(g2.existsPropertyKey("id"));
        Assert.assertFalse(g2.existsPropertyKey("name"));
        Assert.assertFalse(g2.existsPropertyKey("age"));
        Assert.assertFalse(g2.existsPropertyKey("city"));
        Assert.assertFalse(g2.existsPropertyKey("weight"));
        Assert.assertFalse(g2.existsPropertyKey("born"));
        Assert.assertFalse(g2.existsPropertyKey("time"));

        Assert.assertFalse(g2.existsVertexLabel("person"));
        Assert.assertFalse(g2.existsVertexLabel("person2"));
        Assert.assertFalse(g2.existsEdgeLabel("friend"));

        Assert.assertFalse(g2.existsIndexLabel("personByName"));
        Assert.assertFalse(g2.existsIndexLabel("personByCity"));
        Assert.assertFalse(g2.existsIndexLabel("personByAge"));
        Assert.assertFalse(g2.existsIndexLabel("friendByTime"));

        // Copy schema from g1 to g2
        g2.schema().copyFrom(g1.schema());

        Assert.assertTrue(g2.existsPropertyKey("id"));
        Assert.assertTrue(g2.existsPropertyKey("name"));
        Assert.assertTrue(g2.existsPropertyKey("age"));
        Assert.assertTrue(g2.existsPropertyKey("city"));
        Assert.assertTrue(g2.existsPropertyKey("weight"));
        Assert.assertTrue(g2.existsPropertyKey("born"));
        Assert.assertTrue(g2.existsPropertyKey("time"));

        Assert.assertTrue(g2.existsVertexLabel("person"));
        Assert.assertTrue(g2.existsVertexLabel("person2"));
        Assert.assertTrue(g2.existsEdgeLabel("friend"));

        Assert.assertTrue(g2.existsIndexLabel("personByName"));
        Assert.assertTrue(g2.existsIndexLabel("personByCity"));
        Assert.assertTrue(g2.existsIndexLabel("personByAge"));
        Assert.assertTrue(g2.existsIndexLabel("friendByTime"));

        for (PropertyKey copied : g2.schema().getPropertyKeys()) {
            PropertyKey origin = g1.schema().getPropertyKey(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));
        }
        for (VertexLabel copied : schema.getVertexLabels()) {
            VertexLabel origin = g1.schema().getVertexLabel(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));
        }
        for (EdgeLabel copied : schema.getEdgeLabels()) {
            EdgeLabel origin = g1.schema().getEdgeLabel(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));

        }
        for (IndexLabel copied : schema.getIndexLabels()) {
            IndexLabel origin = g1.schema().getIndexLabel(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));
        }

        // Copy schema again from g1 to g2 (ignore identical content)
        g2.schema().copyFrom(g1.schema());

        for (PropertyKey copied : g2.schema().getPropertyKeys()) {
            PropertyKey origin = g1.schema().getPropertyKey(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));
        }
        for (VertexLabel copied : schema.getVertexLabels()) {
            VertexLabel origin = g1.schema().getVertexLabel(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));
        }
        for (EdgeLabel copied : schema.getEdgeLabels()) {
            EdgeLabel origin = g1.schema().getEdgeLabel(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));

        }
        for (IndexLabel copied : schema.getIndexLabels()) {
            IndexLabel origin = g1.schema().getIndexLabel(copied.name());
            Assert.assertTrue(origin.hasSameContent(copied));
        }

        for (HugeGraph graph : graphs) {
            graph.clearBackend();
        }
        destoryGraphs(graphs);
    }

    @Test
    public void testCopySchemaWithMultiGraphsWithConflict() {
        List<HugeGraph> graphs = openGraphs("schema_g1", "schema_g2");
        for (HugeGraph graph : graphs) {
            graph.initBackend();
        }
        HugeGraph g1 = graphs.get(0);
        HugeGraph g2 = graphs.get(1);
        g1.serverStarted();
        g2.serverStarted();

        g1.schema().propertyKey("id").asInt().create();
        g2.schema().propertyKey("id").asText().create();

        Assert.assertThrows(ExistedException.class, () -> {
            g2.schema().copyFrom(g1.schema());
        }, e -> {
            Assert.assertEquals("The property key 'id' has existed",
                                e.getMessage());
        });

        for (HugeGraph graph : graphs) {
            graph.clearBackend();
        }
        destoryGraphs(graphs);
    }

    @Test
    public void testCreateGraphsWithInvalidNames() {
        List<String> invalidNames = ImmutableList.of(
                "", " ", " g", "g 1", " .", ". .",
                "@", "$", "%", "^", "&", "*", "(", ")",
                "_", "+", "`", "-", "=", "{", "}", "|",
                "[", "]", "\"", "<", "?", ";", "'", "~",
                ",", ".", "/", "\\",
                "~g", "g~", "g'",
                "_1", "_a",
                "1a", "123",
                NAME48 + "8");
        for (String name : invalidNames) {
            Assert.assertThrows(RuntimeException.class, () -> openGraphs(name));
        }
    }

    @Test
    public void testCreateGraphsWithSameName() {
        List<HugeGraph> graphs = openGraphs("g", "g", "G");
        HugeGraph g1 = graphs.get(0);
        HugeGraph g2 = graphs.get(1);
        HugeGraph g3 = graphs.get(2);

        g1.initBackend();
        Assert.assertTrue(g1.backendStoreSystemInfo().exists());
        Assert.assertTrue(g2.backendStoreSystemInfo().exists());
        Assert.assertTrue(g3.backendStoreSystemInfo().exists());

        g2.initBackend(); // no error
        g3.initBackend();

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> g2.vertexLabel("node"));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> g3.vertexLabel("node"));
        g1.schema().vertexLabel("node").useCustomizeNumberId()
          .ifNotExist().create();
        g2.vertexLabel("node");
        g3.vertexLabel("node");

        g1.addVertex(T.label, "node", T.id, 1);
        g1.tx().commit();
        Iterator<Vertex> vertices = g2.vertices(1);
        Assert.assertTrue(vertices.hasNext());
        Vertex vertex = vertices.next();
        Assert.assertFalse(vertices.hasNext());
        Assert.assertEquals(IdGenerator.of(1), vertex.id());

        vertices = g3.vertices(1);
        Assert.assertTrue(vertices.hasNext());
        vertex = vertices.next();
        Assert.assertFalse(vertices.hasNext());
        Assert.assertEquals(IdGenerator.of(1), vertex.id());

        try {
            // clearBackend操作会等待task执行完成
            // 在执行任务查询过程中，如果图未初始化则会抛出异常
            g1.clearBackend();
            g2.clearBackend();
            g3.clearBackend();
        } catch (Throwable t) {
            // pass
            LOG.warn("Error when clearBackend", t);
        }

        destoryGraphs(ImmutableList.of(g1, g2, g3));
    }

    @Test
    public void testCreateGraphsWithMultiDisksForRocksDB() {
        HugeGraph g1 = openGraphWithBackend(
                "g1", "rocksdb", "binary",
                "rocksdb.data_disks",
                "[g/secondary_index:rocksdb-index1," +
                " g/range_int_index:rocksdb-index1," +
                " g/range_long_index:rocksdb-index2]");
        g1.initBackend();
        g1.clearBackend();

        final HugeGraph[] g2 = new HugeGraph[1];
        Assert.assertThrows(BackendException.class, () -> {
            g2[0] = openGraphWithBackend("g2", "rocksdb", "binary",
                                         "rocksdb.data_disks",
                                         "[g/range_int_index:rocksdb-index1]");
            g2[0].initBackend();
        }, e -> {
            Throwable root = HugeException.rootCause(e);
            Assert.assertInstanceOf(RocksDBException.class, root);
            Assert.assertContains("lock hold by current process",
                                  root.getMessage());
            Assert.assertContains("No locks available",
                                  root.getMessage());

            // close task scheduler
            g2[0].taskScheduler().close();
            ((StandardHugeGraph) g2[0]).clearSchedulerAndLock();
            ;
        });

        destoryGraphs(ImmutableList.of(g1));
    }
}