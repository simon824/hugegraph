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

package org.apache.hugegraph.unit.cache;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.VirtualGraphTransaction;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

public class VirtualGraphTransactionTest extends BaseUnitTest {

    private static final Logger LOG = Log.logger(VirtualGraphTransactionTest.class);

    private VirtualGraphTransaction vgraph;
    private HugeGraphParams params;

    @BeforeClass
    public static void initTaskManager() {
        TaskManager.instance(4);
    }

    @Before
    public void setup() {
        HugeGraph graph = HugeFactory.open(FakeObjects.newConfig());
        this.params = Whitebox.getInternalState(graph, "params");
        this.vgraph = new VirtualGraphTransaction(this.params,
                                                  this.params.loadGraphStore());
        initSchema();
    }

    @After
    public void teardown() throws Exception {
        this.vgraph().graph().clearBackend();
        this.vgraph().graph().close();
    }

    private VirtualGraphTransaction vgraph() {
        Assert.assertNotNull(this.vgraph);
        return this.vgraph;
    }

    private void initSchema() {
        SchemaManager schema = this.vgraph().graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asLong().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("score").asInt().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("amount").asFloat().create();
        schema.propertyKey("message").asText().create();
        schema.propertyKey("place").asText().create();
        schema.propertyKey("tool").asText().create();
        schema.propertyKey("reason").asText().create();
        schema.propertyKey("hurt").asBoolean().create();
        schema.propertyKey("arrested").asBoolean().create();
        schema.propertyKey("date").asDate().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .idStrategy(IdStrategy.CUSTOMIZE_NUMBER)
              .checkExist(false)
              .create();

        LOG.debug("===============  edgeLabel  ================");

        schema.edgeLabel("transfer")
              .properties("id", "amount", "timestamp", "message")
              .nullableKeys("message")
              .multiTimes().sortKeys("id")
              .link("person", "person")
              .enableLabelIndex(false)
              .create();

    }

    @Test
    public void testEventClearVertex() throws Exception {
        VirtualGraphTransaction vgraph = this.vgraph();

        Vertex louise = vgraph.addVertex(T.label, "person", T.id, 1, "name", "Louise",
                                         "city", "Beijing", "age", 21);
        Vertex sean = vgraph.addVertex(T.label, "person", T.id, 2, "name", "Sean",
                                       "city", "Beijing", "age", 23);
        vgraph.commit();

        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2, this.params.vGraph().getVertexSize());

        this.params.graphEventHub().notify(Events.CACHE, "clear", null).get();

        Assert.assertEquals(0, this.params.vGraph().getVertexSize());

        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertEquals(1, this.params.vGraph().getVertexSize());
        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2, this.params.vGraph().getVertexSize());
    }

    @Test
    public void testEventInvalidVertex() throws Exception {
        VirtualGraphTransaction vgraph = this.vgraph();

        Vertex louise = vgraph.addVertex(T.label, "person", T.id, 1, "name", "Louise",
                                         "city", "Beijing", "age", 21);
        Vertex sean = vgraph.addVertex(T.label, "person", T.id, 2, "name", "Sean",
                                       "city", "Beijing", "age", 23);
        vgraph.commit();

        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2,
                            this.params.vGraph().getVertexSize());

        this.params.graphEventHub().notify(Events.CACHE, "invalid",
                                           HugeType.VERTEX, IdGenerator.of(1))
                   .get();

        Assert.assertEquals(1, this.params.vGraph().getVertexSize());
        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(1, this.params.vGraph().getVertexSize());
        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertEquals(2, this.params.vGraph().getVertexSize());
    }

    @Test
    public void testEventClearEdge() throws Exception {
        VirtualGraphTransaction vgraph = this.vgraph();

        Vertex louise = vgraph.addVertex(T.label, "person", T.id, 1, "name", "Louise",
                                         "city", "Beijing", "age", 21);
        Vertex sean = vgraph.addVertex(T.label, "person", T.id, 2, "name", "Sean",
                                       "city", "Beijing", "age", 23);

        Edge edge1 = louise.addEdge("transfer", sean, "id", 1,
                                    "amount", 500.00F, "timestamp", 1L,
                                    "message", "Happy birthday!");
        Edge edge2 = louise.addEdge("transfer", sean, "id", 2,
                                    "amount", -1234.56F, "timestamp", -100L,
                                    "message", "Happy birthday!");
        vgraph.commit();

        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(vgraph.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2, this.params.vGraph().getVertexSize());

        Iterator<Edge> iteratorEdge1 = vgraph.queryEdgesByVertex(IdGenerator.of(1));
        Assert.assertTrue(iteratorEdge1.hasNext());

        List<Edge> edgeListByVertex = IteratorUtils.toList(iteratorEdge1);
        Iterator<Edge> iteratorEdgeByEId = vgraph.queryEdges(edge1.id(), edge2.id());
        Assert.assertArrayEquals(edgeListByVertex.toArray(),
                                 IteratorUtils.toArray(iteratorEdgeByEId));
        Assert.assertEquals(2, this.params.vGraph().getEdgeSize());

        this.params.graphEventHub().notify(Events.CACHE, "clear", null).get();
        Assert.assertEquals(0, this.params.vGraph().getVertexSize());
        Assert.assertEquals(0, this.params.vGraph().getEdgeSize());

        Assert.assertEquals(vgraph.queryEdge(edge1.id()), edge1);
        Assert.assertEquals(1, this.params.vGraph().getEdgeSize());
        Assert.assertEquals(vgraph.queryEdge(edge2.id()), edge2);
        Assert.assertEquals(2, this.params.vGraph().getEdgeSize());
    }

    @Test
    public void testEventInvalidEdge() throws Exception {
        VirtualGraphTransaction vgraph = this.vgraph();

        Vertex louise = vgraph.addVertex(T.label, "person", T.id, 1, "name", "Louise",
                                         "city", "Beijing", "age", 21);
        Vertex sean = vgraph.addVertex(T.label, "person", T.id, 2, "name", "Sean",
                                       "city", "Beijing", "age", 23);

        Edge edge1 = louise.addEdge("transfer", sean, "id", 1,
                                    "amount", 500.00F, "timestamp", 1L,
                                    "message", "Happy birthday!");
        Edge edge2 = louise.addEdge("transfer", sean, "id", 2,
                                    "amount", -1234.56F, "timestamp", -100L,
                                    "message", "Happy birthday!");
        vgraph.commit();

        Assert.assertTrue(vgraph.queryEdges(edge1.id(), edge2.id()).hasNext());
        Assert.assertEquals(2,
                            this.params.vGraph().getEdgeSize());

        this.params.graphEventHub().notify(Events.CACHE, "invalid",
                                           HugeType.EDGE, edge1.id())
                   .get();

        Assert.assertEquals(1, this.params.vGraph().getEdgeSize());
        Assert.assertEquals(vgraph.queryEdge(edge2.id()), edge2);
        Assert.assertEquals(1, this.params.vGraph().getEdgeSize());
        Assert.assertEquals(vgraph.queryEdge(edge1.id()), edge1);
        Assert.assertEquals(2, this.params.vGraph().getEdgeSize());
    }
}
