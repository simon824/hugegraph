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

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.algorithm.KoutTraverser;
import org.apache.hugegraph.traversal.algorithm.records.KoutRecords;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class TraverserCoreTest extends BaseCoreTest {

    /**
     * 测试拥有General类型边的时候kout的是否正常
     */
    @Test
    public void testKoutWithGeneralEdge() {
        HugeGraph graph = graph();
        initSchema();
        Vertex marko =
                graph.addVertex(T.label, "person", "name", "marko", "age",
                                29, "city", "Beijing");
        Vertex vadas = graph.addVertex(T.label, "person", "name", "vadas",
                                       "age", 27, "city", "Hongkong");
        Vertex lop = graph.addVertex(T.label, "software", "name", "lop", "lang"
                , "java", "price", 328);
        Vertex josh = graph.addVertex(T.label, "person", "name", "josh", "age",
                                      32, "city", "Beijing");

        Vertex ripple = graph.addVertex(T.label, "software", "name", "ripple"
                , "lang", "java", "price", 199);
        Vertex peter = graph.addVertex(T.label, "person", "name", "peter",
                                       "age", 29, "city", "Shanghai");
        vadas.addEdge("created", lop, "date", "20160110", "city", "Shanghai");
        vadas.addEdge("created", ripple, "date", "20160110", "city", "Beijing");
        lop.addEdge("help", peter, "date", "20160110", "city", "Beijing");
        ripple.addEdge("help", peter, "date", "20160110", "city", "Shenzhen");
        peter.addEdge("knows", josh, "date", "20160110");
        marko.addEdge("knows", vadas, "date", "20160110");
        graph.tx().commit();


        KoutTraverser traverser = new KoutTraverser(graph());
        // Get接口方法
        Collection<Id> ids = traverser.kout((Id) marko.id(), Directions.BOTH, null, 1,
                                            true, true, 1000, -1, -1);
        Assert.assertEquals(1, ids.size());
        ids = traverser.kout((Id) marko.id(), Directions.BOTH, null, 2,
                             true, true, 1000, -1, -1);
        Assert.assertEquals(2, ids.size());
        ids = traverser.kout((Id) marko.id(), Directions.BOTH, null, 3,
                             true, true, 1000, -1, -1);
        Assert.assertEquals(1, ids.size());
        ids = traverser.kout((Id) ripple.id(), Directions.BOTH, "help", 1,
                             true, true, 1000, -1, -1);
        Assert.assertEquals(1, ids.size());

        // Post接口方法
        // 测试边的过滤
        List<VEStepEntity> vSteps = new ArrayList<>();
        List<VEStepEntity> eSteps = new ArrayList<>();
        VEStepEntity eEntity = new VEStepEntity("knows", null);
        eSteps.add(eEntity);
        VESteps veSteps = new VESteps(Directions.BOTH, 1000, 1000,
                                      vSteps, eSteps);
        Steps steps = steps(graph(), veSteps);

        KoutRecords results =
                traverser.customizedKout((Id) marko.id(), steps, 1,
                                         true, -1, -1, true);
        Assert.assertEquals(1, results.ids(1000).size());

        results =
                traverser.customizedKout((Id) peter.id(), steps, 1,
                                         true, -1, -1, true);
        Assert.assertEquals(1, results.ids(1000).size());


        vSteps = new ArrayList<>();
        eSteps = new ArrayList<>();
        eEntity = new VEStepEntity("help", null);
        eSteps.add(eEntity);
        veSteps = new VESteps(Directions.BOTH, 1000, 1000,
                              vSteps, eSteps);
        steps = steps(graph(), veSteps);

        results =
                traverser.customizedKout((Id) peter.id(), steps, 1,
                                         true, -1, -1, true);
        Assert.assertEquals(2, results.ids(1000).size());

        // 测试点的过滤
        vSteps = new ArrayList<>();
        eSteps = new ArrayList<>();
        veSteps = new VESteps(Directions.BOTH, 1000, 1000,
                              vSteps, eSteps);
        steps = steps(graph(), veSteps);

        results =
                traverser.customizedKout((Id) peter.id(), steps, 1,
                                         true, -1, -1, true);
        Assert.assertEquals(3, results.ids(1000).size());

        vSteps = new ArrayList<>();
        eSteps = new ArrayList<>();
        VEStepEntity vEntity = new VEStepEntity("person", null);
        vSteps.add(vEntity);
        veSteps = new VESteps(Directions.BOTH, 1000, 1000,
                              vSteps, eSteps);
        steps = steps(graph(), veSteps);
        results =
                traverser.customizedKout((Id) marko.id(), steps, 1,
                                         true, -1, -1, true);
        Assert.assertEquals(1, results.ids(1000).size());


        vSteps = new ArrayList<>();
        eSteps = new ArrayList<>();
        vEntity = new VEStepEntity("software", null);
        VEStepEntity vEntity1 = new VEStepEntity("person", null);
        vSteps.add(vEntity);
        vSteps.add(vEntity1);
        veSteps = new VESteps(Directions.BOTH, 1000, 1000,
                              vSteps, eSteps);
        steps = steps(graph(), veSteps);
        results = traverser.customizedKout((Id) peter.id(), steps, 1,
                                           false, -1, -1, true);
        Assert.assertEquals(3, results.ids(1000).size());

    }


    protected Steps steps(HugeGraph graph, VESteps steps) {
        Map<String, Map<String, Object>> vSteps = new HashMap<>(4);
        if (steps.vSteps != null) {
            for (VEStepEntity vStep : steps.vSteps) {
                vSteps.put(vStep.label, vStep.properties);
            }
        }

        Map<String, Map<String, Object>> eSteps = new HashMap<>(4);
        if (steps.eSteps != null) {
            for (VEStepEntity eStep : steps.eSteps) {
                eSteps.put(eStep.label, eStep.properties);
            }
        }

        return new Steps(graph, steps.direction, eSteps, vSteps,
                         steps.maxDegree, steps.skipDegree);
    }

    private void initSchema() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").asText().ifNotExist().create();
        graph.schema().propertyKey("age").asInt().ifNotExist().create();
        graph.schema().propertyKey("city").asText().ifNotExist().create();
        graph.schema().propertyKey("lang").asText().ifNotExist().create();
        graph.schema().propertyKey("date").asText().ifNotExist().create();
        graph.schema().propertyKey("price").asInt().ifNotExist().create();

        graph.schema().vertexLabel("person")
             .properties("name", "age", "city")
             .primaryKeys("name").ifNotExist().create();

        graph.schema().vertexLabel("software")
             .properties("name", "lang", "price").primaryKeys("name")
             .ifNotExist().create();

        graph.schema().edgeLabel("knows")
             .sourceLabel("person").targetLabel("person")
             .properties("date").ifNotExist().create();

        graph.schema().edgeLabel("created")
             // .sourceLabel("person").targetLabel("software")
             .asGeneral()
             .properties("date", "city").ifNotExist()
             .create();

        graph.schema().edgeLabel("help")
             // .sourceLabel("software").targetLabel("person")
             .asGeneral()
             .properties("date", "city").ifNotExist()
             .create();
    }

    @Test
    public void testKneighborWithGeneralEdge() {
        // pass
    }

    protected static class VEStepEntity {

        public String label;
        public Map<String, Object> properties;

        public VEStepEntity(String label, Map<String, Object> properties) {
            this.label = label;
            this.properties = properties;
        }

        @Override
        public String toString() {
            return String.format("VEStepEntity{label=%s,properties=%s}",
                                 this.label, this.properties);
        }
    }


    protected static class VESteps {

        public Directions direction;
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        public long skipDegree = 0L;
        public List<VEStepEntity> vSteps;
        public List<VEStepEntity> eSteps;

        public VESteps(Directions direction, long maxDegree, long skipDegree,
                       List<VEStepEntity> vSteps, List<VEStepEntity> eSteps) {
            this.direction = direction;
            this.maxDegree = maxDegree;
            this.skipDegree = skipDegree;
            this.vSteps = vSteps;
            this.eSteps = eSteps;
        }

        @Override
        public String toString() {
            return String.format("Steps{direction=%s,maxDegree=%s," +
                                 "skipDegree=%s,vSteps=%s,eSteps=%s}",
                                 this.direction, this.maxDegree,
                                 this.skipDegree, this.vSteps, this.eSteps);
        }
    }
}
