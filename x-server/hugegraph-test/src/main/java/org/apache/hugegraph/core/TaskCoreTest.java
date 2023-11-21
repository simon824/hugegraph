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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.api.job.GremlinAPI.GremlinRequest;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.job.CypherJob;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.EphemeralJobBuilder;
import org.apache.hugegraph.job.GremlinJob;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

public class TaskCoreTest extends BaseCoreTest {

    private static void sleepAWhile() {
        sleepAWhile(100);
    }

    private static void sleepAWhile(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Before
    @Override
    public void setup() {
        super.setup();

        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        Iterator<HugeTask<Object>> iter = scheduler.tasks(null, -1, null);
        while (iter.hasNext()) {
            scheduler.delete(iter.next().id(), true);
        }
    }

    @Test
    public void testTask() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();


        TaskCallable<Object> callable = new SleepCallable<>();

        Id id = IdGenerator.of(88888);
        HugeTask<?> task = new HugeTask<>(id, null, callable);
        task.type("test");
        task.name("test-task");

        scheduler.schedule(task);
        Assert.assertEquals(id, task.id());
        Assert.assertFalse(task.completed());


        // DistributedTaskScheduler删除为异步操作，未执行完的任务可以执行cancel操作
        // Assert.assertThrows(IllegalArgumentException.class, () -> {
        //     scheduler.delete(id);
        // }, e -> {
        //     Assert.assertContains("Can't delete incomplete task '88888'",
        //                           e.getMessage());
        // });

        sleepAWhile(15000);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(id, task.id());
        Assert.assertEquals("test-task", task.name());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());

        Assert.assertEquals("test-task", scheduler.task(id).name());
        Assert.assertEquals("test-task", scheduler.tasks(List.of(id))
                                                  .next().name());

        Iterator<HugeTask<Object>> iter = scheduler.tasks(ImmutableList.of(id));
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("test-task", iter.next().name());
        Assert.assertFalse(iter.hasNext());

        scheduler.delete(id);
        sleepAWhile(10000);
        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.task(id);
        });
    }

    @Test
    public void testTaskWithFailure() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        TaskCallable<Integer> callable = new TaskCallable<>() {
            @Override
            public Integer call() throws Exception {
                sleepAWhile();
                return 125;
            }

            @Override
            protected void done() {
                scheduler.save(this.task());
            }
        };

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeTask<>(null, null, callable);
        }, e -> {
            Assert.assertContains("Task id can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id id = IdGenerator.of("88888");
            new HugeTask<>(id, null, callable);
        }, e -> {
            Assert.assertContains("Invalid task id type, it must be number",
                                  e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            Id id = IdGenerator.of(88888);
            new HugeTask<>(id, null, null);
        });

        Assert.assertThrows(RuntimeException.class, () -> {
            Id id = IdGenerator.of(88888);
            HugeTask<?> task2 = new HugeTask<>(id, null, callable);
            task2.name("test-task");
            scheduler.schedule(task2);
        }, e -> {
            Assert.assertContains("Task type can't be null", e.getMessage());
        });

        Assert.assertThrows(RuntimeException.class, () -> {
            Id id = IdGenerator.of(88888);
            HugeTask<?> task2 = new HugeTask<>(id, null, callable);
            task2.type("test");
            scheduler.schedule(task2);
        }, e -> {
            Assert.assertContains("Task name can't be null", e.getMessage());
        });
    }

    @SneakyThrows
    @Test
    public void testEphemeralJob() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        EphemeralJobBuilder<Object> builder = EphemeralJobBuilder.of(graph);
        builder.name("test-job-ephemeral")
               .job(new EphemeralJob<>() {
                   @Override
                   public String type() {
                       return "test";
                   }

                   @Override
                   public Object execute() throws Exception {
                       sleepAWhile();
                       return ImmutableMap.of("k1", 13579, "k2", "24680");
                   }
               });

        HugeTask<Object> task = builder.schedule();
        Assert.assertEquals("test-job-ephemeral", task.name());
        Assert.assertEquals("test", task.type());
        Assert.assertFalse(task.completed());

        // 等待任务执行完成
        task.get();

        HugeTask<?> task2 = task;
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("{\"k1\":13579,\"k2\":\"24680\"}", task.result());

        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        Assert.assertEquals("{\"k1\":13579,\"k2\":\"24680\"}", task2.result());

        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.task(task.id());
        });
    }

    //    TODO: fix and remove annotation
//    @Test
    public void testGremlinJob() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        GremlinRequest request = new GremlinRequest();
        request.gremlin("sleep(100); 3 + 5");

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input(request.toJson())
               .job(new GremlinJob());

        HugeTask<Object> task = builder.schedule();
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertFalse(task.completed());
        Assert.assertNull(task.result());

        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("8", task.result());

        task = scheduler.task(task.id());
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("8", task.result());
    }

    //    TODO: fix and remove annotation
//    @Test
    public void testGremlinJobWithScript() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        String script = "schema=graph.schema();"
                        + "schema.propertyKey('name').asText().ifNotExist().create();"
                        + "schema.propertyKey('age').asInt().ifNotExist().create();"
                        + "schema.propertyKey('lang').asText().ifNotExist().create();"
                        + "schema.propertyKey('date').asDate().ifNotExist().create();"
                        + "schema.propertyKey('price').asInt().ifNotExist().create();"
                        +
                        "schema.vertexLabel('person1').properties('name','age').ifNotExist()" +
                        ".create();"
                        +
                        "schema.vertexLabel('person2').properties('name','age').ifNotExist()" +
                        ".create();"
                        + "schema.edgeLabel('knows').link('person1','person2')"
                        + ".properties('date').ifNotExist().create();"
                        + "for(int i = 0; i < 1000; i++) {"
                        + "  p1=graph.addVertex(T.label,'person1','name','p1-'+i,'age',29);"
                        + "  p2=graph.addVertex(T.label,'person2','name','p2-'+i,'age',27);"
                        + "  p1.addEdge('knows',p2,'date','2016-01-10');"
                        + "}";

        HugeTask<Object> task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 20);
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[]", task.result());

        script = "g.V().count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[2000]", task.result());

        script = "g.V().hasLabel('person1').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 15);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.V().hasLabel('person2').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 15);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.E().count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 15);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.E().hasLabel('knows').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 15);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());
    }

    //    TODO: fix and remove annotation
//    @Test
    public void testGremlinJobWithSerializedResults() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        String script = "schema=graph.schema();"
                        + "schema.propertyKey('name').asText().ifNotExist().create();"
                        +
                        "schema.vertexLabel('char').useCustomizeNumberId().properties('name')" +
                        ".ifNotExist().create();"
                        +
                        "schema.edgeLabel('next').link('char','char').properties('name')" +
                        ".ifNotExist().create();"
                        + "g.addV('char').property(id,1).property('name','A').as('a')"
                        + " .addV('char').property(id,2).property('name','B').as('b')"
                        + " .addV('char').property(id,3).property('name','C').as('c')"
                        + " .addV('char').property(id,4).property('name','D').as('d')"
                        + " .addV('char').property(id,5).property('name','E').as('e')"
                        + " .addV('char').property(id,6).property('name','F').as('f')"
                        + " .addE('next').from('a').to('b').property('name','ab')"
                        + " .addE('next').from('b').to('c').property('name','bc')"
                        + " .addE('next').from('b').to('d').property('name','bd')"
                        + " .addE('next').from('c').to('d').property('name','cd')"
                        + " .addE('next').from('c').to('e').property('name','ce')"
                        + " .addE('next').from('d').to('e').property('name','de')"
                        + " .addE('next').from('e').to('f').property('name','ef')"
                        + " .addE('next').from('f').to('d').property('name','fd')"
                        + " .iterate();"
                        + "g.tx().commit(); g.E().count();";

        HugeTask<Object> task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[8]", task.result());

        Id edgeLabelId = graph.schema().getEdgeLabel("next").id();
        Id vertexLabelId = graph.schema().getVertexLabel("char").id();

        script = "g.V(1).outE().inV().path()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        String expected = String.format("[{\"labels\":[[],[],[]],\"objects\":["
                                        +
                                        "{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                                        "\"properties\":{\"name\":\"A\"}},"
                                        + "{\"id\":\"L1>%s>%s>%s>>L2\",\"label\":\"next\","
                                        + "\"type\":\"edge\",\"outV\":1,\"outVLabel\":\"char\","
                                        + "\"inV\":2,\"inVLabel\":\"char\","
                                        + "\"properties\":{\"name\":\"ab\"}},"
                                        +
                                        "{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                                        "\"properties\":{\"name\":\"B\"}}"
                                        + "]}]", edgeLabelId, vertexLabelId, vertexLabelId);
        Assert.assertEquals(expected, task.result());

        script = "g.V(1).out().out().path()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        expected = "[{\"labels\":[[],[],[]],\"objects\":["
                   +
                   "{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"A\"}},"
                   +
                   "{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"B\"}},"
                   +
                   "{\"id\":3,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"C\"}}]},"
                   + "{\"labels\":[[],[],[]],\"objects\":["
                   +
                   "{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"A\"}},"
                   +
                   "{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"B\"}},"
                   + "{\"id\":4,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"D\"}}]}]";
        Assert.assertEquals(expected, task.result());

        script = "g.V(1).outE().inV().tree()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        expected = String.format("[[{\"key\":{\"id\":1,"
                                 + "\"label\":\"char\",\"type\":\"vertex\","
                                 + "\"properties\":{\"name\":\"A\"}},"
                                 + "\"value\":["
                                 + "{\"key\":{\"id\":\"L1>%s>%s>%s>>L2\",\"label\":\"next\","
                                 + "\"type\":\"edge\",\"outV\":1,\"outVLabel\":\"char\","
                                 + "\"inV\":2,\"inVLabel\":\"char\","
                                 + "\"properties\":{\"name\":\"ab\"}},"
                                 + "\"value\":[{\"key\":{\"id\":2,"
                                 + "\"label\":\"char\",\"type\":\"vertex\","
                                 + "\"properties\":{\"name\":\"B\"}},\"value\":[]}]}]}]]",
                                 edgeLabelId, vertexLabelId, vertexLabelId);
        Assert.assertEquals(expected, task.result());

        script = "g.V(1).out().out().tree()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        expected =
                "[[{\"key\":{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                "\"properties\":{\"name\":\"A\"}},"
                +
                "\"value\":[{\"key\":{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                "\"properties\":{\"name\":\"B\"}},"
                + "\"value\":["
                +
                "{\"key\":{\"id\":3,\"label\":\"char\",\"type\":\"vertex\"," +
                "\"properties\":{\"name\":\"C\"}},\"value\":[]},"
                +
                "{\"key\":{\"id\":4,\"label\":\"char\",\"type\":\"vertex\"," +
                "\"properties\":{\"name\":\"D\"}},\"value\":[]}]}]}]]";
        Assert.assertEquals(expected, task.result());
    }

    @Test
    public void testGremlinJobWithFailure() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("")
               .job(new GremlinJob());
        HugeTask<Object> task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Can't read json", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        task = scheduler.task(task.id());
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("The input can't be null", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid gremlin value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":8}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid gremlin value '8'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid bindings value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid bindings value ''", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid language value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, \"language\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid language value '{}'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, \"language\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid aliases value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, " +
                      "\"language\":\"test\", \"aliases\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("test is not an available GremlinScriptEngine",
                              task.result());
    }

    @Test
    public void testGremlinJobWithError() throws TimeoutException {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            JobBuilder.of(graph)
                      .job(new GremlinJob())
                      .schedule();
        }, e -> {
            Assert.assertContains("Job name can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            JobBuilder.of(graph)
                      .name("test-job-gremlin")
                      .schedule();
        }, e -> {
            Assert.assertContains("Job callable can't be null", e.getMessage());
        });

        // // Test failure task with big input
        // int length = 8 * 1024 * 1024;
        // Random random = new Random();
        // StringBuilder sb = new StringBuilder(length);
        // for (int i = 0; i < length; i++) {
        //     sb.append("node:").append(random.nextInt(1000));
        // }
        // String bigInput = sb.toString();
        // Assert.assertThrows(HugeException.class, () -> {
        //     runGremlinJob(bigInput);
        // }, e -> {
        //     Assert.assertContains("Task input size", e.getMessage());
        //     Assert.assertContains("exceeded limit 16777216 bytes",
        //                           e.getMessage());
        // });
    }

    //    TODO: fix and remove annotation
//    @Test
    public void testGremlinJobAndCancel() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        HugeTask<Object> task = runGremlinJob("Thread.sleep(1000 * 10);");

        sleepAWhile();
        task = scheduler.task(task.id());
        scheduler.cancel(task);

        sleepAWhile();
        task = scheduler.task(task.id());
        Assert.assertEquals(TaskStatus.CANCELLING, task.status());

        task = scheduler.waitUntilTaskCompleted(task.id(), 15);
        Assert.assertEquals(TaskStatus.CANCELLED, task.status());
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertTrue(task.result(), task.result() == null ||
                                         task.result().endsWith("InterruptedException"));

        // Cancel success task
        HugeTask<Object> task2 = runGremlinJob("1+2");
        task2 = scheduler.waitUntilTaskCompleted(task2.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        scheduler.cancel(task2);
        task2 = scheduler.task(task2.id());
        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        Assert.assertEquals("3", task2.result());

        // Cancel failure task with big results (job size exceeded limit)
        String bigList = "def l=[]; for (i in 1..20000001) l.add(i); l;";
        HugeTask<Object> task3 = runGremlinJob(bigList);
        task3 = scheduler.waitUntilTaskCompleted(task3.id(), 12);
        Assert.assertEquals(TaskStatus.FAILED, task3.status());
        scheduler.cancel(task3);
        task3 = scheduler.task(task3.id());
        Assert.assertEquals(TaskStatus.FAILED, task3.status());
        Assert.assertContains(
                "org.apache.hugegraph.exception.LimitExceedException: Job results size " +
                "20000001 has exceeded the max limit 10000000",
                task3.result());

        // Cancel failure task with big results (task exceeded limit 16M)
        String bigResults = "def random = new Random(); def rs=[];" +
                            "for (i in 0..4) {" +
                            "  def len = 1024 * 1024;" +
                            "  def item = new StringBuilder(len);" +
                            "  for (j in 0..len) { " +
                            "    item.append(\"node:\"); " +
                            "    item.append((char) random.nextInt(256)); " +
                            "    item.append(\",\"); " +
                            "  };" +
                            "  rs.add(item);" +
                            "};" +
                            "rs;";
        HugeTask<Object> task4 = runGremlinJob(bigResults);
        task4 = scheduler.waitUntilTaskCompleted(task4.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task4.status());
        scheduler.cancel(task4);
        task4 = scheduler.task(task4.id());
        Assert.assertEquals(TaskStatus.FAILED, task4.status());
        Assert.assertContains("LimitExceedException: Task result size",
                              task4.result());
        Assert.assertContains("exceeded limit 16777216 bytes",
                              task4.result());
    }

    //    TODO: fix and remove annotation
//    @Test
    public void testGremlinJobAndDelete() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        HugeTask<Object> task = runGremlinJob("Thread.sleep(1000 * 10);");

        sleepAWhile();
        task = scheduler.task(task.id());
        HugeTask<Object> finalTask = task;

        // 分布式任务调度器，删除任务不会触发异常
        // Assert.assertThrows(IllegalArgumentException.class, () ->
        //         scheduler.delete(finalTask.id()));

        scheduler.delete(task.id(), true);
        sleepAWhile();
        Assert.assertThrows(NotFoundException.class, () ->
                scheduler.task(finalTask.id()));

        // Cancel success task
        HugeTask<Object> task2 = runGremlinJob("1+2");
        task2 = scheduler.waitUntilTaskCompleted(task2.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());

        // 分布式任务调度器，非强制删除任务不会立即删除
        // scheduler.delete(task2.id());
        // HugeTask<Object> finalTask1 = task2;
        // Assert.assertThrows(NotFoundException.class, () ->
        //         scheduler.task(finalTask1.id()));

        // Cancel failure task with big results (job size exceeded limit)
        String bigList = "def l=[]; for (i in 1..20000001) l.add(i); l;";
        HugeTask<Object> task3 = runGremlinJob(bigList);
        task3 = scheduler.waitUntilTaskCompleted(task3.id(), 12);
        // 任务可能会重新调起
        // Assert.assertEquals(TaskStatus.FAILED, task3.status());

        // 分布式任务调度器，非强制删除任务不会立即删除
        // scheduler.delete(task3.id());
        // HugeTask<Object> finalTask2 = task3;
        // Assert.assertThrows(NotFoundException.class, () ->
        //         scheduler.task(finalTask2.id()));
    }

    private HugeTask<Object> runGremlinJob(String gremlin) {
        HugeGraph graph = graph();

        GremlinRequest request = new GremlinRequest();
        request.gremlin(gremlin);

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-gremlin-job")
               .input(request.toJson())
               .job(new GremlinJob());

        return builder.schedule();
    }

    @Test
    public void testCypherJob() {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        HugeTask<Object> task = runCypherJob("MATCH (n) RETURN n LIMIT 1");

        sleepAWhile();
        task = scheduler.task(task.id());
        scheduler.delete(task.id(), true);
        sleepAWhile();
    }

    @Test
    public void testTaskScheduler() {
        TaskManager.instance().serviceGraphSpace("other");
        HugeGraph g = Mockito.mock(StandardHugeGraph.class);
        HugeGraphParams gp = Mockito.mock(HugeGraphParams.class);
        Mockito.when(gp.schedulerType()).thenReturn("local");
        Mockito.when(gp.graph()).thenReturn(g);

        // 图服务挂在到非DEFAULT，DEFAULT图空间图，不生成scheduler
        Mockito.when(g.graphSpace()).thenReturn("DEFAULT");
        TaskManager.instance().addScheduler(gp);
        Assert.assertNull(TaskManager.instance().getScheduler(gp));

        // 图服务挂在到非DEFAULT，非同名图空间图，不生成scheduler
        Mockito.when(g.graphSpace()).thenReturn("other1");
        TaskManager.instance().addScheduler(gp);
        Assert.assertNull(TaskManager.instance().getScheduler(gp));

        // 图服务挂在到非DEFAULT，同图空间的图，生成scheduler
        Mockito.when(g.graphSpace()).thenReturn("other");
        TaskManager.instance().addScheduler(gp);
        Assert.assertNotNull(TaskManager.instance().getScheduler(gp));

        // 图服务挂在到DEFAULT，DEFAULT图空间图，生成scheduler
        TaskManager.instance().serviceGraphSpace("DEFAULT");
        Mockito.when(g.graphSpace()).thenReturn("DEFAULT");
        Assert.assertNotNull(TaskManager.instance().getScheduler(gp));

        // 图服务挂在到DEFAULT，非DEFAULT图控的图，生成scheduler
        Mockito.when(g.graphSpace()).thenReturn("other");
        Assert.assertNotNull(TaskManager.instance().getScheduler(gp));
    }

    private HugeTask<Object> runCypherJob(String cypher) {
        HugeGraph graph = graph();

        GremlinRequest request = new GremlinRequest();
        request.gremlin(cypher);
        request.aliase("graphspace", graph.graphSpace());
        request.aliase("graph", graph.name());
        request.aliase("authorization", "Basic YWRtaW46YWRtaW4=");


        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-gremlin-job")
               .input(request.toJson())
               .job(new CypherJob());

        return builder.schedule();
    }

    public static class SleepCallable<V> extends TaskCallable<V> {

        public SleepCallable() {
            // pass
        }

        @Override
        public V call() throws Exception {
            Thread.sleep(1000);
            return null;
        }

        @Override
        public void done() {
            this.graph().taskScheduler().save(this.task());
        }
    }
}
