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

package org.apache.hugegraph.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.job.ComputerDisJob;
import org.apache.hugegraph.job.ComputerJob;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.GremlinJob;
import org.apache.hugegraph.job.VermeerJob;
import org.apache.hugegraph.job.schema.SchemaCallable;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.SerialEnum;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;

import com.google.common.base.Strings;

public class HugeTask<V> extends FutureTask<V> {

    private static final Logger LOG = Log.logger(HugeTask.class);

    private static final float DECOMPRESS_RATIO = 10.0F;
    private final TaskCallable<V> callable;
    private final Id id;
    private final Id parent;
    private transient TaskScheduler scheduler = null;
    private String type;
    private String name;
    private Set<Id> dependencies;
    private String description;
    private String context;
    private Date create;
    private Id server;
    private int load;
    private TaskPriority priority = TaskPriority.NORMAL;

    private String graphSpace;
    private String graphName;

    private volatile TaskStatus status;
    private volatile int progress;
    private volatile Date update;
    private volatile int retries;
    private volatile String input;
    private volatile String result;

    public HugeTask(Id id, Id parent, String callable, String input) {
        this(id, parent, TaskCallable.fromClass(callable));
        this.input(input);
    }

    public HugeTask(Id id, Id parent, TaskCallable<V> callable) {
        super(callable);

        E.checkArgumentNotNull(id, "Task id can't be null");
        E.checkArgument(id.number(), "Invalid task id type, it must be number");

        this.callable = callable;
        this.type = null;
        this.name = null;
        this.id = id;
        this.parent = parent;
        this.dependencies = null;
        this.description = null;
        this.context = null;
        this.status = TaskStatus.NEW;
        this.progress = 0;
        this.create = new Date();
        this.update = null;
        this.retries = 0;
        this.input = null;
        this.result = null;
        this.server = null;
        this.load = 1;
    }

    public static <V> HugeTask<V> fromVertex(Vertex vertex) {
        String callableName = vertex.value(P.CALLABLE);
        TaskCallable<V> callable;
        try {
            callable = TaskCallable.fromClass(callableName);
        } catch (Exception e) {
            callable = TaskCallable.empty(e);
        }

        HugeTask<V> task = new HugeTask<>((Id) vertex.id(), null, callable);
        for (Iterator<VertexProperty<Object>> iter = vertex.properties();
             iter.hasNext(); ) {
            VertexProperty<Object> prop = iter.next();
            task.property(prop.key(), prop.value());
        }
        return task;
    }

    private static <V> Collector<V, ?, Set<V>> toOrderSet() {
        return Collectors.toCollection(InsertionOrderUtil::newSet);
    }

    public Id id() {
        return this.id;
    }

    public Id parent() {
        return this.parent;
    }

    public Set<Id> dependencies() {
        return Collections.unmodifiableSet(this.dependencies);
    }

    public void depends(Id id) {
        E.checkState(this.status == TaskStatus.NEW,
                     "Can't add dependency in status '%s'", this.status);
        if (this.dependencies == null) {
            this.dependencies = InsertionOrderUtil.newSet();
        }
        this.dependencies.add(id);
    }

    public TaskStatus status() {
        return this.status;
    }

    public void type(String type) {
        this.type = type;
    }

    public String type() {
        return this.type;
    }

    public void name(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    public void description(String description) {
        this.description = description;
    }

    public String description() {
        return this.description;
    }

    public final void context(String context) {
        if (Strings.isNullOrEmpty(this.context)) {
            LOG.warn("Set null context for task({}) is null", this.id);
        }

        LOG.debug("Set context({}) for task() is null", context, this.id);

        this.context = context;
    }

    public final String context() {
        return this.context;
    }

    public void progress(int progress) {
        this.progress = progress;
    }

    public int progress() {
        return this.progress;
    }

    public void createTime(Date create) {
        this.create = create;
    }

    public Date createTime() {
        return this.create;
    }

    public void updateTime(Date update) {
        this.update = update;
    }

    public Date updateTime() {
        return this.update;
    }

    public void retry() {
        ++this.retries;
    }

    public void retries(int retry) {
        this.retries = retry;
    }

    public int retries() {
        return this.retries;
    }

    public void input(String input) {
        // checkPropertySize(input, P.INPUT);
        this.input = input;
    }

    public String input() {
        return this.input;
    }

    public String result() {
        return this.result;
    }

    public synchronized void result(HugeTaskResult result) {
        this.result = result.result();
    }

    public synchronized boolean result(TaskStatus status, String result) {
        checkPropertySize(result, P.RESULT);
        if (this.status(status)) {
            this.result = result;
            return true;
        }
        return false;
    }

    /**
     * Force to overwrite result with a non-null value
     *
     * @param result
     */
    public synchronized void overwriteResult(String result) {
        if (null != result) {
            this.result = result;
        }
    }

    /**
     * Force to overwrite context with a non-null value
     *
     * @param context
     */
    public final void overwriteContext(String context) {
        if (null != context) {
            this.context = context;
        }
    }

    public void server(Id server) {
        this.server = server;
    }

    public Id server() {
        return this.server;
    }

    public void load(int load) {
        this.load = load;
    }

    public int load() {
        return this.load;
    }

    public TaskPriority priority() {
        return this.priority;
    }

    public void priority(TaskPriority priority) {
        this.priority = priority;
    }

    public boolean completed() {
        return TaskStatus.COMPLETED_STATUSES.contains(this.status);
    }

    public boolean success() {
        return this.status == TaskStatus.SUCCESS;
    }

    public boolean cancelled() {
        return this.status == TaskStatus.CANCELLED || this.isCancelled();
    }

    public boolean cancelling() {
        return this.status == TaskStatus.CANCELLING;
    }

    public boolean computer() {
        return ComputerDisJob.COMPUTER_DIS.equals(this.type) ||
               ComputerJob.COMPUTER.equals(this.type);
    }

    public boolean vermeer() {
        return VermeerJob.VERMEER_TASK.equals(this.type);
    }

    public boolean schemaTask() {
        return this.callable instanceof SchemaCallable;
    }

    public boolean gremlinTask() {
        return this.callable instanceof GremlinJob;
    }

    public boolean ephemeralTask() {
        return this.callable instanceof EphemeralJob;
    }

    @Override
    public String toString() {
        return String.format("HugeTask(%s)%s", this.id, this.asMap());
    }

    @Override
    public void run() {
        if (this.cancelled()) {
            // Scheduled task is running after cancelled
            return;
        }

        TaskManager.setContext(this.context());

        try {
            assert this.status.code() <= TaskStatus.RUNNING.code() : this.status;
            if (this.checkDependenciesSuccess()) {
                this.status(TaskStatus.RUNNING);
                super.run();
            }
        } catch (Throwable e) {
            this.setException(e);
        } finally {
            LOG.debug("Task is finished {}", this);
            TaskManager.resetContext();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {

        // update status
        this.status(TaskStatus.CANCELLED);
        try {
            this.callable.cancelled();
        } catch (Throwable e) {
            LOG.error("An exception occurred when calling cancelled()", e);
        }
        // super.cancel() will call finishCompletion() and done()

        // NOTE: Gremlin sleep() can't be interrupted by default
        // https://mrhaki.blogspot.com/2016/10/groovy-goodness-interrupted-sleeping.html
        return super.cancel(mayInterruptIfRunning);
    }

    public boolean fail(Throwable e) {
        E.checkNotNull(e, "exception");
        if (!(this.cancelled() && HugeException.isInterrupted(e))) {
            LOG.warn("An exception occurred when running task: {}",
                     this.id(), e);
            // Update status to FAILED if exception occurred(not interrupted)
            return this.result(TaskStatus.FAILED, e.toString());
        }
        return false;
    }

    public void failToSave(Throwable e) {
        if (!this.fail(e)) {
            // Can't update status, just set result to error message
            this.result = e.toString();
        }
    }

    @Override
    protected void done() {
        try {
            // Callback for saving status to store
            this.callable.done();
        } catch (Throwable e) {
            LOG.warn("An exception occurred when calling done()", e);
        } finally {
            scheduler.taskDone(this);
        }
    }

    @Override
    protected void set(V v) {
        String result = JsonUtil.toJson(v);
        checkPropertySize(result, P.RESULT);
        if (!this.result(TaskStatus.SUCCESS, result)) {
            assert this.completed();
        }
        // Will call done() and may cause to save to store
        super.set(v);
    }

    @Override
    protected void setException(Throwable e) {
        this.fail(e);
        super.setException(e);
    }

    protected void scheduler(TaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    protected TaskScheduler scheduler() {
        E.checkState(this.scheduler != null,
                     "Can't call scheduler() before scheduling task");
        return this.scheduler;
    }

    protected boolean checkDependenciesSuccess() {
        if (this.dependencies == null || this.dependencies.isEmpty()) {
            return true;
        }
        TaskScheduler scheduler = this.scheduler();
        for (Id dependency : this.dependencies) {
            HugeTask<?> task = scheduler.task(dependency);
            if (!task.completed()) {
                // Dependent task not completed, re-schedule self
                scheduler.schedule(this);
                return false;
            } else if (task.status() == TaskStatus.CANCELLED) {
                this.result(TaskStatus.CANCELLED, String.format(
                        "Cancelled due to dependent task '%s' cancelled",
                        dependency));
                this.done();
                return false;
            } else if (task.status() == TaskStatus.FAILED) {
                this.result(TaskStatus.FAILED, String.format(
                        "Failed due to dependent task '%s' failed",
                        dependency));
                this.done();
                return false;
            }
        }
        return true;
    }

    protected TaskCallable<V> callable() {
        E.checkNotNull(this.callable, "callable");
        return this.callable;
    }

    protected synchronized boolean status(TaskStatus status) {
        E.checkNotNull(status, "status");
        if (status.code() > TaskStatus.NEW.code()) {
            E.checkState(this.type != null, "Task type can't be null");
            E.checkState(this.name != null, "Task name can't be null");
        }
        // Allow computer task change completed status (better to unify later)
        if (!this.completed() || this.computer()) {
            /*
            assert this.status.code() < status.code() ||
                   status == TaskStatus.RESTORING :
                   this.status + " => " + status + " (task " + this.id + ")";*/
            this.status = status;
            return true;
        }
        return false;
    }

    public synchronized void overwriteStatus(TaskStatus status) {
        this.status = status;
    }

    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.TYPE:
                this.type = (String) value;
                break;
            case P.NAME:
                this.name = (String) value;
                break;
            case P.CALLABLE:
                // pass
                break;
            case P.STATUS:
                this.status = SerialEnum.fromCode(TaskStatus.class,
                                                  (byte) value);
                break;
            case P.PROGRESS:
                this.progress = (int) value;
                break;
            case P.CREATE:
                this.create = (Date) value;
                break;
            case P.RETRIES:
                this.retries = (int) value;
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            case P.CONTEXT:
                this.context = (String) value;
                break;
            case P.UPDATE:
                this.update = (Date) value;
                break;
            case P.DEPENDENCIES:
                @SuppressWarnings("unchecked")
                Set<Long> values = (Set<Long>) value;
                this.dependencies = values.stream().map(IdGenerator::of)
                                          .collect(toOrderSet());
                break;
            case P.INPUT:
                this.input = StringEncoding.decompress(((Blob) value).bytes(),
                                                       DECOMPRESS_RATIO);
                break;
            case P.RESULT:
                this.result = StringEncoding.decompress(((Blob) value).bytes(),
                                                        DECOMPRESS_RATIO);
                break;
            case P.SERVER:
                this.server = IdGenerator.of((String) value);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    protected synchronized Object[] asArray() {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        List<Object> list = new ArrayList<>(28);

        list.add(T.label);
        list.add(P.TASK);

        list.add(T.id);
        list.add(this.id);

        list.add(P.TYPE);
        list.add(this.type);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.CALLABLE);
        list.add(this.callable.getClass().getName());

        list.add(P.STATUS);
        list.add(this.status.code());

        list.add(P.PROGRESS);
        list.add(this.progress);

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.RETRIES);
        list.add(this.retries);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        if (this.context != null) {
            list.add(P.CONTEXT);
            list.add(this.context);
        }

        if (this.update != null) {
            list.add(P.UPDATE);
            list.add(this.update);
        }

        if (this.dependencies != null) {
            list.add(P.DEPENDENCIES);
            list.add(this.dependencies.stream().map(Id::asLong)
                                      .collect(toOrderSet()));
        }

        if (this.input != null) {
            byte[] bytes = StringEncoding.compress(this.input);
            checkPropertySize(bytes.length, P.INPUT);
            list.add(P.INPUT);
            list.add(bytes);
        }

        if (this.result != null) {
            byte[] bytes = StringEncoding.compress(this.result);
            checkPropertySize(bytes.length, P.RESULT);
            list.add(P.RESULT);
            list.add(bytes);
        }

        if (this.server != null) {
            list.add(P.SERVER);
            list.add(this.server.asString());
        }

        return list.toArray();
    }

    // 仅处理任务相关，不保存任务结果
    protected synchronized Object[] asArrayWithoutResult() {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        List<Object> list = new ArrayList<>(28);

        list.add(T.label);
        list.add(P.TASK);

        list.add(T.id);
        list.add(this.id);

        list.add(P.TYPE);
        list.add(this.type);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.CALLABLE);
        list.add(this.callable.getClass().getName());

        list.add(P.STATUS);
        list.add(this.status.code());

        list.add(P.PROGRESS);
        list.add(this.progress);

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.RETRIES);
        list.add(this.retries);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        if (this.context != null) {
            list.add(P.CONTEXT);
            list.add(this.context);
        }

        if (this.update != null) {
            list.add(P.UPDATE);
            list.add(this.update);
        }

        if (this.dependencies != null) {
            list.add(P.DEPENDENCIES);
            list.add(this.dependencies.stream().map(Id::asLong)
                                      .collect(toOrderSet()));
        }

        if (this.input != null) {
            byte[] bytes = StringEncoding.compress(this.input);
            checkPropertySize(bytes.length, P.INPUT);
            list.add(P.INPUT);
            list.add(bytes);
        }

        if (this.server != null) {
            list.add(P.SERVER);
            list.add(this.server.asString());
        }

        return list.toArray();
    }

    public Map<String, Object> asMap() {
        return this.asMap(true);
    }

    public synchronized Map<String, Object> asMap(boolean withDetails) {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.ID), this.id);

        map.put(Hidden.unHide(P.TYPE), this.type);
        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.STATUS), this.status.string());
        map.put(Hidden.unHide(P.PROGRESS), this.progress);
        map.put(Hidden.unHide(P.CREATE), this.create);
        map.put(Hidden.unHide(P.RETRIES), this.retries);
        map.put(Hidden.unHide(P.PRIORITY), this.priority.toString());

        if (this.context != null) {
            map.put(Hidden.unHide(P.CONTEXT), this.context);
        }

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }
        if (this.update != null) {
            map.put(Hidden.unHide(P.UPDATE), this.update);
        }
        if (this.dependencies != null) {
            Set<Long> value = this.dependencies.stream().map(Id::asLong)
                                               .collect(toOrderSet());
            map.put(Hidden.unHide(P.DEPENDENCIES), value);
        }

        if (this.server != null) {
            map.put(Hidden.unHide(P.SERVER), this.server.asString());
        }

        if (withDetails) {
            map.put(Hidden.unHide(P.CALLABLE),
                    this.callable.getClass().getName());
            if (this.input != null) {
                map.put(Hidden.unHide(P.INPUT), this.input);
            }
            if (this.result != null) {
                map.put(Hidden.unHide(P.RESULT), this.result);
            }
        }

        return map;
    }

    private void checkPropertySize(String property, String propertyName) {
        byte[] bytes = StringEncoding.compress(property);
        checkPropertySize(bytes.length, propertyName);
    }

    private void checkPropertySize(int propertyLength, String propertyName) {
        long propertyLimit = BytesBuffer.STRING_LEN_MAX;
        HugeGraph graph = this.scheduler().graph();
        if (propertyName.equals(P.INPUT)) {
            propertyLimit = graph.option(CoreOptions.TASK_INPUT_SIZE_LIMIT);
        } else if (propertyName.equals(P.RESULT)) {
            propertyLimit = graph.option(CoreOptions.TASK_RESULT_SIZE_LIMIT);
        }

        if (propertyLength > propertyLimit) {
            throw new LimitExceedException(
                    "Task %s size %s exceeded limit %s bytes",
                    P.unhide(propertyName), propertyLength, propertyLimit);
        }
    }

    public void syncWait() {
        // This method is just called by tests
        HugeTask<?> task;
        try {
            task = this.scheduler().waitUntilTaskCompleted(this.id());
        } catch (Throwable e) {
            if (this.callable() instanceof EphemeralJob &&
                e.getClass() == NotFoundException.class &&
                e.getMessage().contains("Can't find task with id")) {
                /*
                 * The task with EphemeralJob won't saved in backends and
                 * will be removed from memory when completed
                 */
                return;
            }
            throw new HugeException("Failed to wait for task '%s' completed",
                                    e, this.id);
        }
        assert task != null;
        /*
         * This can be enabled for debug to expose schema-clear errors early，
         * but also lead to some negative tests failed,
         */
        boolean debugTest = false;
        if (debugTest && !task.success()) {
            throw new HugeException("Task '%s(%s)' is failed with error: %s",
                                    task.id(), task.status(), task.result());
        }
    }

    public static final class P {

        public static final String TASK = Hidden.hide("task");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String TYPE = "~task_type";
        public static final String NAME = "~task_name";
        public static final String CALLABLE = "~task_callable";
        public static final String DESCRIPTION = "~task_description";
        public static final String CONTEXT = "~task_context";
        public static final String STATUS = "~task_status";
        public static final String PROGRESS = "~task_progress";
        public static final String CREATE = "~task_create";
        public static final String UPDATE = "~task_update";
        public static final String RETRIES = "~task_retries";
        public static final String INPUT = "~task_input";
        public static final String RESULT = "~task_result";
        public static final String DEPENDENCIES = "~task_dependencies";
        public static final String SERVER = "~task_server";
        public static final String PRIORITY = "~task_priority";

        //public static final String PARENT = hide("parent");
        //public static final String CHILDREN = hide("children");

        public static String unhide(String key) {
            final String prefix = Hidden.hide("task_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static class Schema {
        private final HugeGraph graph;

        public Schema(HugeGraph graph) {
            this.graph = graph;
        }

        public void initSchemaIfNeeded() {
            if (graph.existsVertexLabel(P.TASK)) {
                LOG.info("task schema exist");
                return;
            }

            LOG.info("init task schema");

            String[] properties = this.initProperties();

            // Create vertex label '~task'
            VertexLabel label = graph.schema().vertexLabel(P.TASK)
                                     .properties(properties)
                                     .useCustomizeNumberId()
                                     .nullableKeys(P.DESCRIPTION, P.CONTEXT,
                                                   P.UPDATE, P.INPUT, P.RESULT,
                                                   P.DEPENDENCIES, P.SERVER)
                                     .enableLabelIndex(true)
                                     .build();
            this.graph.addVertexLabel(label);

            // Create index
            String name = Hidden.hide("task-index-by-" + P.STATUS);
            IndexLabel indexLabel = graph.schema().indexLabel(name)
                                         .on(HugeType.VERTEX_LABEL, P.TASK)
                                         .by(P.STATUS)
                                         .build();
            graph.addIndexLabel(label, indexLabel);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.TYPE));
            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.CALLABLE));
            props.add(createPropertyKey(P.DESCRIPTION));
            props.add(createPropertyKey(P.CONTEXT));
            props.add(createPropertyKey(P.STATUS, DataType.BYTE));
            props.add(createPropertyKey(P.PROGRESS, DataType.INT));
            props.add(createPropertyKey(P.CREATE, DataType.DATE));
            props.add(createPropertyKey(P.UPDATE, DataType.DATE));
            props.add(createPropertyKey(P.RETRIES, DataType.INT));
            props.add(createPropertyKey(P.INPUT, DataType.BLOB));
            props.add(createPropertyKey(P.RESULT, DataType.BLOB));
            props.add(createPropertyKey(P.DEPENDENCIES, DataType.LONG,
                                        Cardinality.SET));
            props.add(createPropertyKey(P.SERVER));

            return props.toArray(new String[0]);
        }


        private String createPropertyKey(String name) {
            return createPropertyKey(name, DataType.TEXT);
        }

        private String createPropertyKey(String name, DataType dataType) {
            return createPropertyKey(name, dataType, Cardinality.SINGLE);
        }

        private String createPropertyKey(String name, DataType dataType,
                                         Cardinality cardinality) {
            SchemaManager schema = graph.schema();
            PropertyKey propertyKey = schema.propertyKey(name)
                                            .dataType(dataType)
                                            .cardinality(cardinality)
                                            .build();
            this.graph.addPropertyKey(propertyKey);
            return name;
        }
    }
}