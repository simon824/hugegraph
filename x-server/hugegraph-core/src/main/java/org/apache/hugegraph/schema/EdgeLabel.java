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

package org.apache.hugegraph.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.builder.SchemaBuilder;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.google.common.base.Objects;

public class EdgeLabel extends SchemaLabel {

    public static final EdgeLabel NONE = new EdgeLabel(null, NONE_ID, UNDEF);

    private Set<Pair<Id, Id>> links = new HashSet<>();
    private Id sourceLabel = NONE_ID;
    private Frequency frequency;
    private List<Id> sortKeys;

    private EdgeLabelType edgeLabelType = EdgeLabelType.NORMAL;
    private Id fatherId;

    public EdgeLabel(final HugeGraph graph, Id id, String name) {
        super(graph, id, name);
        this.frequency = Frequency.DEFAULT;
        this.sortKeys = new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    public static EdgeLabel fromMap(Map<String, Object> map, HugeGraph graph) {
        Id id = IdGenerator.of((int) map.get(EdgeLabel.P.ID));
        String name = (String) map.get(EdgeLabel.P.NAME);
        EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case P.ID:
                case P.NAME:
                    break;
                case P.STATUS:
                    edgeLabel.status(
                            SchemaStatus.valueOf(((String) entry.getValue()).toUpperCase()));
                    break;
                case P.USERDATA:
                    edgeLabel.userdata((Map<String, Object>) entry.getValue());
                    break;
                case P.PROPERTIES:
                    Set<Id> ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.properties(ids);
                    break;
                case P.NULLABLE_KEYS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.nullableKeys(ids);
                    break;
                case P.INDEX_LABELS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.indexLabels(ids.toArray(new Id[0]));
                    break;
                case P.ENABLE_LABEL_INDEX:
                    boolean enableLabelIndex = (Boolean) entry.getValue();
                    edgeLabel.enableLabelIndex(enableLabelIndex);
                    break;
                case P.TTL:
                    long ttl = Long.parseLong((String) entry.getValue());
                    edgeLabel.ttl(ttl);
                    break;
                case P.TT_START_TIME:
                    long ttlStartTime =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.ttlStartTime(IdGenerator.of(ttlStartTime));
                    break;
                case P.LINKS:
                    // TODO: serialize and deserialize
                    List<Map> list = (List<Map>) entry.getValue();
                    for (Map m : list) {
                        for (Object key : m.keySet()) {
                            Id sid = IdGenerator.of(Long.parseLong((String) key));
                            Id tid = IdGenerator.of(Long.parseLong(String.valueOf(m.get(key))));
                            edgeLabel.links(Pair.of(sid, tid));
                        }
                    }
                    break;
                case P.SOURCE_LABEL:
                    long sourceLabel =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.sourceLabel(IdGenerator.of(sourceLabel));
                    break;
                case P.TARGET_LABEL:
                    long targetLabel =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.targetLabel(IdGenerator.of(targetLabel));
                    break;
                case P.FATHER_ID:
                    long fatherId =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.fatherId(IdGenerator.of(fatherId));
                    break;
                case P.EDGELABEL_TYPE:
                    EdgeLabelType edgeLabelType =
                            EdgeLabelType.valueOf(
                                    ((String) entry.getValue()).toUpperCase());
                    edgeLabel.edgeLabelType(edgeLabelType);
                    break;
                case P.FREQUENCY:
                    Frequency frequency =
                            Frequency.valueOf(((String) entry.getValue()).toUpperCase());
                    edgeLabel.frequency(frequency);
                    break;
                case P.SORT_KEYS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.sortKeys(ids.toArray(new Id[0]));
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Invalid key '%s' for edge label",
                            entry.getKey()));
            }
        }
        return edgeLabel;
    }

    public static EdgeLabel undefined(HugeGraph graph, Id id) {
        return new EdgeLabel(graph, id, UNDEF);
    }

    public static void main(String[] args) {
        EdgeLabel el = new EdgeLabel(null, IdGenerator.of(10), "knows");
        el.sourceLabel(IdGenerator.of(1));
        el.targetLabel(IdGenerator.of(1));
        el.links(Pair.of(IdGenerator.of(1), IdGenerator.of(1)));
        el.properties(IdGenerator.of(3), IdGenerator.of(4), IdGenerator.of(5));

        String content = JsonUtil.toJson(el.asMap());
        System.out.println("el to json:\n" + content);

        el = EdgeLabel.fromMap(JsonUtil.fromJson(content, Map.class), null);
        content = JsonUtil.toJson(el.asMap());
        System.out.println("el to json to el to json:\n" + content);
//        String content = "{\"ttlStartTime\":\"0\",\"sourceLabel\":\"1\",\"targetLabel\":\"1\",
//        \"ttl\":\"0\",\"frequency\":\"SINGLE\",
//        \"userdata\":{},\"name\":\"1628\",
//        \"links\":[{\"1\":1}],\"sortKeys\":[],
//        \"enableLabelIndex\":true,\"id\":110001,\"nullableKeys\":[],
//        \"properties\":[5],\"indexLabels\":[],\"status\":\"created\"}";
//        Map map = JsonUtil.fromJson(content, Map.class);
//        EdgeLabel el = EdgeLabel.fromMap(map);
//        System.out.println("el序列化的json:" + content);
//        System.out.println("json反序列化的结果" + AbstractMetaManager.serialize(el));
    }

    @Override
    public HugeType type() {
        return HugeType.EDGE_LABEL;
    }

    public boolean isFather() {
        return this.edgeLabelType.parent();
    }

    public void edgeLabelType(EdgeLabelType type) {
        this.edgeLabelType = type;
    }

    public EdgeLabelType edgeLabelType() {
        return this.edgeLabelType;
    }

    public boolean hasFather() {
        return this.edgeLabelType.sub();
    }

    public Id fatherId() {
        return this.fatherId;
    }

    public void fatherId(Id fatherId) {
        this.fatherId = fatherId;
    }

    public Frequency frequency() {
        return this.frequency;
    }

    public void frequency(Frequency frequency) {
        this.frequency = frequency;
    }

    public boolean directed() {
        // TODO: implement (do we need this method?)
        return true;
    }

    public String sourceLabelName() {
        E.checkState(this.links.size() == 1,
                     "Only edge label has single vertex label pair can call " +
                     "sourceLabelName(), but current edge label got %s",
                     this.links.size());
        return this.graph.vertexLabelOrNone(
                this.links.iterator().next().getLeft()).name();
    }

    public List<Id> linksIds() {
        List<Id> ids = new ArrayList<>(this.links.size() * 2);
        for (Pair<Id, Id> link : this.links) {
            ids.add(link.getLeft());
            ids.add(link.getRight());
        }
        return ids;
    }

    public void linksIds(Id[] ids) {
        this.links = new HashSet<>(ids.length / 2);
        for (int i = 0; i < ids.length; i += 2) {
            this.links.add(Pair.of(ids[i], ids[i + 1]));
        }
    }

    public Id sourceLabel() {
        if (links.size() == 1) {
            return links.iterator().next().getLeft();
        }
        return NONE_ID;
    }

    public void sourceLabel(Id id) {
        E.checkArgument(this.links.isEmpty(),
                        "Not allowed add source label to an edge label which " +
                        "already has links");
        this.sourceLabel = id;
    }

    public String targetLabelName() {
        E.checkState(this.links.size() == 1,
                     "Only edge label has single vertex label pair can call " +
                     "sourceLabelName(), but current edge label got %s",
                     this.links.size());
        return this.graph.vertexLabelOrNone(
                this.links.iterator().next().getRight()).name();
    }

    public Id targetLabel() {
        if (links.size() == 1) {
            return links.iterator().next().getRight();
        }
        return NONE_ID;
    }

    public void targetLabel(Id id) {
        E.checkArgument(this.links.isEmpty(),
                        "Not allowed add source label to an edge label which " +
                        "already has links");
        E.checkArgument(this.sourceLabel != NONE_ID,
                        "Not allowed add target label to an edge label which " +
                        "not has source label yet");
        this.links.add(Pair.of(this.sourceLabel, id));
        this.sourceLabel = NONE_ID;
    }

    public boolean linkWithLabel(Id id) {
        for (Pair<Id, Id> link : this.links) {
            if (link.getLeft().equals(id) || link.getRight().equals(id)) {
                return true;
            }
        }
        return false;
    }

    public boolean checkLinkEqual(Id sourceLabel, Id targetLabel) {
        return this.links.contains(Pair.of(sourceLabel, targetLabel));
    }

    public Set<Pair<Id, Id>> links() {
        return this.links;
    }

    public void links(Pair<Id, Id> link) {
        if (this.links == null) {
            this.links = new HashSet<>();
        }
        this.links.add(link);
    }

    public boolean existSortKeys() {
        return this.sortKeys.size() > 0;
    }

    public List<Id> sortKeys() {
        return Collections.unmodifiableList(this.sortKeys);
    }

    public void sortKey(Id id) {
        this.sortKeys.add(id);
    }

    public void sortKeys(Id... ids) {
        this.sortKeys.addAll(Arrays.asList(ids));
    }

    public boolean hasSameContent(EdgeLabel other) {
        return super.hasSameContent(other) &&
               this.frequency == other.frequency &&
               Objects.equal(this.links, other.links) &&
               Objects.equal(this.graph.mapPkId2Name(this.sortKeys),
                             other.graph.mapPkId2Name(other.sortKeys));
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();

        if (this.sourceLabel() != null && this.sourceLabel() != NONE_ID) {
            map.put(P.SOURCE_LABEL, this.sourceLabel().asString());
        }

        if (this.targetLabel() != null && this.targetLabel() != NONE_ID) {
            map.put(P.TARGET_LABEL, this.targetLabel().asString());
        }

        if (this.properties() != null) {
            map.put(P.PROPERTIES, this.properties());
        }

        if (this.nullableKeys() != null) {
            map.put(P.NULLABLE_KEYS, this.nullableKeys());
        }

        if (this.indexLabels() != null) {
            map.put(P.INDEX_LABELS, this.indexLabels());
        }

        if (this.ttlStartTime() != null) {
            map.put(P.TT_START_TIME, this.ttlStartTime().asString());
        }

        if (this.sortKeys() != null) {
            map.put(P.SORT_KEYS, this.sortKeys);
        }

        map.put(P.EDGELABEL_TYPE, this.edgeLabelType);
        if (this.fatherId() != null) {
            map.put(P.FATHER_ID, this.fatherId().asString());
        }
        map.put(P.ENABLE_LABEL_INDEX, this.enableLabelIndex());
        map.put(P.TTL, String.valueOf(this.ttl()));
        map.put(P.LINKS, this.links());
        map.put(P.FREQUENCY, this.frequency().toString());

        return super.asMap(map);
    }

    public String convert2Groovy() {
        StringBuilder builder = new StringBuilder(SCHEMA_PREFIX);
        // Name
        builder.append("edgeLabel").append("('")
               .append(this.name())
               .append("')");

        if (this.links.size() == 1) {
            Pair<Id, Id> link = this.links.iterator().next();
            // Source label
            VertexLabel vl = this.graph.vertexLabel(link.getLeft());
            builder.append(".sourceLabel('")
                   .append(vl.name())
                   .append("')");

            // Target label
            vl = this.graph.vertexLabel(link.getRight());
            builder.append(".targetLabel('")
                   .append(vl.name())
                   .append("')");
        } else {
            for (Pair<Id, Id> link : this.links) {
                // Source label
                VertexLabel vl = this.graph.vertexLabel(link.getLeft());
                builder.append(".link('")
                       .append(vl.name())
                       .append("',");

                // Target label
                vl = this.graph.vertexLabel(link.getRight());
                builder.append("'")
                       .append(vl.name())
                       .append("')");
            }
        }

        // Properties
        Set<Id> properties = this.properties();
        if (!properties.isEmpty()) {
            builder.append(".").append("properties(");

            int size = properties.size();
            for (Id id : this.properties()) {
                PropertyKey pk = this.graph.propertyKey(id);
                builder.append("'")
                       .append(pk.name())
                       .append("'");
                if (--size > 0) {
                    builder.append(",");
                }
            }
            builder.append(")");
        }

        // Frequency
        switch (this.frequency) {
            case SINGLE:
                // Single is default, prefer not output
                break;
            case MULTIPLE:
                builder.append(".multiTimes()");
                break;
            default:
                throw new AssertionError(String.format(
                        "Invalid frequency '%s'", this.frequency));
        }

        // Sort keys
        List<Id> sks = this.sortKeys();
        if (!sks.isEmpty()) {
            builder.append(".sortKeys(");
            int size = sks.size();
            for (Id id : sks) {
                PropertyKey pk = this.graph.propertyKey(id);
                builder.append("'")
                       .append(pk.name())
                       .append("'");
                if (--size > 0) {
                    builder.append(",");
                }
            }
            builder.append(")");
        }

        // Nullable keys
        properties = this.nullableKeys();
        if (!properties.isEmpty()) {
            builder.append(".").append("nullableKeys(");
            int size = properties.size();
            for (Id id : properties) {
                PropertyKey pk = this.graph.propertyKey(id);
                builder.append("'")
                       .append(pk.name())
                       .append("'");
                if (--size > 0) {
                    builder.append(",");
                }
            }
            builder.append(")");
        }

        // TTL
        if (this.ttl() != 0) {
            builder.append(".ttl(")
                   .append(this.ttl())
                   .append(")");
            if (this.ttlStartTime() != null) {
                PropertyKey pk = this.graph.propertyKey(this.ttlStartTime());
                builder.append(".ttlStartTime('")
                       .append(pk.name())
                       .append("')");
            }
        }

        // Enable label index
        if (this.enableLabelIndex()) {
            builder.append(".enableLabelIndex(true)");
        } else {
            builder.append(".enableLabelIndex(false)");
        }

        // User data
        Map<String, Object> userdata = this.userdata();
        if (userdata.isEmpty()) {
            return builder.toString();
        }
        for (Map.Entry<String, Object> entry : userdata.entrySet()) {
            if (Graph.Hidden.isHidden(entry.getKey())) {
                continue;
            }
            builder.append(".userdata('")
                   .append(entry.getKey())
                   .append("',")
                   .append(entry.getValue())
                   .append(")");
        }

        builder.append(".ifNotExist().create();");
        return builder.toString();
    }

    public interface Builder extends SchemaBuilder<EdgeLabel> {

        Id rebuildIndex();

        Builder asBase();

        Builder asGeneral();

        Builder withBase(String fatherLabel);

        Builder link(String sourceLabel, String targetLabel);

        Builder sourceLabel(String label);

        Builder targetLabel(String label);

        Builder singleTime();

        Builder multiTimes();

        Builder sortKeys(String... keys);

        Builder properties(String... properties);

        Builder nullableKeys(String... keys);

        Builder frequency(Frequency frequency);

        Builder ttl(long ttl);

        Builder ttlStartTime(String ttlStartTime);

        Builder enableLabelIndex(boolean enable);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }

    public static final class P {

        public static final String ID = "id";
        public static final String NAME = "name";

        public static final String STATUS = "status";
        public static final String USERDATA = "userdata";

        public static final String PROPERTIES = "properties";
        public static final String NULLABLE_KEYS = "nullableKeys";
        public static final String INDEX_LABELS = "indexLabels";

        public static final String ENABLE_LABEL_INDEX = "enableLabelIndex";
        public static final String TTL = "ttl";
        public static final String TT_START_TIME = "ttlStartTime";
        public static final String LINKS = "links";
        public static final String SOURCE_LABEL = "sourceLabel";
        public static final String TARGET_LABEL = "targetLabel";
        public static final String EDGELABEL_TYPE = "edgeLabelType";
        public static final String FATHER_ID = "fatherId";
        public static final String FREQUENCY = "frequency";
        public static final String SORT_KEYS = "sortKeys";
    }
}
