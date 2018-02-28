/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

public class TextSerializer extends AbstractSerializer {

    private static final String VALUE_SPLITOR = TextBackendEntry.VALUE_SPLITOR;

    @Override
    public TextBackendEntry newBackendEntry(HugeType type, Id id) {
        return new TextBackendEntry(type, id);
    }

    private TextBackendEntry newBackendEntry(HugeElement elem) {
        Id id = IdGenerator.of(writeEntryId(elem.id()));
        return new TextBackendEntry(elem.type(), id);
    }

    private TextBackendEntry newBackendEntry(SchemaElement elem) {
        Id id = IdGenerator.of(writeId(elem.id()));
        return new TextBackendEntry(elem.type(), id);
    }

    @Override
    protected TextBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof TextBackendEntry)) {
            throw new HugeException("The entry '%s' is not TextBackendEntry",
                                    backendEntry);
        }
        return (TextBackendEntry) backendEntry;
    }

    private String formatSyspropName(String name) {
        return SplicingIdGenerator.concat(writeType(HugeType.SYS_PROPERTY),
                                          name);
    }

    private String formatSyspropName(HugeKeys col) {
        return this.formatSyspropName(col.string());
    }

    private String formatPropertyName(String key) {
        return SplicingIdGenerator.concat(writeType(HugeType.PROPERTY), key);
    }

    private String formatPropertyName(HugeProperty<?> prop) {
        return this.formatPropertyName(writeId(prop.propertyKey().id()));
    }

    private String formatPropertyValue(HugeProperty<?> prop) {
        // May be a single value or a list of values
        return JsonUtil.toJson(prop.value());
    }

    private void parseProperty(String colName, String colValue,
                               HugeElement owner) {
        String[] colParts = SplicingIdGenerator.split(colName);
        assert colParts.length == 2 : colName;

        // Get PropertyKey by PropertyKey id
        PropertyKey pkey = owner.graph().propertyKey(readId(colParts[1]));

        // Parse value
        Object value = JsonUtil.fromJson(colValue, pkey.clazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-sigle property: %s", colValue);
            }
            for (Object v : (Collection<?>) value) {
                v = JsonUtil.castNumber(v, pkey.dataType().clazz());
                owner.addProperty(pkey, v);
            }
        }
    }

    private String formatEdgeName(HugeEdge edge) {
        // Edge name: type + edge-label-name + sortKeys + targetVertex
        return writeEdgeId(edge.idWithDirection(), false);
    }

    private String formatEdgeValue(HugeEdge edge) {
        StringBuilder sb = new StringBuilder(256 * edge.getProperties().size());
        // Edge id
        sb.append(edge.id().asString());
        // Edge properties
        for (HugeProperty<?> property : edge.getProperties().values()) {
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyName(property));
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyValue(property));
        }
        return sb.toString();
    }

    /**
     * Parse an edge from a column item
     */
    private void parseEdge(String colName, String colValue,
                           HugeVertex vertex) {
        String[] colParts = EdgeId.split(colName);

        HugeGraph graph = vertex.graph();
        EdgeLabel label = graph.edgeLabel(readId(colParts[1]));

        VertexLabel sourceLabel = graph.vertexLabel(label.sourceLabel());
        VertexLabel targetLabel = graph.vertexLabel(label.targetLabel());

        Id otherVertexId = readEntryId(colParts[3]);

        boolean isOutEdge = colParts[0].equals(writeType(HugeType.EDGE_OUT));
        HugeVertex otherVertex;
        if (isOutEdge) {
            vertex.vertexLabel(sourceLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, targetLabel);
        } else {
            vertex.vertexLabel(targetLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, sourceLabel);
        }

        HugeEdge edge = new HugeEdge(graph, null, label);
        edge.name(colParts[2]);

        if (isOutEdge) {
            edge.vertices(vertex, vertex, otherVertex);
            edge.assignId();
            vertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            edge.vertices(vertex, otherVertex, vertex);
            edge.assignId();
            vertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }

        vertex.propNotLoaded();
        otherVertex.propNotLoaded();

        String[] valParts = colValue.split(VALUE_SPLITOR);
        // Edge properties
        for (int i = 1; i < valParts.length; i += 2) {
            this.parseProperty(valParts[i], valParts[i + 1], edge);
        }
    }

    private void parseColumn(String colName, String colValue,
                             HugeVertex vertex) {
        // Column name
        String type = SplicingIdGenerator.split(colName)[0];
        // Parse property
        if (type.equals(writeType(HugeType.PROPERTY))) {
            this.parseProperty(colName, colValue, vertex);
        }
        // Parse edge
        if (type.equals(writeType(HugeType.EDGE_OUT)) ||
            type.equals(writeType(HugeType.EDGE_IN))) {
            this.parseEdge(colName, colValue, vertex);
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        TextBackendEntry entry = newBackendEntry(vertex);

        // Write label (NOTE: maybe just with edges if label is null)
        if (vertex.schemaLabel() != null) {
            entry.column(this.formatSyspropName(HugeKeys.LABEL),
                         writeId(vertex.schemaLabel().id()));
        }

        // Add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatPropertyName(prop),
                         this.formatPropertyValue(prop));
        }

        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        HugeVertex vertex = prop.element();
        TextBackendEntry entry = newBackendEntry(vertex);
        entry.subId(IdGenerator.of(prop.key()));

        // Write label (NOTE: maybe just with edges if label is null)
        if (vertex.schemaLabel() != null) {
            entry.column(this.formatSyspropName(HugeKeys.LABEL),
                         writeId(vertex.schemaLabel().id()));
        }

        entry.column(this.formatPropertyName(prop),
                     this.formatPropertyValue(prop));
        return entry;
    }

    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        // Parse label
        String labelId = entry.column(this.formatSyspropName(HugeKeys.LABEL));
        VertexLabel label = null;
        if (labelId != null) {
            label = graph.vertexLabel(readId(labelId));
        }

        Id id = IdUtil.readString(entry.id().asString());
        HugeVertex vertex = new HugeVertex(graph, id, label);

        // Parse all properties or edges of a Vertex
        for (String name : entry.columnNames()) {
            this.parseColumn(name, entry.column(name), vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        Id id = IdGenerator.of(edge.idWithDirection().asString());
        TextBackendEntry entry = newBackendEntry(edge.type(), id);
        entry.column(this.formatEdgeName(edge), this.formatEdgeValue(edge));
        return entry;
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        HugeEdge edge = prop.element();
        Id id = IdGenerator.of(edge.idWithDirection().asString());
        TextBackendEntry entry = newBackendEntry(edge.type(), id);
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(this.formatEdgeName(edge), this.formatEdgeValue(edge));
        return entry;
    }

    @Override
    public HugeEdge readEdge(HugeGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        // TODO: implement
        throw new NotImplementedException("Unsupported readEdge()");
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        TextBackendEntry entry = newBackendEntry(index.type(), index.id());
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(formatSyspropName(HugeKeys.INDEX_LABEL_ID),
                         writeId(index.indexLabel()));
        } else {
            // TODO: field-values may be a number (range index)
            entry.column(formatSyspropName(HugeKeys.FIELD_VALUES),
                         JsonUtil.toJson(index.fieldValues()));
            entry.column(formatSyspropName(HugeKeys.INDEX_LABEL_ID),
                         writeId(index.indexLabel()));
            entry.column(formatSyspropName(HugeKeys.ELEMENT_IDS),
                         writeIds(index.elementIds()));
            entry.subId(index.elementId());
        }
        return entry;
    }

    @Override
    public HugeIndex readIndex(HugeGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        String indexValues = entry.column(
                formatSyspropName(HugeKeys.FIELD_VALUES));
        String indexLabelId = entry.column(
                formatSyspropName(HugeKeys.INDEX_LABEL_ID));
        String elementIds = entry.column(
                formatSyspropName(HugeKeys.ELEMENT_IDS));

        IndexLabel indexLabel = IndexLabel.label(graph, readId(indexLabelId));
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(JsonUtil.fromJson(indexValues, Object.class));
        index.elementIds(readIds(elementIds));
        return index;
    }

    @Override
    public TextBackendEntry writeId(HugeType type, Id id) {
        id = this.writeQueryId(type, id);
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(HugeType type, Id id) {
        if (type.isEdge()) {
            id = IdGenerator.of(writeEdgeId(id, true));
        } else if (type.isGraph()) {
            id = IdGenerator.of(writeEntryId(id));
        } else {
            assert type.isSchema();
            id = IdGenerator.of(writeId(id));
        }
        return id;
    }

    @Override
    protected Id writeQueryEdgeCondition(Query query) {
        // Convert query-by-condition to query-by-id
        List<String> condParts = new ArrayList<>(query.conditions().size());

        for (HugeKeys key : EdgeId.KEYS) {
            Object value = ((ConditionQuery) query).condition(key);
            if (value == null) {
                break;
            }
            // Serialize condition value
            if (key == HugeKeys.OWNER_VERTEX || key == HugeKeys.OTHER_VERTEX) {
                condParts.add(writeEntryId((Id) value));
            } else if (key == HugeKeys.DIRECTION) {
                condParts.add(writeType(((Directions) value).type()));
            } else if (key == HugeKeys.LABEL) {
                condParts.add(writeId((Id) value));
            } else {
                condParts.add(value.toString());
            }
        }

        if (condParts.size() > 0) {
            // Conditions to id
            String id = EdgeId.concat(condParts.toArray(new String[0]));
            return IdGenerator.of(id);
        }

        return null;
    }

    @Override
    protected void writeQueryCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        // No user-prop when serialize
        assert result.allSysprop();
        for (Condition.Relation r : result.relations()) {
            // Serialize key
            if (query.resultType().isSchema()) {
                r.serialKey(((HugeKeys) r.key()).string());
            } else {
                r.serialKey(formatSyspropName((HugeKeys) r.key()));
            }

            if (r.value() instanceof Id) {
                // Serialize id value
                r.serialValue(writeId((Id) r.value()));
            } else {
                // Serialize other type value
                r.serialValue(JsonUtil.toJson(r.value()));
            }

            if (r.relation() == Condition.RelationType.CONTAINS_KEY) {
                // Serialize has-key
                String key = (String) r.serialValue();
                r.serialValue(formatPropertyName(key));
            }
        }
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        TextBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(vertexLabel.name()));
        entry.column(HugeKeys.ID_STRATEGY,
                     JsonUtil.toJson(vertexLabel.idStrategy()));
        entry.column(HugeKeys.PROPERTIES,
                     writeIds(vertexLabel.properties()));
        entry.column(HugeKeys.PRIMARY_KEYS,
                     writeIds(vertexLabel.primaryKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     writeIds(vertexLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     writeIds(vertexLabel.indexLabels()));
        entry.column(HugeKeys.ENABLE_LABEL_INDEX,
                     JsonUtil.toJson(vertexLabel.enableLabelIndex()));
        writeUserData(vertexLabel, entry);
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String idStrategy = entry.column(HugeKeys.ID_STRATEGY);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String primaryKeys = entry.column(HugeKeys.PRIMARY_KEYS);
        String nullableKeys = entry.column(HugeKeys.NULLABLE_KEYS);
        String indexLabels = entry.column(HugeKeys.INDEX_LABELS);
        String enableLabelIndex = entry.column(HugeKeys.ENABLE_LABEL_INDEX);

        VertexLabel vertexLabel = new VertexLabel(graph, id, name);
        vertexLabel.idStrategy(JsonUtil.fromJson(idStrategy,
                                                 IdStrategy.class));
        vertexLabel.properties(readIds(properties));
        vertexLabel.primaryKeys(readIds(primaryKeys));
        vertexLabel.nullableKeys(readIds(nullableKeys));
        vertexLabel.indexLabels(readIds(indexLabels));
        vertexLabel.enableLabelIndex(JsonUtil.fromJson(enableLabelIndex,
                                                       Boolean.class));
        readUserData(vertexLabel, entry);
        return vertexLabel;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        TextBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(edgeLabel.name()));
        entry.column(HugeKeys.SOURCE_LABEL, writeId(edgeLabel.sourceLabel()));
        entry.column(HugeKeys.TARGET_LABEL, writeId(edgeLabel.targetLabel()));
        entry.column(HugeKeys.FREQUENCY,
                     JsonUtil.toJson(edgeLabel.frequency()));
        entry.column(HugeKeys.PROPERTIES, writeIds(edgeLabel.properties()));
        entry.column(HugeKeys.SORT_KEYS, writeIds(edgeLabel.sortKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     writeIds(edgeLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS, writeIds(edgeLabel.indexLabels()));
        entry.column(HugeKeys.ENABLE_LABEL_INDEX,
                     JsonUtil.toJson(edgeLabel.enableLabelIndex()));
        writeUserData(edgeLabel, entry);
        return entry;
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph,
                                   BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String sourceLabel = entry.column(HugeKeys.SOURCE_LABEL);
        String targetLabel = entry.column(HugeKeys.TARGET_LABEL);
        String frequency = entry.column(HugeKeys.FREQUENCY);
        String sortKeys = entry.column(HugeKeys.SORT_KEYS);
        String nullablekeys = entry.column(HugeKeys.NULLABLE_KEYS);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String indexLabels = entry.column(HugeKeys.INDEX_LABELS);
        String enableLabelIndex = entry.column(HugeKeys.ENABLE_LABEL_INDEX);

        EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
        edgeLabel.sourceLabel(readId(sourceLabel));
        edgeLabel.targetLabel(readId(targetLabel));
        edgeLabel.frequency(JsonUtil.fromJson(frequency, Frequency.class));
        edgeLabel.properties(readIds(properties));
        edgeLabel.sortKeys(readIds(sortKeys));
        edgeLabel.nullableKeys(readIds(nullablekeys));
        edgeLabel.indexLabels(readIds(indexLabels));
        edgeLabel.enableLabelIndex(JsonUtil.fromJson(enableLabelIndex,
                                                     Boolean.class));
        readUserData(edgeLabel, entry);
        return edgeLabel;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        TextBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(propertyKey.name()));
        entry.column(HugeKeys.DATA_TYPE,
                     JsonUtil.toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY,
                     JsonUtil.toJson(propertyKey.cardinality()));
        entry.column(HugeKeys.PROPERTIES, writeIds(propertyKey.properties()));
        writeUserData(propertyKey, entry);
        return entry;
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String dataType = entry.column(HugeKeys.DATA_TYPE);
        String cardinality = entry.column(HugeKeys.CARDINALITY);
        String properties = entry.column(HugeKeys.PROPERTIES);

        PropertyKey propertyKey = new PropertyKey(graph, id, name);
        propertyKey.dataType(JsonUtil.fromJson(dataType, DataType.class));
        propertyKey.cardinality(JsonUtil.fromJson(cardinality,
                                                  Cardinality.class));
        propertyKey.properties(readIds(properties));
        readUserData(propertyKey, entry);
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        TextBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(indexLabel.name()));
        entry.column(HugeKeys.BASE_TYPE,
                     JsonUtil.toJson(indexLabel.baseType()));
        entry.column(HugeKeys.BASE_VALUE, writeId(indexLabel.baseValue()));
        entry.column(HugeKeys.INDEX_TYPE,
                     JsonUtil.toJson(indexLabel.indexType()));
        entry.column(HugeKeys.FIELDS, writeIds(indexLabel.indexFields()));
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String baseType = entry.column(HugeKeys.BASE_TYPE);
        String baseValue = entry.column(HugeKeys.BASE_VALUE);
        String indexType = entry.column(HugeKeys.INDEX_TYPE);
        String indexFields = entry.column(HugeKeys.FIELDS);

        IndexLabel indexLabel = new IndexLabel(graph, id, name);
        indexLabel.baseType(JsonUtil.fromJson(baseType, HugeType.class));
        indexLabel.baseValue(readId(baseValue));
        indexLabel.indexType(JsonUtil.fromJson(indexType, IndexType.class));
        indexLabel.indexFields(readIds(indexFields));
        return indexLabel;
    }

    private String writeEdgeId(Id id, boolean withOwnerVertex) {
        EdgeId edgeId;
        if (id instanceof EdgeId) {
            edgeId = (EdgeId) id;
        } else {
            edgeId = EdgeId.parse(id.asString());
        }
        List<String> list = new ArrayList<>(5);
        if (withOwnerVertex) {
            list.add(writeEntryId(edgeId.ownerVertexId()));
        }
        // Edge name: type + edge-label-name + sortKeys + targetVertex
        list.add(writeType(edgeId.direction().type()));
        list.add(writeId(edgeId.edgeLabelId()));
        list.add(edgeId.sortValues());
        list.add(writeEntryId(edgeId.otherVertexId()));

        return EdgeId.concat(list.toArray(new String[0]));
    }

    private static String writeType(HugeType type) {
        return type.string();
    }

    private static String writeEntryId(Id id) {
        return IdUtil.writeString(id);
    }

    private static Id readEntryId(String id) {
        return IdUtil.readString(id);
    }

    private static String writeId(Id id) {
        if (id.number()) {
            return JsonUtil.toJson(id.asLong());
        } else {
            return JsonUtil.toJson(id.asString());
        }
    }

    private static Id readId(String id) {
        Object value = JsonUtil.fromJson(id, Object.class);
        if (value instanceof Number) {
            return IdGenerator.of(((Number) value).longValue());
        } else {
            assert value instanceof String;
            return IdGenerator.of(value.toString());
        }
    }

    private static Id readId(Id id) {
        return readId(id.asString());
    }

    private static String writeIds(Collection<Id> ids) {
        Object[] array = new Object[ids.size()];
        int i = 0;
        for (Id id : ids) {
            if (id.number()) {
                array[i++] = id.asLong();
            } else {
                array[i++] = id.asString();
            }
        }
        return JsonUtil.toJson(array);
    }

    private static Id[] readIds(String str) {
        Object[] values = JsonUtil.fromJson(str, Object[].class);
        Id[] ids = new Id[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value instanceof Number) {
                ids[i] = IdGenerator.of(((Number) value).longValue());
            } else {
                assert value instanceof String;
                ids[i] = IdGenerator.of(value.toString());
            }
        }
        return ids;
    }

    private static void writeUserData(SchemaElement schema,
                                      TextBackendEntry entry) {
        entry.column(HugeKeys.USER_DATA, JsonUtil.toJson(schema.userData()));
    }

    private static void readUserData(SchemaElement schema,
                                     TextBackendEntry entry) {
        // Parse all user data of a schema element
        String userDataStr = entry.column(HugeKeys.USER_DATA);
        @SuppressWarnings("unchecked")
        Map<String, Object> userData = JsonUtil.fromJson(userDataStr,
                                                         Map.class);
        for (String key : userData.keySet()) {
            schema.userData(key, userData.get(key));
        }
    }
}
