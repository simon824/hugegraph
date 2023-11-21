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

package org.apache.hugegraph.auth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine.Relationship;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

public class HugeAccess extends Relationship {

    private static final long serialVersionUID = -7644007602408729385L;

    private String graphSpace;
    private Id role;
    private Id target;
    private HugePermission permission;
    private String description;

    public HugeAccess(String graphSpace, Id role, Id target) {
        this(graphSpace, role, target, null);
    }

    public HugeAccess(String graphSpace, Id role, Id target, HugePermission permission) {
        this.graphSpace = graphSpace;
        this.role = role;
        this.target = target;
        this.permission = permission;
        this.description = null;
    }

    public static String accessId(String roleName, String targetName, String code) {
        E.checkArgument(StringUtils.isNotEmpty(roleName) &&
                        StringUtils.isNotEmpty(targetName),
                        "The role name '%s' or target name '%s' is empty",
                        roleName, targetName);
        return String.join("->", roleName, code, targetName);
    }

    public static HugeAccess fromMap(Map<String, Object> map) {
        HugeAccess access = new HugeAccess("", null, null);
        return fromMap(map, access);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
    }

    @Override
    public String label() {
        return P.ACCESS;
    }

    @Override
    public String sourceLabel() {
        return P.ROLE;
    }

    @Override
    public String targetLabel() {
        return P.TARGET;
    }

    @Override
    public String graphSpace() {
        return this.graphSpace;
    }

    @Override
    public Id source() {
        return this.role;
    }

    @Override
    public Id target() {
        return this.target;
    }

    public HugePermission permission() {
        return this.permission;
    }

    public void permission(HugePermission permission) {
        this.permission = permission;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("HugeAccess(%s->%s)", this.role, this.target);
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.GRAPHSPACE:
                this.graphSpace = (String) value;
                break;
            case P.ROLE:
                this.role = IdGenerator.of((String) value);
                break;
            case P.TARGET:
                this.target = IdGenerator.of((String) value);
                break;
            case P.PERMISSION:
                this.permission = HugePermission.valueOf(value.toString());
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        List<Object> list = new ArrayList<>(12);

        list.add(T.label);
        list.add(P.ACCESS);

        list.add(P.GRAPHSPACE);
        list.add(this.graphSpace);

        list.add(P.ROLE);
        list.add(this.role);

        list.add(P.TARGET);
        list.add(this.target);

        list.add(P.PERMISSION);
        list.add(this.permission);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GRAPHSPACE), this.graphSpace);
        map.put(Hidden.unHide(P.ROLE), this.role);
        map.put(Hidden.unHide(P.TARGET), this.target);
        map.put(Hidden.unHide(P.PERMISSION), this.permission);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    @Override
    public void setId() {
        String opCode = String.valueOf(this.permission.code());
        String accessId = accessId(this.source().asString(),
                                   this.target.asString(),
                                   opCode);
        this.id(IdGenerator.of(accessId));
    }

    public static final class P {

        public static final String ACCESS = Hidden.hide("access");

        public static final String LABEL = T.label.getAccessor();

        public static final String GRAPHSPACE = "~graphspace";

        public static final String ROLE = "~role";    // HugeRole.P.ROLE;
        public static final String TARGET = "~target";   // HugeTarget.P.TARGET;

        public static final String PERMISSION = "~access_permission";
        public static final String DESCRIPTION = "~access_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("access_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.ACCESS);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create edge label
            EdgeLabel label = this.schema().edgeLabel(this.label)
                                  .sourceLabel(P.ROLE)
                                  .targetLabel(P.TARGET)
                                  .properties(properties)
                                  .nullableKeys(P.DESCRIPTION)
                                  .sortKeys(P.PERMISSION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.PERMISSION, DataType.BYTE));
            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }
}
