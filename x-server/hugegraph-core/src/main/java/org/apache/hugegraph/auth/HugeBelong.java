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

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine.Relationship;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

import com.google.common.collect.ImmutableList;

public class HugeBelong extends Relationship {

    public static final String UG = "ug";
    public static final String UR = "ur";
    public static final String GR = "gr";
    public static final String ALL = "*";
    private static final long serialVersionUID = -7242751631755533423L;
    private String graphSpace;
    private Id user;
    private Id group;
    private Id role;
    private String description;
    private String link;

    public HugeBelong(Id user, Id group) {
        this("*", user, group, null, UG);
    }

    public HugeBelong(String graphSpace, Id user, Id role) {
        this(graphSpace, user, null, role, UR);
    }

    public HugeBelong(String graphSpace, Id user, Id group,
                      Id role, String link) {
        this.graphSpace = graphSpace;
        this.user = user;
        this.group = group;
        this.role = role;
        this.description = null;
        this.link = link;
    }

    public static boolean isLink(String link) {
        List<String> linkList = ImmutableList.of(UG, UR, GR);
        return linkList.contains(link);
    }

    public static HugeBelong fromMap(Map<String, Object> map) {
        HugeBelong belong = new HugeBelong("", null, null, null,
                                           null);
        return fromMap(map, belong);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    @Override
    public void setId() {
        String belongId = String.join("->", this.source().asString(),
                                      this.link,
                                      this.target().asString());
        this.id(IdGenerator.of(belongId));
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
    }

    @Override
    public String label() {
        return P.BELONG;
    }

    @Override
    public String sourceLabel() {
        return P.USER;
    }

    @Override
    public String targetLabel() {
        return P.ROLE;
    }

    @Override
    public String graphSpace() {
        return this.graphSpace;
    }

    @Override
    public Id source() {
        if (GR.equals(this.link)) {
            return this.group;
        }
        return this.user;
    }

    public void source(Id id) {
        if (GR.equals(this.link)) {
            this.group = id;
        }
        this.user = id;
    }

    @Override
    public Id target() {
        if (UG.equals(this.link)) {
            return this.group;
        }
        return this.role;
    }

    public void target(Id id) {
        if (UG.equals(this.link)) {
            this.group = id;
        }
        this.role = id;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    public String link() {
        return this.link;
    }

    @Override
    public String toString() {
        return String.format("HugeBelong(%s->%s->%s)",
                             this.source(), this.link, this.target());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value) || value == null) {
            return true;
        }
        switch (key) {
            case P.GRAPHSPACE:
                this.graphSpace = (String) value;
                break;
            case P.USER:
                this.user = IdGenerator.of((String) value);
                break;
            case P.GROUP:
                this.group = IdGenerator.of((String) value);
                break;
            case P.ROLE:
                this.role = IdGenerator.of((String) value);
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            case P.LINK:
                this.link = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        List<Object> list = new ArrayList<>(10);

        list.add(T.label);
        list.add(this.label());

        list.add(P.GRAPHSPACE);
        list.add(this.graphSpace);

        list.add(P.USER);
        list.add(this.user);

        list.add(P.GROUP);
        list.add(this.group);

        list.add(P.ROLE);
        list.add(this.role);

        list.add(P.LINK);
        list.add(this.link);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GRAPHSPACE), this.graphSpace);
        map.put(Hidden.unHide(P.LINK), this.link);

        if (this.user != null) {
            map.put(Hidden.unHide(P.USER), this.user);
        }

        if (this.group != null) {
            map.put(Hidden.unHide(P.GROUP), this.group);
        }

        if (this.role != null) {
            map.put(Hidden.unHide(P.ROLE), this.role);
        }

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static final class P {

        public static final String BELONG = Hidden.hide("belong");
        public static final String LABEL = T.label.getAccessor();

        public static final String GRAPHSPACE = "~graphspace";

        public static final String USER = "~user";      //  HugeUser.P.USER;
        public static final String GROUP = "~group";    // HugeGroup.P.GROUP;
        public static final String ROLE = "~role";     // HugeRole.P.ROLE;

        public static final String DESCRIPTION = "~belong_description";
        public static final String LINK = "~link";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("belong_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.BELONG);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create edge label
            EdgeLabel label = this.schema().edgeLabel(this.label)
                                  .sourceLabel(P.USER)
                                  .targetLabel(P.ROLE)
                                  .properties(properties)
                                  .nullableKeys(P.DESCRIPTION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }
}
