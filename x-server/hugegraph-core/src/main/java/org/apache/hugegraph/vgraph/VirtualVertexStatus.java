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

package org.apache.hugegraph.vgraph;

public enum VirtualVertexStatus {
    None(0x00, "none"),
    Id(0x01, "id"),
    Property(0x02, "property"),
    OutEdge(0x04, "out_edge"),
    InEdge(0x08, "in_edge"),
    AllEdge(OutEdge.code | InEdge.code, "all_edge"),
    OK((Id.code | Property.code | OutEdge.code | InEdge.code), "ok");

    private byte code;
    private String name;

    VirtualVertexStatus(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public static VirtualVertexStatus fromCode(byte code) {
        switch (code) {
            case 0x00:
                return None;
            case 0x01:
                return Id;
            case 0x02:
                return Property;
            case 0x04:
                return OutEdge;
            case 0x08:
                return InEdge;
            case 0x0c:
                return AllEdge;
            case 0x0f:
                return OK;
            default:
                throw new IllegalStateException("Unexpected value: " + code);
        }
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public boolean match(VirtualVertexStatus other) {
        return (this.code & other.code) == this.code;
    }

    public boolean match(byte other) {
        return (this.code & other) == this.code;
    }

    public VirtualVertexStatus or(VirtualVertexStatus other) {
        return fromCode(or(other.code));
    }

    public byte or(byte other) {
        return ((byte) (this.code | other));
    }
}
