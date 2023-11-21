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

public enum VirtualEdgeStatus {
    None(0x00, "none"),
    Id(0x01, "id"),
    Property(0x02, "property"),
    OK((Id.code | Property.code), "ok");

    private byte code;
    private String name;

    VirtualEdgeStatus(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public static VirtualEdgeStatus fromCode(byte code) {
        switch (code) {
            case 0x00:
                return None;
            case 0x01:
                return Id;
            case 0x02:
                return Property;
            case 0x03:
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

    public boolean match(VirtualEdgeStatus other) {
        return (this.code & other.code) == this.code;
    }

    public boolean match(byte other) {
        return (this.code & other) == this.code;
    }

    public VirtualEdgeStatus or(VirtualEdgeStatus other) {
        return fromCode(or(other.code));
    }

    public byte or(byte other) {
        return ((byte) (this.code | other));
    }
}
