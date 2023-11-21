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

import org.apache.hugegraph.type.define.SerialEnum;
import org.apache.hugegraph.util.E;

public enum HugePermission implements SerialEnum {

    NONE(0x00, "none"),

    READ(0x01, "read"),
    WRITE(0x02, "write"),
    DELETE(0x04, "delete"),
    EXECUTE(0x08, "execute"),

    SPACE(0x1f, "space"),
    ADMIN(0x7f, "admin");

    static {
        SerialEnum.register(HugePermission.class);
    }

    private byte code;
    private String name;

    HugePermission(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public static HugePermission fromCode(byte code) {
        return SerialEnum.fromCode(HugePermission.class, code);
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public boolean match(HugePermission other) {
        if (other == ADMIN) {
            return this == ADMIN;
        }
        return (this.code & other.code) != 0;
    }

    public boolean isCreatable() {
        // return true if this in NONE, READ, WRITE, DELETE, EXECUTE
        return (this.code & 0x10) == 0;
    }

    public void checkCreatable() {
        E.checkArgument(this.isCreatable(),
                        "The access_permission could only be NONE, READ, " +
                        "WRITE, DELETE or EXECUTE");
    }
}