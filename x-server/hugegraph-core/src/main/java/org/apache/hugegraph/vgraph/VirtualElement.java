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

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeProperty;

public abstract class VirtualElement {

    protected static final BinarySerializer SERIALIZER = new BinarySerializer();

    protected byte status;
    protected long expiredTime;
    protected ByteBufferWrapper propertyBuf;


    protected VirtualElement(HugeElement element,
                             byte status) {
        assert element != null;
        this.expiredTime = element.expiredTime();
        this.status = status;
        if (element.hasProperties()) {
            setProperties(element.getProperties().values());
        }
    }

    protected VirtualElement() {
    }

    protected BytesBuffer getPropertiesBufForRead() {
        assert propertyBuf != null;
        ByteBuffer byteBuffer = propertyBuf.getByteBuffer();
        return BytesBuffer.wrap(byteBuffer.array(), propertyBuf.getOffset(),
                                byteBuffer.position() - propertyBuf.getOffset());
    }

    public void fillProperties(HugeElement owner) {
        if (propertyBuf != null) {
            SERIALIZER.parseProperties(getPropertiesBufForRead(), owner);
        }
    }

    public void setProperties(Collection<HugeProperty<?>> properties) {
        if (properties != null && !properties.isEmpty()) {
            BytesBuffer buffer = new BytesBuffer();
            SERIALIZER.formatProperties(properties, buffer);
            ByteBuffer innerBuffer = buffer.asByteBuffer();
            propertyBuf = new ByteBufferWrapper(innerBuffer.arrayOffset(), innerBuffer);
        } else {
            propertyBuf = null;
        }
    }

    public void copyProperties(VirtualElement element) {
        propertyBuf = element.propertyBuf;
    }

    public byte getStatus() {
        return status;
    }
}
