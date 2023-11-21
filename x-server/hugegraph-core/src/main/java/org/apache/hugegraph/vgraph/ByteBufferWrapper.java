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

public class ByteBufferWrapper {

    private int offset;
    private ByteBuffer byteBuffer;

    public ByteBufferWrapper(int offset, ByteBuffer byteBuffer) {
        this.offset = offset;
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getBytes() {
        int length = this.byteBuffer.position() - this.offset;
        byte[] result = new byte[length];
        ByteBuffer bufferWrapped = ByteBuffer.wrap(this.byteBuffer.array(), this.offset, length);
        bufferWrapped.get(result);
        return result;
    }
}
