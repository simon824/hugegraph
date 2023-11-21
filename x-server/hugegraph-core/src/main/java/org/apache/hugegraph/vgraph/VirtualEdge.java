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

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;

public class VirtualEdge extends VirtualElement {

    private Id ownerVertexId;
    private Directions directions;
    private EdgeLabel edgeLabel;
    private String name;
    private Id otherVertexId;

    public VirtualEdge(HugeEdge edge, byte status) {
        super(edge, status);
        this.ownerVertexId = edge.id().ownerVertexId();
        this.directions = edge.direction();
        this.edgeLabel = edge.schemaLabel();
        this.name = edge.name();
        this.otherVertexId = edge.id().otherVertexId();
    }

    private VirtualEdge() {
        super();
    }

    public static VirtualEdge readFromBuffer(BytesBuffer buffer, HugeGraph graph, Id ownerVertexId,
                                             Directions directions) {
        VirtualEdge edge = new VirtualEdge();
        edge.ownerVertexId = ownerVertexId;
        edge.status = buffer.read();
        edge.directions = directions;
        edge.edgeLabel = graph.edgeLabelOrNone(buffer.readId());
        edge.name = buffer.readString();
        edge.otherVertexId = buffer.readId();
        int length = buffer.readVInt();
        if (length > 0) {
            BytesBuffer bytesBuffer =
                    BytesBuffer.wrap(buffer.array(), buffer.position(), length).forReadAll();
            edge.propertyBuf = new ByteBufferWrapper(buffer.position(), bytesBuffer.asByteBuffer());
            ByteBuffer innerBuffer = buffer.asByteBuffer();
            innerBuffer.position(bytesBuffer.position());
        }
        if (edge.edgeLabel.ttl() > 0L) {
            edge.expiredTime = buffer.readVLong();
        }
        return edge;
    }

    public Id getOwnerVertexId() {
        return this.ownerVertexId;
    }

    public HugeEdge getEdge(HugeVertex owner) {
        assert owner.id().equals(this.ownerVertexId);
        boolean direction = EdgeId.isOutDirectionFromCode(this.directions.type().code());
        HugeEdge edge = HugeEdge.constructEdge(owner, direction, this.edgeLabel, this.name,
                                               this.otherVertexId);
        edge.expiredTime(this.expiredTime);
        this.fillProperties(edge);
        return edge;
    }

    void orStatus(VirtualEdgeStatus status) {
        this.status = status.or(this.status);
    }

    public void writeToBuffer(BytesBuffer buffer) {
        buffer.write(this.status);
        buffer.writeId(this.edgeLabel.id());
        buffer.writeString(this.name);
        buffer.writeId(this.otherVertexId);
        if (this.propertyBuf == null) {
            buffer.writeVInt(0);
        } else {
            buffer.writeBytes(this.propertyBuf.getBytes());
        }
        if (this.edgeLabel.ttl() > 0L) {
            buffer.writeVLong(this.expiredTime);
        }
    }
}
