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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;

public class VirtualVertex extends VirtualElement {

    private Id id;
    private VertexLabel label;
    private BytesBuffer outEdgesBuf;
    private BytesBuffer inEdgesBuf;

    public VirtualVertex(HugeVertex vertex, byte status) {
        super(vertex, status);
        assert vertex != null;
        this.id = vertex.id();
        this.label = vertex.schemaLabel();
        this.outEdgesBuf = null;
        this.inEdgesBuf = null;
    }

    public HugeVertex getVertex(HugeGraph graph) {
        HugeVertex vertex = new HugeVertex(graph, id, label);
        vertex.expiredTime(this.expiredTime);
        this.fillProperties(vertex);
        return vertex;
    }

    public Iterator<VirtualEdge> getEdges(HugeGraph graph) {
        ExtendableIterator<VirtualEdge> result = new ExtendableIterator<>();
        List<VirtualEdge> outEdges = readEdgesFromBuffer(this.outEdgesBuf, graph, Directions.OUT);
        if (outEdges != null) {
            result.extend(outEdges.listIterator());
        }
        List<VirtualEdge> inEdges = readEdgesFromBuffer(this.inEdgesBuf, graph, Directions.IN);
        if (inEdges != null) {
            result.extend(inEdges.listIterator());
        }
        return result;
    }

    public void addOutEdges(List<VirtualEdge> edges) {
        this.outEdgesBuf = writeEdgesToBuffer(edges);
    }

    public void addInEdges(List<VirtualEdge> edges) {
        this.inEdgesBuf = writeEdgesToBuffer(edges);
    }

    public void copyInEdges(VirtualVertex other) {
        this.inEdgesBuf = other.inEdgesBuf;
    }

    private BytesBuffer writeEdgesToBuffer(List<VirtualEdge> edges) {
        assert edges != null;
        if (edges.size() > 0) {
            BytesBuffer buffer = new BytesBuffer();
            // Write edge list size
            buffer.writeVInt(edges.size());
            // Write edge
            for (VirtualEdge edge : edges) {
                edge.writeToBuffer(buffer);
            }
            return buffer;
        } else {
            return null;
        }
    }

    private List<VirtualEdge> readEdgesFromBuffer(BytesBuffer buffer, HugeGraph graph,
                                                  Directions directions) {
        if (buffer != null) {
            ByteBuffer byteBuffer = buffer.asByteBuffer();
            BytesBuffer wrapedBuffer = BytesBuffer.wrap(byteBuffer.array(),
                                                        byteBuffer.arrayOffset(),
                                                        byteBuffer.position());

            int size = wrapedBuffer.readVInt();
            assert size >= 0;
            List<VirtualEdge> result = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                VirtualEdge edge =
                        VirtualEdge.readFromBuffer(wrapedBuffer, graph, this.id, directions);
                result.add(edge);
            }
            return result;
        } else {
            return null;
        }
    }

    void orStatus(VirtualVertexStatus status) {
        this.status = status.or(this.status);
    }
}
