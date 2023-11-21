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

package org.apache.hugegraph.serializer;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser.CrosspointsPaths;
import org.apache.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.WeightedPaths;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface Serializer {

    public String writeMap(Map<?, ?> map);

    public String writeList(String label, Collection<?> list);

    public String writePropertyKey(PropertyKey propertyKey);

    public String writePropertyKeys(List<PropertyKey> propertyKeys);

    public String writeVertexLabel(VertexLabel vertexLabel);

    public String writeVertexLabels(List<VertexLabel> vertexLabels);

    public String writeEdgeLabel(EdgeLabel edgeLabel);

    public String writeEdgeLabels(List<EdgeLabel> edgeLabels);

    public String writeIndexlabel(IndexLabel indexLabel);

    public String writeIndexlabels(List<IndexLabel> indexLabels);

    public String writeTaskWithSchema(SchemaElement.TaskWithSchema tws);

    public String writeVertex(Vertex v);

    public String writeVertices(Iterator<Vertex> vertices, boolean paging);

    public String writeEdge(Edge e);

    public String writeEdges(Iterator<Edge> edges, boolean paging);

    public String writeIds(List<Id> ids);

    public String writeAuthElement(AuthElement elem);

    public <V extends AuthElement> String writeAuthElements(String label,
                                                            List<V> users);

    public String writePaths(String name, Collection<HugeTraverser.Path> paths,
                             boolean withCrossPoint, Iterator<Vertex> vertices,
                             Iterator<Edge> edges);

    public default String writePaths(String name,
                                     Collection<HugeTraverser.Path> paths,
                                     boolean withCrossPoint) {
        return this.writePaths(name, paths, withCrossPoint, null, null);
    }

    public String writeCrosspoints(CrosspointsPaths paths,
                                   Iterator<Vertex> iterator, boolean withPath);

    public String writeSimilars(SimilarsMap similars,
                                Iterator<Vertex> vertices);

    public String writeWeightedPath(NodeWithWeight path,
                                    Iterator<Vertex> vertices);

    public String writeWeightedPaths(WeightedPaths paths,
                                     Iterator<Vertex> vertices);

    public String writeNodesWithPath(String name, List<Id> nodes, long size,
                                     Collection<HugeTraverser.Path> paths,
                                     Iterator<Vertex> vertices,
                                     Iterator<Edge> edges, List<Integer> sizes);

    public String writeGraphSpace(GraphSpace graphSpace);

    public String writeService(Service service);

    public String writeSchemaTemplate(SchemaTemplate template);
}
