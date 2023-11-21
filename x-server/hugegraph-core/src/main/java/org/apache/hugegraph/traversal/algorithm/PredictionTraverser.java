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

package org.apache.hugegraph.traversal.algorithm;

import static java.lang.Math.log;

import java.util.Set;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.exception.ErrorCode;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;

public class PredictionTraverser extends OltpTraverser {

    public PredictionTraverser(HugeGraph graph) {
        super(graph);
    }

    private static void reachCapacity(long count, long capacity) {
        if (capacity != NO_LIMIT && count > capacity) {
            throw new HugeException(ErrorCode.REACH_CAPACITY, capacity);
        }
    }

    public double adamicAdar(Id source, Id target, Directions dir,
                             String label, long degree, long limit) {
        Set<Id> neighbors = checkAndGetCommonNeighbors(source, target, dir,
                                                       label, degree, limit);
        Id labelId = label == null ? null : graph().edgeLabel(label).id();
        return neighbors.stream()
                        .mapToDouble(vid -> 1.0 / log(vertexDegree(vid, dir, labelId)))
                        .sum();
    }

    public double resourceAllocation(Id source, Id target, Directions dir,
                                     String label, long degree, long limit) {
        Set<Id> neighbors = checkAndGetCommonNeighbors(source, target, dir,
                                                       label, degree, limit);
        Id labelId = label == null ? null : graph().edgeLabel(label).id();
        return neighbors.stream()
                        .mapToDouble(vid -> 1.0 / vertexDegree(vid, dir, labelId))
                        .sum();
    }

    private Set<Id> checkAndGetCommonNeighbors(Id source, Id target,
                                               Directions dir, String label,
                                               long degree, long limit) {
        E.checkNotNull(source, "source id");
        E.checkNotNull(target, "the target id");
        this.checkVertexExist(source, "vertex");
        this.checkVertexExist(target, "other");
        E.checkNotNull(dir, "direction");
        checkDegree(degree);
        SameNeighborTraverser traverser = new SameNeighborTraverser(graph());
        Set<Id> result = traverser.sameNeighbors(source, target, dir,
                                                 label, degree, limit);
        this.edgeIterCounter.addAndGet(traverser.edgeIterCounter.get());
        this.vertexIterCounter.addAndGet(traverser.vertexIterCounter.get());
        return result;
    }

    public double computeInterUnion(Set<Id> set1, Set<Id> set2) {
        int interNum = CollectionUtil.intersect(set1, set2).size();
        int unionNum = CollectionUtil.union(set1, set2).size();
        if (unionNum == 0) {
            return 0.0D;
        }
        return (double) interNum / unionNum;
    }
}
