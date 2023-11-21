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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.type.HugeType;

public class VirtualGraphQueryTask<T extends VirtualElement> {

    private final HugeType hugeType;
    private final List<Id> ids;
    private final CompletableFuture<Iterator<T>> future;

    public VirtualGraphQueryTask(HugeType hugeType, List<Id> ids) {
        assert hugeType == HugeType.VERTEX || hugeType == HugeType.EDGE;
        assert ids != null;

        this.hugeType = hugeType;
        this.ids = ids;
        this.future = new CompletableFuture<>();
    }

    public HugeType getHugeType() {
        return hugeType;
    }

    public List<Id> getIds() {
        return ids;
    }

    public CompletableFuture<Iterator<T>> getFuture() {
        return future;
    }
}