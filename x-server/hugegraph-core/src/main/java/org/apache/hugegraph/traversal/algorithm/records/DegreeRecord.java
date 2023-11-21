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

package org.apache.hugegraph.traversal.algorithm.records;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.backend.id.Id;

public class DegreeRecord {
    Map<Id, AtomicLong> record;
    long degree;

    public DegreeRecord(Collection<Id> ids, long degree) {
        this.degree = degree;
        record = new HashMap<>();
        for (Id id : ids) {
            record.put(id, new AtomicLong());
        }
    }

    public boolean putAndCheck(Id id) {
        AtomicLong atomicLong = record.get(id);
        if (atomicLong == null) {
            return false;
        }

        if (atomicLong.longValue() < degree) {
            return atomicLong.addAndGet(1) <= degree;
        }

        return false;
    }
}
