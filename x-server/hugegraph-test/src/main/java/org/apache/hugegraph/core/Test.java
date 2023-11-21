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

package org.apache.hugegraph.core;

import java.util.HashMap;

import org.apache.hugegraph.backend.store.hstore.HstoreTable;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.type.HugeType;

/**
 * @author zhangyingjie
 * @date 2022/8/29
 **/
public class Test {
    public static final byte[] EMPTY_BYTES = new byte[0];

    @org.junit.Test
    public void testHugeType() {
        HashMap<HugeType, HstoreTable> map = new HashMap<>();
        map.put(HugeType.UNKNOWN, new HstoreTable("11", "222"));
        map.put(HugeType.VERTEX_LABEL, new HstoreTable("11", "222"));
        map.put(HugeType.EDGE_LABEL, new HstoreTable("11", "222"));
        map.put(HugeType.PROPERTY_KEY, new HstoreTable("11", "222"));
        map.put(HugeType.INDEX_LABEL, new HstoreTable("11", "222"));
        map.put(HugeType.COUNTER, new HstoreTable("11", "222"));
        map.put(HugeType.VERTEX, new HstoreTable("11", "222"));
        map.put(HugeType.SYS_PROPERTY, new HstoreTable("11", "222"));
        map.put(HugeType.PROPERTY, new HstoreTable("11", "222"));
        map.put(HugeType.AGGR_PROPERTY_V, new HstoreTable("11", "222"));
        map.put(HugeType.AGGR_PROPERTY_E, new HstoreTable("11", "222"));
        map.put(HugeType.OLAP, new HstoreTable("11", "222"));
        map.put(HugeType.EDGE, new HstoreTable("11", "222"));
        map.put(HugeType.EDGE_OUT, new HstoreTable("11", "222"));
        map.put(HugeType.EDGE_IN, new HstoreTable("11", "222"));
        map.put(HugeType.SECONDARY_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.VERTEX_LABEL_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.EDGE_LABEL_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.RANGE_INT_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.RANGE_FLOAT_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.RANGE_LONG_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.RANGE_DOUBLE_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.SEARCH_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.SHARD_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.UNIQUE_INDEX, new HstoreTable("11", "222"));
        map.put(HugeType.TASK, new HstoreTable("11", "222"));
        map.put(HugeType.SYS_SCHEMA, new HstoreTable("11", "222"));
        map.put(HugeType.MAX_TYPE, new HstoreTable("11", "222"));
        System.out.println(map.size());
    }

    @org.junit.Test
    public void testGetData() {
        HgStoreClient storeClient;
        PDClient pdClient;
        storeClient = HgStoreClient.create(PDConfig.of("10.157.12.66:8686")
                                                   .setEnableCache(true));
        pdClient = storeClient.getPdClient();
        HgStoreSession session = storeClient.openSession("DEFAULT/hugegraph/g");
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator("g+v",
                                                                      0, 5462,
                                                                      HgKvStore.SCAN_HASHCODE,
                                                                      EMPTY_BYTES)) {
            int count = 0;
            if (iterators.hasNext()) {
                count++;
                HgKvEntry next = iterators.next();
                System.out.println(" " + new String(next.key()) + " " + new String(next.value()));
            }

        }
    }
}
