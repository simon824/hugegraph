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

package org.apache.hugegraph.unit;

import org.apache.hugegraph.unit.cache.CacheManagerTest;
import org.apache.hugegraph.unit.cache.CacheTest;
import org.apache.hugegraph.unit.cache.CachedGraphTransactionTest;
import org.apache.hugegraph.unit.cache.CachedSchemaTransactionTest;
import org.apache.hugegraph.unit.core.AnalyzerTest;
import org.apache.hugegraph.unit.core.BackendMutationTest;
import org.apache.hugegraph.unit.core.BackendStoreSystemInfoTest;
import org.apache.hugegraph.unit.core.ConditionQueryFlattenTest;
import org.apache.hugegraph.unit.core.ConditionTest;
import org.apache.hugegraph.unit.core.DataTypeTest;
import org.apache.hugegraph.unit.core.DirectionsTest;
import org.apache.hugegraph.unit.core.ExceptionTest;
import org.apache.hugegraph.unit.core.IdSetTest;
import org.apache.hugegraph.unit.core.Int2IntsMapTest;
import org.apache.hugegraph.unit.core.LocksTableTest;
import org.apache.hugegraph.unit.core.ObjectIntMappingTest;
import org.apache.hugegraph.unit.core.PageStateTest;
import org.apache.hugegraph.unit.core.QueryTest;
import org.apache.hugegraph.unit.core.RangeTest;
import org.apache.hugegraph.unit.core.RolePermissionTest;
import org.apache.hugegraph.unit.core.RowLockTest;
import org.apache.hugegraph.unit.core.SerialEnumTest;
import org.apache.hugegraph.unit.core.TraversalUtilTest;
import org.apache.hugegraph.unit.id.EdgeIdTest;
import org.apache.hugegraph.unit.id.IdTest;
import org.apache.hugegraph.unit.id.IdUtilTest;
import org.apache.hugegraph.unit.id.SplicingIdGeneratorTest;
import org.apache.hugegraph.unit.rocksdb.RocksDBCountersTest;
import org.apache.hugegraph.unit.rocksdb.RocksDBSessionTest;
import org.apache.hugegraph.unit.rocksdb.RocksDBSessionsTest;
import org.apache.hugegraph.unit.serializer.BinaryBackendEntryTest;
import org.apache.hugegraph.unit.serializer.BinarySerializerTest;
import org.apache.hugegraph.unit.serializer.BytesBufferTest;
import org.apache.hugegraph.unit.serializer.SerializerFactoryTest;
import org.apache.hugegraph.unit.serializer.StoreSerializerTest;
import org.apache.hugegraph.unit.util.CollectionFactoryTest;
import org.apache.hugegraph.unit.util.CompressUtilTest;
import org.apache.hugegraph.unit.util.JsonUtilTest;
import org.apache.hugegraph.unit.util.RateLimiterTest;
import org.apache.hugegraph.unit.util.StringEncodingTest;
import org.apache.hugegraph.unit.util.VersionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        /* cache */
        CacheTest.RamCacheTest.class,
        CacheTest.OffheapCacheTest.class,
        CacheTest.LevelCacheTest.class,
        CachedSchemaTransactionTest.class,
        CachedGraphTransactionTest.class,
        CacheManagerTest.class,

        /* types */
        DataTypeTest.class,
        DirectionsTest.class,
        SerialEnumTest.class,

        /* id */
        IdTest.class,
        EdgeIdTest.class,
        IdUtilTest.class,
        SplicingIdGeneratorTest.class,

        /* core */
        LocksTableTest.class,
        RowLockTest.class,
        AnalyzerTest.class,
        BackendMutationTest.class,
        ConditionTest.class,
        ConditionQueryFlattenTest.class,
        QueryTest.class,
        RangeTest.class,
//    TODO: fix and remove annotation
//    SecurityManagerTest.class,
        RolePermissionTest.class,
        ExceptionTest.class,
        BackendStoreSystemInfoTest.class,
        TraversalUtilTest.class,
        PageStateTest.class,
        Int2IntsMapTest.class,
        ObjectIntMappingTest.class,
        IdSetTest.class,

        /* serializer */
        BytesBufferTest.class,
        SerializerFactoryTest.class,
        BinaryBackendEntryTest.class,
        BinarySerializerTest.class,
        StoreSerializerTest.class,

        /* rocksdb */
        RocksDBSessionsTest.class,
        RocksDBSessionTest.class,
        RocksDBCountersTest.class,

        /* utils */
        VersionTest.class,
        JsonUtilTest.class,
        StringEncodingTest.class,
        CompressUtilTest.class,
        CollectionFactoryTest.class,
        RateLimiterTest.FixedTimerWindowRateLimiterTest.class,
        RateLimiterTest.FixedWatchWindowRateLimiterTest.class
})
public class UnitTestSuite {
}
