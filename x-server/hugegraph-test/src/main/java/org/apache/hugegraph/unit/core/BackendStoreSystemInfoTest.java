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

package org.apache.hugegraph.unit.core;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.store.BackendStoreSystemInfo;
import org.apache.hugegraph.backend.tx.SchemaTransaction;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;
import org.mockito.Mockito;

public class BackendStoreSystemInfoTest {

    private static final String PK_BACKEND_INFO = "~backend_info";

    @Test
    public void testBackendStoreSystemInfoIllegalStateException() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        SchemaTransaction stx = Mockito.mock(SchemaTransaction.class);
        Mockito.when(stx.getPropertyKey(PK_BACKEND_INFO))
               .thenThrow(new IllegalStateException("Should not exist schema " +
                                                    "with same name '~backend_info'"));
        Mockito.when(stx.graph()).thenReturn(graph);

        BackendStoreSystemInfo info = new BackendStoreSystemInfo(stx);

        Assert.assertThrows(HugeException.class, () -> {
            Whitebox.invoke(BackendStoreSystemInfo.class, "info", info);
        }, e -> {
            Assert.assertContains("There exists multiple backend info",
                                  e.getMessage());
        });
    }
}
