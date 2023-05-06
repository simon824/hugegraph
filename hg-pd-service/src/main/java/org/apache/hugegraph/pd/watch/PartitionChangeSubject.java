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

package org.apache.hugegraph.pd.watch;

import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentNotNull;
import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentValid;

import javax.annotation.concurrent.ThreadSafe;

import com.baidu.hugegraph.pd.grpc.watch.WatchChangeType;
import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchType;

/**
 * The subject of partition change.
 *
 * @author lynn.bond@hotmail.com created on 2021/11/5
 */
@ThreadSafe
final class PartitionChangeSubject extends AbstractWatchSubject {

    PartitionChangeSubject() {
        super(WatchType.WATCH_TYPE_PARTITION_CHANGE);
    }

    @Override
    String toNoticeString(WatchResponse res) {
        StringBuilder sb = new StringBuilder();
        return sb.append("graph:").append(res.getPartitionResponse().getGraph())
                 .append(",")
                 .append("partitionId:").append(res.getPartitionResponse().getPartitionId())
                 .toString();
    }

    public void notifyWatcher(WatchChangeType changeType, String graph, int partitionId) {
        isArgumentNotNull(changeType, "changeType");
        isArgumentValid(graph, "graph");

        super.notifyWatcher(builder -> {
            builder.setPartitionResponse(
                    builder.getPartitionResponseBuilder().clear()
                           .setGraph(graph)
                           .setPartitionId(partitionId)
                           .setChangeType(changeType)
                           .build()
            );

        });
    }

}