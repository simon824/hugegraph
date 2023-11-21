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

package org.apache.hugegraph.job.schema;

import org.apache.hugegraph.backend.tx.SchemaTransaction;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class OlapPropertyKeyCreateCallable extends SchemaCallable {

    protected static final Logger LOG = Log.logger(OlapPropertyKeyCreateCallable.class);

    @Override
    public String type() {
        return CREATE_OLAP;
    }

    @Override
    public Object execute() {
        SchemaTransaction schemaTx = this.params().schemaTransaction();
        PropertyKey propertyKey = schemaTx.getPropertyKey(this.schemaId());
        // TODO: shall we sync 2 actions here?
        LOG.debug("start create index label for olap pk");
        schemaTx.createIndexLabelForOlapPk(propertyKey);
        LOG.debug("start create olap pk");
        schemaTx.createOlapPk(this.schemaId());
        return null;
    }
}