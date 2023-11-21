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

package org.apache.hugegraph.api.filter;

import java.io.IOException;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.ext.Provider;

@Provider
@Singleton
@PreMatching
public class LicenseFilter implements ContainerRequestFilter {

    private static final Logger LOG = Log.logger(LicenseFilter.class);

    @Context
    private jakarta.inject.Provider<GraphManager> managerProvider;

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        if (managerProvider.get().isLicenseValid() == false) {
            LOG.info("License is invalid.");
            throw new HugeException("License is invalid.");
        }
    }
}
