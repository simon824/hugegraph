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

package org.apache.hugegraph.api.gremlin;

import java.util.Set;

import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableSet;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

@Path("gremlin")
@Singleton
public class GremlinAPI extends GremlinQueryAPI {

    private static final Logger LOG = Log.logger(GremlinAPI.class);

    private static final Histogram gremlinInputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");
    private static final Histogram gremlinOutputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-output");
    private static final Set<String> FORBIDDEN_REQUEST_EXCEPTIONS =
            ImmutableSet.of("java.lang.SecurityException",
                            "javax.ws.rs.ForbiddenException");
    private static final Set<String> BAD_REQUEST_EXCEPTIONS = ImmutableSet.of(
            "java.lang.IllegalArgumentException",
            "java.util.concurrent.TimeoutException",
            "groovy.lang.",
            "org.codehaus.",
            "org.apache.hugegraph."
    );

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    private GremlinClient client;

    public static String convertToPostRequest(String graphSpace, String graph,
                                              String query) {
        // 直接转为 gremlin post 格式，避免维护 doGetRequest 功能
        String graphQuery = "";
        if (!query.contains(".") || query.startsWith("g.")) {
            graphQuery = graph;
        } else {
            graphQuery = query.substring(0, query.indexOf("."));
        }

        E.checkArgument(graphQuery != null && !graphQuery.isEmpty(),
                        "Graph for the gremlin query is not provided.");

        String spaceGraph = graphSpace + "-" + graphQuery;
        String body = "{\n" +
                      "  \"gremlin\": \"%s\",\n" +
                      "  \"bindings\": {},\n" +
                      "  \"language\": \"gremlin-groovy\",\n" +
                      "  \"aliases\": {\"%s\":\"%s\",\n" +
                      "                \"g\":\"__g_%s\"}\n" +
                      "}";
        String request = String.format(body, query, graphQuery, spaceGraph,
                                       spaceGraph);
        return request;
    }

    @Override
    public GremlinClient client() {
        if (this.client != null) {
            return this.client;
        }
        HugeConfig config = this.configProvider.get();
        String url = config.get(ServerOptions.GREMLIN_SERVER_URL);
        int timeout = config.get(ServerOptions.GREMLIN_SERVER_TIMEOUT) * 1000;
        int maxRoutes = config.get(ServerOptions.GREMLIN_SERVER_MAX_ROUTE);
        this.client = new GremlinClient(url, timeout, maxRoutes, maxRoutes);
        return this.client;
    }

    @GET
    @Timed
    @Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response get(@Context HugeConfig conf,
                        @Context HttpHeaders headers,
                        @QueryParam("graphspace")
                        @DefaultValue("DEFAULT") String graphSpace,
                        @QueryParam("graph") String graph,
                        @QueryParam("gremlin") String query) {
        LOG.info("GremlinAPI get request:{}", query);

        String request = convertToPostRequest(graphSpace, graph, query);

        // code same with post api
        String auth = headers.getHeaderString("ICBC-Authorization");
        if (auth == null) {
            auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        }
        auth = auth.split(",")[0];

        Response response = this.client().doPostRequest(auth, request);
        gremlinInputHistogram.update(request.length());
        gremlinOutputHistogram.update(response.getLength());
        return transformResponseIfNeeded(response);
    }

    @POST
    @Timed
    @Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response post(@Context HugeConfig conf,
                         @Context HttpHeaders headers,
                         String request) {
        /* The following code is reserved for forwarding request */
        // context.getRequestDispatcher(location).forward(request, response);
        // return Response.seeOther(UriBuilder.fromUri(location).build())
        // .build();
        // Response.temporaryRedirect(UriBuilder.fromUri(location).build())
        // .build();
        LOG.info("GremlinAPI request:{}", request);

        String auth = headers.getHeaderString("ICBC-Authorization");
        if (auth == null) {
            auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        }
        auth = auth.split(",")[0];

        Response response = this.client().doPostRequest(auth, request);
        gremlinInputHistogram.update(request.length());
        gremlinOutputHistogram.update(response.getLength());
        return transformResponseIfNeeded(response);
    }
}
