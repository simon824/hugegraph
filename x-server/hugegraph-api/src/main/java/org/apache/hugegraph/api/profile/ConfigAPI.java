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

package org.apache.hugegraph.api.profile;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.core.GraphManager;

import com.codahale.metrics.annotation.Timed;

import jakarta.inject.Singleton;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;

@Path("graphspaces/{graphspace}/configs")
@Singleton
public class ConfigAPI extends API {

    /**
     * Available rest fields
     */
    private static final Set<String> REST_FIELDS
            = new HashSet<>(Arrays.asList(
            "restserver.url",
            "server.start_ignore_single_graph_error",
            "batch.max_write_ratio",
            "batch.max_write_threads",
            "batch.max_vertices_per_batch",
            "batch.max_edges_per_batch",
            "server.k8s_url",
            "server.k8s_use_ca",
            "server.k8s_ca",
            "server.k8s_client_ca",
            "server.k8s_client_key",
            "server.k8s_oltp_image",
            "k8s.internal_algorithm_image_url",
            "k8s.internal_algorithm",
            "k8s.algorithms"
    ));

    @GET
    @Timed
    @Path("rest")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public String getRestConfig(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace) {
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace));
    }

    @GET
    @Timed
    @Path("rest/{servicename}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public String getRestConfig(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace,
                                @PathParam("servicename") String serviceName) {
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace,
                                                       serviceName));
    }

    @GET
    @Timed
    @Path("rest/config-fields")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public String getRestConfigFields(@Context GraphManager manager) {
        return manager.serializer().writeList("fields", REST_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @POST
    @Timed
    @Path("rest")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public String restAll(@Context GraphManager manager,
                          @PathParam("graphspace") String graphSpace,
                          Map<String, Object> extendProperties) {
        String serviceName = String.valueOf(extendProperties.get("name"));

        Map<String, Object> properties =
                (Map<String, Object>) extendProperties.get("config");

        // this.validateFields(properties);

        Map<String, Object> result = manager.restProperties(graphSpace,
                                                            serviceName,
                                                            properties);
        return manager.serializer().writeMap(result);

    }

    @PUT
    @Timed
    @Path("rest")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public String rest(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       Map<String, Object> properties) {
        // this.validateFields(properties);
        checkRestUpdate(properties);
        // manager.createServiceRestConfig(graphSpace, serviceName, properties);
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace,
                                                       // serviceName,
                                                       properties));
    }

    @PUT
    @Timed
    @Path("rest/{servicename}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public String rest(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("servicename") String serviceName,
                       Map<String, Object> properties) {
        // this.validateFields(properties);
        checkRestUpdate(properties);
        return manager.serializer()
                      .writeMap(manager.restProperties(graphSpace,
                                                       serviceName,
                                                       properties));
    }

    @DELETE
    @Timed
    @Path("rest/{servicename}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public void rest(@Context GraphManager manager,
                     @PathParam("graphspace") String graphSpace,
                     @PathParam("servicename") String serviceName) {
        manager.clearRestProperties(graphSpace, serviceName);
    }

    @DELETE
    @Timed
    @Path("rest/{servicename}/{key}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$dynamic"})
    public void rest(@Context GraphManager manager,
                     @PathParam("graphspace") String graphSpace,
                     @PathParam("servicename") String serviceName,
                     @PathParam("key") String key) {
        manager.deleteRestProperties(graphSpace, serviceName, key);
    }

    @GET
    @Timed
    @Path("gremlin")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String gremlinYaml(@Context GraphManager manager,
                              @PathParam("graphspace") String graphSpace) {
        return manager.gremlinYaml(graphSpace);
    }

    @PUT
    @Timed
    @Path("gremlin")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String gremlinYaml(@Context GraphManager manager,
                              @PathParam("graphspace") String graphSpace,
                              String yaml) {
        return manager.gremlinYaml(graphSpace, yaml);
    }


    /**
     * Validate the keys of properties. Not used currently
     *
     * @param properties
     */
    @SuppressWarnings("unused")
    private void validateFields(Map<String, Object> properties) {
        if (null == properties) {
            throw new BadRequestException(
                    "Config is null while setting rest config");
        }
        properties.keySet().forEach((key) -> {
            if (!REST_FIELDS.contains(key)) {
                throw new BadRequestException(
                        "Invalid filed [" + key + "] while setting rest " +
                        "config");
            }
        });
    }

    private void checkRestUpdate(Map<String, Object> properties) {
    }
}
