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

package org.apache.hugegraph.api.space;

import java.util.Set;

import javax.annotation.security.RolesAllowed;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

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
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;

@Path("graphspaces/{graphspace}/services")
@Singleton
public class ServiceApi extends API {

    private static final Logger LOGGER = Log.logger(RestServer.class);

    private static final String CONFIRM_DROP = "I'm sure to delete the service";

    private static final String CLUSTER_IP = "ClusterIP";
    private static final String NODE_PORT = "NodePort";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @Context SecurityContext sc) {
        E.checkArgument(space(manager, graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        Set<String> services = manager.services(graphSpace);
        return ImmutableMap.of("services", services);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("name") String name) {
        E.checkArgument(space(manager, graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        Service service = service(manager, graphSpace, name);
        service.serverDdsUrls(manager.getServiceDdsUrls(graphSpace, name));
        service.serverNodePortUrls(
                manager.getServiceNodePortUrls(graphSpace, name));
        return manager.serializer().writeService(service);
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonService jsonService) {

        E.checkArgument(space(manager, graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        jsonService.checkCreate(false);

        String username = manager.authManager().username();
        Service temp = jsonService.toService(username);


        Service service = manager.createService(graphSpace, temp);
        LOGGER.info("[AUDIT] Add service with service id {}", service.serviceId());
        return manager.serializer().writeService(service);
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("k8s-register")
    @RolesAllowed({"space"})
    public void registerK8S(@Context GraphManager manager) throws Exception {
        // manager.registerK8StoPd();
    }

    @PUT
    @Timed
    @Status(Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @Path("stop/{name}")
    @RolesAllowed({"space"})
    public void stopService(@Context GraphManager manager,
                            @PathParam("graphspace") String graphSpace,
                            @PathParam("name") String serviceName) {

        Service service = service(manager, graphSpace, serviceName);
        if (null == service || 0 == service.running()) {
            return;
        }
        if (!service.k8s()) {
            throw new BadRequestException("Cannot stop service in Manual mode");
        }
        manager.stopService(graphSpace, serviceName);
        LOGGER.info("[AUDIT] Stop service with service id {}", service.serviceId());

    }

    @PUT
    @Timed
    @Status(Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @Path("start/{name}")
    @RolesAllowed({"space"})
    public void startService(@Context GraphManager manager,
                             @PathParam("graphspace") String graphSpace,
                             @PathParam("name") String serviceName) {
        Service service = service(manager, graphSpace, serviceName);
        if (!service.k8s()) {
            throw new BadRequestException("Cannot stop service in Manual mode");
        }
        if (0 == service.running()) {
            manager.startService(graphSpace, service);
        }
        LOGGER.info("[AUDIT] Start service with service id {}", service.serviceId());
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("name") String name,
                       @QueryParam("confirm_message") String message) {
        E.checkArgument(space(manager, graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        E.checkArgument(CONFIRM_DROP.equals(message),
                        "Please take the message: %s", CONFIRM_DROP);
        Service service = service(manager, graphSpace, name);
        manager.dropService(graphSpace, name);

        LOGGER.info("[AUDIT] Remove service with service id {}", service.serviceId());
    }

    private static class JsonService implements Checkable {

        @JsonProperty("name")
        public String name;
        @JsonProperty("type")
        public Service.ServiceType serviceType;
        @JsonProperty("deployment_type")
        public Service.DeploymentType deploymentType;
        @JsonProperty("description")
        public String description;
        @JsonProperty("count")
        public int count;

        @JsonProperty("cpu_limit")
        public int cpuLimit;
        @JsonProperty("memory_limit")
        public int memoryLimit;
        @JsonProperty("storage_limit")
        public int storageLimit;

        @JsonProperty("route_type")
        public String routeType;
        @JsonProperty("port")
        public int port;

        @JsonProperty("urls")
        public Set<String> urls;

        public static boolean isNodePort(String routeType) {
            return NODE_PORT.equals(routeType);
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.name != null &&
                            !StringUtils.isEmpty(this.name),
                            "The name of service can't be null or empty");

            E.checkArgument(this.serviceType != null,
                            "The type of service can't be null");

            E.checkArgument(this.deploymentType != null,
                            "The deployment type of service can't be null");

            E.checkArgument(this.count > 0,
                            "The service count must be > 0, but got: %s",
                            this.count);

            E.checkArgument(this.cpuLimit > 0,
                            "The cpu limit must be > 0, but got: %s",
                            this.cpuLimit);
            E.checkArgument(this.memoryLimit > 0,
                            "The memory limit must be > 0, but got: %s",
                            this.memoryLimit);
            E.checkArgument(this.storageLimit > 0,
                            "The storage limit must be > 0, but got: %s",
                            this.storageLimit);

            if (this.deploymentType == Service.DeploymentType.MANUAL) {
                E.checkArgument(this.urls != null && !this.urls.isEmpty(),
                                "The urls can't be null or empty when " +
                                "deployment type is %s",
                                Service.DeploymentType.MANUAL);
                E.checkArgument(this.routeType == null,
                                "Can't set route type of manual service");
                E.checkArgument(this.port == 0,
                                "Can't set port of manual service, but got: " +
                                "%s", this.port);
            } else {
                E.checkArgument(this.urls == null || this.urls.isEmpty(),
                                "The urls must be null or empty when " +
                                "deployment type is %s",
                                this.deploymentType);
                E.checkArgument(!StringUtils.isEmpty(this.routeType),
                                "The route type of service can't be null or " +
                                "empty");
                E.checkArgument(NODE_PORT.equals(this.routeType) ||
                                CLUSTER_IP.equals(this.routeType),
                                "Invalid route type '%s'", this.routeType);
            }
        }

        public Service toService(String creator) {
            Service service = new Service(this.name, creator, this.serviceType,
                                          this.deploymentType);
            service.description(this.description);
            service.count(this.count);

            service.cpuLimit(this.cpuLimit);
            service.memoryLimit(this.memoryLimit);
            service.storageLimit(this.storageLimit);

            service.routeType(this.routeType);
            if (isNodePort(this.routeType)) {
                service.port(this.port);
            }

            if (this.deploymentType == Service.DeploymentType.MANUAL) {
                service.urls(this.urls);
            }

            return service;
        }

        public String toString() {
            return String.format("JsonService{name=%s, type=%s, " +
                                 "deploymentType=%s, description=%s, " +
                                 "count=%s, cpuLimit=%s, memoryLimit=%s, " +
                                 "storageLimit=%s, port=%s, urls=%s}",
                                 this.name, this.serviceType,
                                 this.deploymentType, this.description,
                                 this.count, this.cpuLimit, this.memoryLimit,
                                 this.storageLimit, this.port, this.urls);
        }
    }
}
