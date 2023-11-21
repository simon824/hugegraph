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

import static org.apache.hugegraph.config.ServerOptions.WHITE_IP_STATUS;

import java.io.IOException;
import java.security.Principal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Priority;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.AuthContext;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeAuthenticator.RequiredPerm;
import org.apache.hugegraph.auth.HugeAuthenticator.RolePerm;
import org.apache.hugegraph.auth.HugeAuthenticator.User;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.utils.Charsets;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;

@Provider
@Priority(Priorities.AUTHENTICATION)
public class AuthenticationFilter implements ContainerRequestFilter {

    public static final String BASIC_AUTH_PREFIX = "Basic ";
    public static final String BEARER_TOKEN_PREFIX = "Bearer ";
    public static final String ALL_GRAPH_SPACES = "*";
    private static final Logger LOGGER = Log.logger(AuthenticationFilter.class);

    private static final List<String> WHITE_API_LIST = ImmutableList.of(
            "auth/login",
            "auth/kglogin",
            "versions"
    );

    private static final List<String> ANONYMOUS_API_LIST = ImmutableList.of(
            "metrics/backend",
            "metrics"
    );
    private static String whiteIpStatus;

    // Set Const admin@AuthContext
    static {
        AuthContext.admin = HugeGraphAuthProxy.Context.admin().toString();
    }

    @Context
    private jakarta.inject.Provider<GraphManager> managerProvider;
    @Context
    private jakarta.inject.Provider<Request> requestProvider;
    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    public static boolean isWhiteAPI(ContainerRequestContext context) {
        String path = context.getUriInfo().getPath();

        E.checkArgument(StringUtils.isNotEmpty(path),
                        "Invalid request uri '%s'", path);

        for (String whiteApi : WHITE_API_LIST) {
            if (path.endsWith(whiteApi)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAnonymousAPI(ContainerRequestContext context) {
        String path = context.getUriInfo().getPath();

        E.checkArgument(StringUtils.isNotEmpty(path),
                        "Invalid request uri '%s'", path);

        for (String anonymousApi : ANONYMOUS_API_LIST) {
            if (path.endsWith(anonymousApi)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        if (AuthenticationFilter.isWhiteAPI(context)) {
            return;
        }
        GraphManager manager = this.managerProvider.get();
        User user = this.authenticate(context);
        Authorizer authorizer =
                new Authorizer(manager, user, context.getUriInfo());
        context.setSecurityContext(authorizer);
    }

    protected User authenticate(ContainerRequestContext context) {
        GraphManager manager = this.managerProvider.get();
        E.checkState(manager != null, "Context GraphManager is absent");

        if (!manager.requireAuthentication()) {
            // Return anonymous user with admin role if disable authentication
            return User.ANONYMOUS;
        }

        if (AuthenticationFilter.isAnonymousAPI(context)) {
            // Return anonymous user if access anonymous api
            return User.ANONYMOUS;
        }

        // Get peer info
        Request request = this.requestProvider.get();
        String peer = null;
        String path = null;
        if (whiteIpStatus == null) {
            whiteIpStatus = this.configProvider.get().get(WHITE_IP_STATUS);
        }
        if (Objects.equals(whiteIpStatus, "enable") && request != null) {
            peer = request.getRemoteAddr() + ":" + request.getRemotePort();
            path = request.getRequestURI();

            // check white ip
            String remoteIp = request.getRemoteAddr();
            List<String> whiteIpList = manager.authManager().listWhiteIp();
            boolean whiteIpEnabled = manager.authManager().getWhiteIpStatus();
            if (!path.contains("whiteiplist") && whiteIpEnabled &&
                !whiteIpList.contains(remoteIp)) {
                throw new ForbiddenException(
                        String.format("Remote ip '%s' is not permitted",
                                      remoteIp));
            }
        }

        Map<String, String> credentials = new HashMap<>();
        // Extract authentication credentials
        // String auth = context.getHeaderString(HttpHeaders.AUTHORIZATION);
        // 兼容工行AUTHORIZATION字段被DDS系统屏蔽
        String auth = context.getHeaderString("ICBC-Authorization");
        if (auth == null) {
            auth = context.getHeaderString(HttpHeaders.AUTHORIZATION);
        }

        if (auth == null) {
            throw new NotAuthorizedException(
                    "Authentication credentials are required",
                    "Missing authentication credentials");
        }
        auth = auth.split(",")[0];

        if (auth.startsWith(BASIC_AUTH_PREFIX)) {
            auth = auth.substring(BASIC_AUTH_PREFIX.length());
            auth = new String(Base64.getDecoder().decode(auth),
                              Charsets.ASCII_CHARSET);
            String[] values = auth.split(":");
            if (values.length != 2) {
                throw new BadRequestException(
                        "Invalid syntax for username and password");
            }

            final String username = values[0];
            final String password = values[1];

            if (StringUtils.isEmpty(username) ||
                StringUtils.isEmpty(password)) {
                throw new BadRequestException(
                        "Invalid syntax for username and password");
            }

            credentials.put(HugeAuthenticator.KEY_USERNAME, username);
            credentials.put(HugeAuthenticator.KEY_PASSWORD, password);
        } else if (auth.startsWith(BEARER_TOKEN_PREFIX)) {
            String token = auth.substring(BEARER_TOKEN_PREFIX.length());
            credentials.put(HugeAuthenticator.KEY_TOKEN, token);
        } else {
            throw new BadRequestException(
                    "Only HTTP Basic or Bearer authentication is supported");
        }

        credentials.put(HugeAuthenticator.KEY_ADDRESS, peer);
        credentials.put(HugeAuthenticator.KEY_PATH, path);

        // Validate the extracted credentials
        try {
            return manager.authenticate(credentials);
        } catch (AuthenticationException e) {
            throw new NotAuthorizedException("Authentication failed",
                                             e.getMessage());
        }
    }

    public static class Authorizer implements SecurityContext {

        private final UriInfo uri;
        private final User user;
        private final Principal principal;
        private final GraphManager manager;

        public Authorizer(GraphManager manager, final User user,
                          final UriInfo uri) {
            E.checkNotNull(user, "user");
            E.checkNotNull(uri, "uri");
            this.manager = manager;
            this.uri = uri;
            this.user = user;
            this.principal = new UserPrincipal();
        }

        public String username() {
            return this.user.username();
        }

        public RolePermission role() {
            return this.user.role();
        }

        public String userId() {
            return this.user.userId().asString();
        }

        @Override
        public Principal getUserPrincipal() {
            return this.principal;
        }

        @Override
        public boolean isUserInRole(String required) {
            if (required.equals(HugeAuthenticator.KEY_DYNAMIC)) {
                // Let the resource itself determine dynamically
                return true;
            } else {
                return this.matchPermission(required);
            }
        }

        @Override
        public boolean isSecure() {
            return "https".equals(this.uri.getRequestUri().getScheme());
        }

        @Override
        public String getAuthenticationScheme() {
            return SecurityContext.BASIC_AUTH;
        }

        private boolean matchPermission(String required) {
            boolean valid;
            RequiredPerm requiredPerm;

            /*
             * if request url contains graph space and the corresponding space
             * does not enable permission check, return true
             * */
            if (!isAuth()) {
                return true;
            }

            if (!required.startsWith(HugeAuthenticator.KEY_GRAPHSPACE)) {
                // Permission format like: "admin", "space", "analyst"
                requiredPerm = new RequiredPerm();
                requiredPerm.owner(required);
                if ("space".equals(required) || "analyst".equals(required)) {
                    String graphSpace = this.getPathParameter("graphspace");
                    requiredPerm.graphSpace(graphSpace);
                }
                valid = RolePerm.match(this.role(), requiredPerm);
                if ("analyst".equals(required) && !valid) {
                    RolePerm rolePerm = RolePerm.fromJson(this.role());
                    String graphSpace = this.getPathParameter("graphspace");
                    valid = rolePerm.matchAnalyst(graphSpace);
                }
            } else {
                // The required like:
                // $graphspace=graphspace $owner=graph1 $action=vertex_write
                requiredPerm = RequiredPerm.fromPermission(required);

                /*
                 * Replace graphspace value (it may be a variable) if the
                 * permission format like:
                 * "$graphspace=$graphspace $owner=$graph $action=vertex_write"
                 */
                String graphSpace = requiredPerm.graphSpace();
                if (graphSpace.startsWith(HugeAuthenticator.VAR_PREFIX)) {
                    int prefixLen = HugeAuthenticator.VAR_PREFIX.length();
                    assert graphSpace.length() > prefixLen;
                    graphSpace = graphSpace.substring(prefixLen);
                    graphSpace = this.getPathParameter(graphSpace);
                    requiredPerm.graphSpace(graphSpace);
                }

                /*
                 * Replace owner value(it may be a variable) if the permission
                 * format like: "$graphspace=$graphspace $owner=$graph $action=vertex_write"
                 */
                String owner = requiredPerm.owner();
                if (owner.startsWith(HugeAuthenticator.VAR_PREFIX)) {
                    // Replace `$graph` with graph name like "graph1"
                    int prefixLen = HugeAuthenticator.VAR_PREFIX.length();
                    assert owner.length() > prefixLen;
                    owner = owner.substring(prefixLen);
                    owner = this.getPathParameter(owner);
                    requiredPerm.owner(owner);
                }

                // authenticate by graph nickname
                HugeGraph graph = this.manager.graph(graphSpace, owner);
                if (graph == null) {
                    throw new NotFoundException(String.format(
                            "Graph '%s' does not exist", owner));
                }
                requiredPerm.owner(graph.nickname());

                valid = RolePerm.match(this.role(), requiredPerm);
            }

            if (!valid &&
                !required.equals(HugeAuthenticator.USER_ADMIN)) {
                LOGGER.info("[AUDIT] User access denied with user id {}, action {}, resource " +
                            "object {}",
                            user.userId().asString(),
                            requiredPerm.action().string(),
                            requiredPerm.resourceObject());
            }
            return valid;
        }

        private String getPathParameter(String key) {
            List<String> params = this.uri.getPathParameters().get(key);
            return params.get(0);
        }

        private boolean isAuth() {
            List<String> params = this.uri.getPathParameters().get(
                    "graphspace");
            if (params != null && params.size() == 1) {
                String graphSpace = params.get(0);
                if (ALL_GRAPH_SPACES.equals(graphSpace)) {
                    return true;
                }
                E.checkArgumentNotNull(this.manager.graphSpace(graphSpace),
                                       "The graph space '%s' does not exist",
                                       graphSpace);
                return this.manager.graphSpace(graphSpace).auth();
            } else {
                return true;
            }
        }

        private final class UserPrincipal implements Principal {

            @Override
            public String getName() {
                return Authorizer.this.user.getName();
            }

            @Override
            public String toString() {
                return Authorizer.this.user.toString();
            }

            @Override
            public int hashCode() {
                return Authorizer.this.user.hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                return Authorizer.this.user.equals(obj);
            }
        }
    }
}
