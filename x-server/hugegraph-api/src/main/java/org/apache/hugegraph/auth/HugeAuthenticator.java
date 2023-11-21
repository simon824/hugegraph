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

package org.apache.hugegraph.auth;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.Context;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.type.Namifiable;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.slf4j.Logger;

public interface HugeAuthenticator extends Authenticator {

    public static final Logger LOG =
            Log.logger(HugeAuthenticator.class);

    public static final String KEY_USERNAME =
            CredentialGraphTokens.PROPERTY_USERNAME;
    public static final String KEY_PASSWORD =
            CredentialGraphTokens.PROPERTY_PASSWORD;
    public static final String KEY_TOKEN = "token";
    public static final String KEY_ROLE = "role";
    public static final String KEY_ADDRESS = "address";
    public static final String KEY_PATH = "path";
    public static final String GENERAL_PATTERN = "*";

    public static final String USER_SYSTEM = RestServer.EXECUTOR;
    public static final String USER_ADMIN = "admin";
    public static final String USER_ANONY = AuthenticatedUser.ANONYMOUS_USERNAME;

    public static final RolePermission ROLE_NONE = RolePermission.none();
    public static final RolePermission ROLE_ADMIN = RolePermission.admin();

    public static final String VAR_PREFIX = "$";
    public static final String KEY_GRAPHSPACE = VAR_PREFIX + "graphspace";
    public static final String KEY_OWNER = VAR_PREFIX + "owner";
    public static final String KEY_DYNAMIC = VAR_PREFIX + "dynamic";
    public static final String KEY_ACTION = VAR_PREFIX + "action";

    public static HugeAuthenticator loadAuthenticator(HugeConfig conf) {
        String authClass = conf.get(ServerOptions.AUTHENTICATOR);
        if (authClass.isEmpty()) {
            return null;
        }

        HugeAuthenticator authenticator;
        ClassLoader cl = conf.getClass().getClassLoader();
        try {
            authenticator = (HugeAuthenticator) cl.loadClass(authClass)
                                                  .newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to load authenticator: '%s'",
                                    authClass, e);
        }

        authenticator.setup(conf);

        return authenticator;
    }

    public void setup(HugeConfig config);

    public UserWithRole authenticate(String username, String password,
                                     String token);

    public AuthManager authManager();

    @Override
    public default void setup(final Map<String, Object> config) {
        E.checkState(config != null,
                     "Must provide a 'config' in the 'authentication'");
        String path = (String) config.get("tokens");
        E.checkState(path != null,
                     "Credentials configuration missing key 'tokens'");
        OptionSpace.register("tokens", ServerOptions.instance());
        this.setup(new HugeConfig(path));
    }

    @Override
    public default User authenticate(final Map<String, String> credentials)
            throws AuthenticationException {

        HugeGraphAuthProxy.resetContext();

        User user = User.ANONYMOUS;
        if (this.requireAuthentication()) {
            String username = credentials.get(KEY_USERNAME);
            String password = credentials.get(KEY_PASSWORD);
            String token = credentials.get(KEY_TOKEN);

            // Currently we just use config tokens to authenticate
            UserWithRole role = this.authenticate(username, password, token);
            if (!verifyRole(role.role())) {
                // Throw if not certified
                String message = "Incorrect username or password";
                throw new AuthenticationException(message);
            }
            user = new User(role.username(), role.role());
            user.client(credentials.get(KEY_ADDRESS));
        }

        HugeGraphAuthProxy.logUser(user, credentials.get(KEY_PATH));
        /*
         * Set authentication context
         * TODO: unset context after finishing a request
         */
        HugeGraphAuthProxy.setContext(new Context(user));

        return user;
    }

    @Override
    public default boolean requireAuthentication() {
        return true;
    }

    public default boolean verifyRole(RolePermission role) {
        if (role == ROLE_NONE || role == null) {
            return false;
        } else {
            return true;
        }
    }

    public static class User extends AuthenticatedUser {

        public static final User ADMIN = new User(USER_ADMIN, ROLE_ADMIN);
        public static final User ANONYMOUS = new User(USER_ANONY, ROLE_ADMIN);
        /**
         * cache for fromjson
         */
        private static final Map<String, User> USERCACHES = new HashMap<>();
        private final RolePermission role;
        private final Id userId;
        private String client; // peer

        public User(String username, RolePermission role) {
            super(username);
            E.checkNotNull(username, "username");
            E.checkNotNull(role, "role");
            this.role = role;
            this.client = "";
            /*
             * 1. Use username as the id to simplify getting userId
             * 2. Only used as cache's key in auth proxy now
             */
            this.userId = IdGenerator.of(username);
        }

        public static User fromJson(String json) {
            if (json == null) {
                return null;
            }

            if (USERCACHES.containsKey(json)) {
                return USERCACHES.get(json);
            }

            synchronized (USERCACHES) {
                if (!USERCACHES.containsKey(json)) {
                    Map<String, Object> userMap = JsonUtil.fromJson(json, Map.class);
                    RolePermission userRole =
                            RolePermission.fromJson(JsonUtil.toJson(userMap.get("role")));
                    if (userMap != null) {
                        User user = new User(userMap.get("username").toString(),
                                             RolePermission.builtin(userRole));
                        if (userMap.get("client") == null) {
                            user.client(null);
                        } else {
                            user.client(userMap.get("client").toString());
                        }
                        USERCACHES.putIfAbsent(json, user);
                    }
                }
            }

            return USERCACHES.get(json);
        }

        public String username() {
            return this.getName();
        }

        public Id userId() {
            return this.userId;
        }

        public RolePermission role() {
            return this.role;
        }

        public void client(String client) {
            this.client = client;
        }

        public String client() {
            return client;
        }

        @Override
        public boolean isAnonymous() {
            return this == ANONYMOUS || this == ANONYMOUS_USER;
        }

        @Override
        public int hashCode() {
            return this.username().hashCode() ^ this.role().hashCode() ^
                   this.client().hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof User)) {
                return false;
            }

            User other = (User) object;
            return this.username().equals(other.username()) &&
                   this.role().equals(other.role()) &&
                   this.client().equals(other.client);
        }

        @Override
        public String toString() {
            return String.format("User{username=%s,role=%s,client=%s}",
                                 this.username(), this.role(), this.client());
        }

        public String toJson() {
            UserJson json = new UserJson();
            json.username = this.username();
            json.role = this.role();
            json.client = this.client();
            return JsonUtil.toJson(json);
        }

        public static class UserJson {

            @JsonProperty("username")
            private String username;
            @JsonProperty("role")
            private RolePermission role;
            @JsonProperty("client")
            private String client;
        }
    }

    public static class RolePerm {

        public static final String ANY = "*";
        public static final String POUND_SEPARATOR = "#";
        @JsonProperty("roles") // graphspace -> graph -> action -> resource
        private Map<String, Map<String, Map<HugePermission, Object>>> roles;

        public RolePerm() {
            this.roles = new HashMap<>();
        }

        public RolePerm(Map<String, Map<String, Map<HugePermission,
                Object>>> roles) {
            this.roles = roles;
        }

        public static boolean matchAuth(Object role, HugePermission required,
                                        ResourceObject<?> resourceObject) {
            if (RolePermission.isAdmin((RolePermission) role)) {
                return true;
            }

            RolePerm rolePerm = RolePerm.fromJson(role);
            if (rolePerm.matchSpace(resourceObject.graphSpace())) {
                return true;
            }
            return false;
        }

        private static boolean matchedPrefix(String key, String graph) {
            if (key.equals(graph)) {
                return true;
            } else if (key.endsWith("*")) {
                key = key.substring(0, key.length() - 1);
                if (!graph.startsWith(key)) {
                    return false;
                }
                return true;
            }
            return false;
        }

        public static List<Map<String, Object>> permissionsReadOfType(
                RolePerm role, String graphSpace, String graph,
                ResourceType type, HugePermission action) {
            List<Map<String, Object>> permissions = new ArrayList<>();
            Map<String, Map<HugePermission, Object>> spaceRoles =
                    role.roles.get(graphSpace);
            if (spaceRoles == null) {
                return permissions;
            }

            for (Map.Entry<String, Map<HugePermission, Object>> e :
                    spaceRoles.entrySet()) {
                if (!matchedPrefix(e.getKey(), graph)) {
                    continue;
                }
                Map<HugePermission, Object> graphRoles = e.getValue();
                if (graphRoles == null) {
                    continue;
                }

                Object readPerm = matchedAction(action, graphRoles);
                if (readPerm == null) {
                    continue;
                }

                Map<String, List<HugeResource>> ressMap = (Map<String,
                        List<HugeResource>>) readPerm;
                for (Map.Entry<String, List<HugeResource>> entry :
                        ressMap.entrySet()) {
                    String[] typeLabel = entry.getKey().split(POUND_SEPARATOR);
                    ResourceType resourceType =
                            ResourceType.valueOf(typeLabel[0]);
                    if (!resourceType.match(type)) {
                        continue;
                    }

                    for (HugeResource resource : entry.getValue()) {
                        Map<String, Object> params = new HashMap<>();
                        params.put("label", resource.label());
                        params.put("properties", resource.getProperties());
                        permissions.add(params);
                    }
                }
            }

            return permissions;
        }

        private static Object matchedAction(HugePermission action,
                                            Map<HugePermission, Object> perms) {
            Object matched = perms.get(action);
            if (matched != null) {
                return matched;
            }
            for (Map.Entry<HugePermission, Object> e : perms.entrySet()) {
                HugePermission permission = e.getKey();
                // May be required = ANY
                if (action.match(permission) ||
                    action.equals(HugePermission.EXECUTE)) {
                    // Return matched resource of corresponding action
                    return e.getValue();
                }
            }
            return null;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public static RolePerm fromJson(Object role) {
            RolePermission table = RolePermission.fromJson(role);
            return new RolePerm((Map) table.map());
        }

        public static boolean match(Object role, RequiredPerm requiredPerm) {
            if (RolePermission.isAdmin((RolePermission) role)) {
                return true;
            }
            if (ROLE_NONE.equals(role)) {
                return false;
            }

            RolePerm rolePerm = RolePerm.fromJson(role);
            if (rolePerm.matchSpace(requiredPerm.graphSpace())) {
                return true;
            }

            if (requiredPerm.action() == HugePermission.NONE) {
                // None action means any action is OK if the owner matched
                return rolePerm.matchOwner(requiredPerm.graphSpace(),
                                           requiredPerm.owner());
            }
            return rolePerm.matchResource(requiredPerm.action(),
                                          requiredPerm.resourceObject());
        }

        public static boolean match(Object role, HugePermission required,
                                    ResourceObject<?> resourceObject) {
            if (RolePermission.isAdmin((RolePermission) role)) {
                return true;
            }
            if (role == ROLE_NONE) {
                return false;
            }
            RolePerm rolePerm = RolePerm.fromJson(role);
            if (rolePerm.matchSpace(resourceObject.graphSpace())) {
                return true;
            }
            return rolePerm.matchResource(required, resourceObject);
        }

        public static boolean match(Object role, RolePermission grant,
                                    ResourceObject<?> resourceObject) {
            if (RolePermission.isAdmin((RolePermission) role)) {
                return true;
            }
            if (role == ROLE_NONE) {
                return false;
            }

            if (resourceObject != null) {
                AuthElement element = (AuthElement) resourceObject.operated();
                if (element instanceof HugeUser &&
                    ((HugeUser) element).name().equals(USER_ADMIN)) {
                    // Can't access admin by other users
                    return false;
                }
            }

            RolePermission rolePerm = RolePermission.fromJson(role);
            return rolePerm.contains(grant);
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        private boolean matchOwner(String graphSpace, String owner) {
            if (graphSpace == null && owner == null) {
                return true;
            }

            return this.roles.containsKey(graphSpace) &&
                   this.roles.get(graphSpace).containsKey(owner);
        }

        private boolean matchSpace(String graphSpace) {
            if (graphSpace == null) {
                return true;
            }

            return this.roles.containsKey(graphSpace) &&
                   this.roles.get(graphSpace).containsKey(GENERAL_PATTERN) &&
                   this.roles.get(graphSpace).get(GENERAL_PATTERN)
                             .containsKey(HugePermission.SPACE);
        }

        public boolean matchAnalyst(String graphSpace) {
            if (this.matchSpace(graphSpace)) {
                return true;
            }

            List<HugePermission> permList =
                    Arrays.asList(HugePermission.READ,
                                  HugePermission.WRITE,
                                  HugePermission.DELETE,
                                  HugePermission.EXECUTE);

            if (!this.roles.containsKey(graphSpace) ||
                !this.roles.get(graphSpace).containsKey(GENERAL_PATTERN)) {
                return false;
            }

            boolean check = true;
            Map<HugePermission, Object> accesses =
                    this.roles.get(graphSpace).get(GENERAL_PATTERN);
            for (HugePermission perm : permList) {
                check = check && accesses.containsKey(perm);
                if (!check) {
                    break;
                }
                Map<String, Object> resources =
                        (Map<String, Object>) accesses.get(perm);
                check = resources.containsKey("ALL");
            }
            return check;
        }

        private boolean matchResource(HugePermission requiredAction,
                                      ResourceObject<?> requiredResource) {
            E.checkNotNull(requiredResource, "resource object");

            /*
             * Is resource allowed to access by anyone?
             * TODO: only allowed resource of related type(USER/TASK/VAR),
             *       such as role VAR is allowed to access '~variables' label
             */
            if (HugeResource.allowed(requiredResource)) {
                return true;
            }

            Map<String, Map<HugePermission, Object>> innerRoles =
                    this.roles.get(requiredResource.graphSpace());
            if (innerRoles == null) {
                return false;
            }

            // * or {graph}
            String owner = requiredResource.graph();
            for (Map.Entry<String, Map<HugePermission, Object>> e :
                    innerRoles.entrySet()) {
                if (!matchedPrefix(e.getKey(), owner)) {
                    continue;
                }
                Map<HugePermission, Object> permissions = e.getValue();
                if (permissions == null) {
                    permissions = innerRoles.get(GENERAL_PATTERN);
                    if (permissions == null) {
                        continue;
                    }
                }

                Object permission = matchedAction(requiredAction, permissions);
                if (permission == null) {
                    continue;
                }

                Map<String, List<HugeResource>> ressMap = (Map<String,
                        List<HugeResource>>) permission;

                ResourceType requiredType = requiredResource.type();
                for (Map.Entry<String, List<HugeResource>> entry :
                        ressMap.entrySet()) {
                    String[] typeLabel = entry.getKey().split(POUND_SEPARATOR);
                    ResourceType type = ResourceType.valueOf(typeLabel[0]);
                    /* assert one type can match but not equal to other only
                     * when it is related to schema and data
                     */
                    if (!type.match(requiredType)) {
                        continue;
                    } else if (type != requiredType) {
                        return true;
                    }

                    // check label
                    String requiredLabel = null;
                    if (requiredType.isSchema()) {
                        requiredLabel =
                                ((Namifiable) requiredResource.operated()).name();
                    } else if (requiredType.isGraph()) {
                        requiredLabel =
                                ((HugeElement) requiredResource.operated()).label();
                    } else {
                        return true;
                    }
                    String label = typeLabel[1];
                    if (!(ANY.equals(label) || "null".equals(label)
                          || requiredLabel.matches(label))) {
                        continue;
                    } else if (requiredType.isSchema()) {
                        return true;
                    }

                    // check properties
                    List<HugeResource> ress =
                            ressMap.get(type + POUND_SEPARATOR + label);

                    for (HugeResource res : ress) {
                        if (res.filter(
                                ((HugeElement) requiredResource.operated()))) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    public static class RequiredPerm {

        @JsonProperty("graphspace")
        private String graphSpace;
        @JsonProperty("owner")
        private String owner;
        @JsonProperty("action")
        private HugePermission action;
        @JsonProperty("resource")
        private ResourceType resource;

        public RequiredPerm() {
            this.graphSpace = "";
            this.owner = "";
            this.action = HugePermission.NONE;
            this.resource = ResourceType.NONE;
        }

        public static String roleFor(String graphSpace, String owner,
                                     HugePermission perm) {
            /*
             * Construct required permission such as:
             *  $owner=graph1 $action=read
             *  (means required read permission of any one resource)
             *
             * In the future maybe also support:
             *  $owner=graph1 $action=vertex_read
             */
            return String.format("%s=%s %s=%s %s=%s",
                                 KEY_GRAPHSPACE, graphSpace,
                                 KEY_OWNER, owner,
                                 KEY_ACTION, perm.string());
        }

        public static RequiredPerm fromJson(String json) {
            return JsonUtil.fromJson(json, RequiredPerm.class);
        }

        public static RequiredPerm fromPermission(String permission) {
            // Permission format like: "$graphspace=$default $owner=$graph1 $action=vertex-write"
            RequiredPerm requiredPerm = new RequiredPerm();
            String[] spaceAndOwnerAndAction = permission.split(" ");
            String[] spaceKV = spaceAndOwnerAndAction[0].split("=", 2);
            E.checkState(spaceKV.length == 2 && spaceKV[0].equals(KEY_GRAPHSPACE),
                         "Bad permission format: '%s'", permission);
            requiredPerm.graphSpace(spaceKV[1]);

            String[] ownerKV = spaceAndOwnerAndAction[1].split("=", 2);
            E.checkState(ownerKV.length == 2 && ownerKV[0].equals(KEY_OWNER),
                         "Bad permission format: '%s'", permission);
            requiredPerm.owner(ownerKV[1]);

            if (spaceAndOwnerAndAction.length == 2) {
                // Return owner if no action (means NONE)
                return requiredPerm;
            }

            E.checkState(spaceAndOwnerAndAction.length == 3,
                         "Bad permission format: '%s'", permission);
            String[] actionKV = spaceAndOwnerAndAction[2].split("=", 2);
            E.checkState(actionKV.length == 2,
                         "Bad permission format: '%s'", permission);
            E.checkState(actionKV[0].equals(StandardAuthenticator.KEY_ACTION),
                         "Bad permission format: '%s'", permission);
            requiredPerm.action(actionKV[1]);

            return requiredPerm;
        }

        public RequiredPerm graphSpace(String graphSpace) {
            this.graphSpace = graphSpace;
            return this;
        }

        public String graphSpace() {
            return this.graphSpace;
        }

        public RequiredPerm owner(String owner) {
            this.owner = owner;
            return this;
        }

        public String owner() {
            return this.owner;
        }

        public RequiredPerm action(String action) {
            this.parseAction(action);
            return this;
        }

        public HugePermission action() {
            return this.action;
        }

        public ResourceType resource() {
            return this.resource;
        }

        public ResourceObject<?> resourceObject() {
            Namifiable elem = HugeResource.NameObject.ANY;
            return ResourceObject.of(this.graphSpace, this.owner,
                                     this.resource, elem);
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        private void parseAction(String action) {
            int offset = action.lastIndexOf('_');
            if (0 < offset && ++offset < action.length()) {
                /*
                 * In order to be compatible with the old permission mechanism,
                 * here is only to provide pre-control by extract the
                 * resource_action {vertex/edge/schema}_{read/write},
                 * resource_action like vertex_read.
                 */
                String resource = action.substring(0, offset - 1);
                this.resource = ResourceType.valueOf(resource.toUpperCase());
                action = action.substring(offset);
            }
            this.action = HugePermission.valueOf(action.toUpperCase());
        }
    }
}
