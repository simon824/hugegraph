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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.security.sasl.AuthenticationException;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.jsonwebtoken.Claims;
import jakarta.ws.rs.ForbiddenException;

public class StandardAuthManager implements AuthManager {

    public static final String ALL_GRAPHS = "*";
    public static final String ALL_GRAPH_SPACES = "*";
    public static final String DEFAULT_SETTER_ROLE_KEY =
            "_DEFAULT_SETTER_ROLE";
    protected static final Logger LOG = Log.logger(StandardAuthManager.class);
    private static final long AUTH_CACHE_EXPIRE = 10 * 60L;
    private static final long AUTH_CACHE_CAPACITY = 1024 * 10L;
    private static final long AUTH_TOKEN_EXPIRE = 3600 * 24L;
    private static final String DEFAULT_ADMIN_ROLE_KEY = "DEFAULT_ADMIN_ROLE";
    private static final String DEFAULT_ADMIN_TARGET_KEY = "DEFAULT_ADMIN_TARGET";
    // Cache <username, HugeUser>
    private final Cache<Id, HugeUser> usersCache;
    // Cache <userId, passwd>
    private final Cache<Id, String> pwdCache;
    // Cache <token, username>
    private final Cache<Id, String> tokenCache;
    private final TokenGenerator tokenGenerator;
    private final MetaManager metaManager;

    public StandardAuthManager(MetaManager metaManager, HugeConfig conf) {
        this.metaManager = metaManager;
        this.usersCache = this.cache("users", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.pwdCache = this.cache("users_pwd", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenCache = this.cache("token", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenGenerator = new TokenGenerator(conf);
    }

    public StandardAuthManager(MetaManager metaManager, String secretKey) {
        this.metaManager = metaManager;
        this.usersCache = this.cache("users", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.pwdCache = this.cache("users_pwd", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenCache = this.cache("token", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenGenerator = new TokenGenerator(secretKey);
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-auth";
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    @Override
    public boolean close() {
        return true;
    }

    private void invalidateUserCache() {
        this.usersCache.clear();
    }

    private void invalidatePasswordCache(Id id) {
        this.pwdCache.invalidate(id);
        // Clear all tokenCache because can't get userId in it
        this.tokenCache.clear();
    }

    private AuthElement updateCreator(AuthElement elem) {
        String username = currentUsername();
        if (elem.creator() == null) {
            if (username != null) {
                elem.creator(username);
            } else {
                elem.creator("init default space role");
            }
        }
        elem.update(new Date());
        return elem;
    }

    private String currentUsername() {
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        if (context != null) {
            return context.user().username();
        }
        return null;
    }

    private <V> V verifyResPermission(String graphSpace,
                                      HugePermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher,
                                      Supplier<Boolean> checker) {
        // TODO: delete this method
        // TODO: call verifyPermission() before actual action
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when verifying resource permission");
        // String username = context.user().username();
        Object role = context.user().role();
        ResourceObject<V> ro = fetcher.get();
        String action = actionPerm.string();

        V result = ro.operated();
        // Verify role permission
        if (!HugeAuthenticator.RolePerm.matchAuth(role, actionPerm, ro)) {
            result = null;
        }

        // result = null means no permission, throw if needed
        if (result == null && throwIfNoPerm) {
            String error = String.format("Permission denied: %s %s",
                                         action, ro);
            throw new ForbiddenException(error);
        }
        return result;
    }

    private <V> V verifyResPermission(String graphSpace,
                                      HugePermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher) {
        return verifyResPermission(graphSpace, actionPerm,
                                   throwIfNoPerm, fetcher, null);
    }

    private <V extends AuthElement> V verifyUserPermission(
            String graphSpace,
            HugePermission actionPerm,
            boolean throwIfNoPerm,
            Supplier<V> elementFetcher) {
        return verifyResPermission(graphSpace, actionPerm, throwIfNoPerm,
                                   () -> {
                                       V elem = elementFetcher.get();
                                       @SuppressWarnings("unchecked")
                                       ResourceObject<V> r = (ResourceObject<V>)
                                               ResourceObject.of(graphSpace, "SYSTEM", elem);
                                       return r;
                                   });
    }

    private <V extends AuthElement> V verifyUserPermission(
            String graphSpace,
            HugePermission actionPerm,
            V elementFetcher) {
        return verifyUserPermission(graphSpace, actionPerm,
                                    true, () -> elementFetcher);
    }

    private <V extends AuthElement> List<V> verifyUserPermission(
            String graphSpace,
            HugePermission actionPerm,
            List<V> elems) {
        List<V> results = new ArrayList<>();
        for (V elem : elems) {
            V r = verifyUserPermission(graphSpace, actionPerm,
                                       false, () -> elem);
            if (r != null) {
                results.add(r);
            }
        }
        return results;
    }

    @Override
    public Id createUser(HugeUser user, boolean required) {
        Id username = IdGenerator.of(user.name());
        HugeUser existed = this.usersCache.get(username);
        if (existed != null) {
            throw new HugeException("The user name '%s' has existed",
                                    user.name());
        }

        try {
            this.updateCreator(user);
            user.create(user.update());
            this.metaManager.createUser(user);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize user", e);
        }

        return username;
    }

    @Override
    public HugeUser updateUser(HugeUser user, boolean required) {
        HugeUser result = null;
        try {
            HugeUser existed = this.findUser(user.name(), false);
            if (required && !existed.name().equals(currentUsername())) {
                // Only admin could update user
                verifyUserPermission("", HugePermission.ADMIN, user);
            }

            this.updateCreator(user);
            result = this.metaManager.updateUser(user);
            this.invalidateUserCache();
            this.invalidatePasswordCache(user.id());
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize user", e);
        }
    }

    protected void deleteBelongsByUserOrGroup(Id id) {
        // delete role belongs
        List<String> spaces = this.listGraphSpace();
        for (String space : spaces) {
            List<HugeBelong> belongs = this.listBelongBySource(space, id,
                                                               HugeBelong.ALL,
                                                               -1, false);
            for (HugeBelong belong : belongs) {
                this.deleteBelong(space, belong.id(), false);
            }
        }

        // delete belongs in * space
        List<HugeBelong> belongsAdmin = this.listBelongBySource(ALL_GRAPH_SPACES,
                                                                id,
                                                                HugeBelong.UR,
                                                                -1, false);
        List<HugeBelong> belongsSource =
                this.listBelongBySource(ALL_GRAPH_SPACES, id, HugeBelong.UG,
                                        -1, false);
        List<HugeBelong> belongsTarget =
                this.listBelongByTarget(ALL_GRAPH_SPACES, id, HugeBelong.UG,
                                        -1, false);

        belongsSource.addAll(belongsAdmin);
        belongsSource.addAll(belongsTarget);
        for (HugeBelong belong : belongsSource) {
            this.deleteBelong(ALL_GRAPH_SPACES, belong.id(), false);
        }
    }

    @Override
    public HugeUser deleteUser(Id id, boolean required) {
        if (id.asString().equals("admin")) {
            throw new HugeException("admin could not be removed");
        }

        try {
            HugeUser user = this.findUser(id.asString(), false);
            E.checkArgument(user != null,
                            "The user name '%s' is not existed",
                            id.asString());
            E.checkArgument(!HugeAuthenticator.USER_ADMIN.equals(user.name()),
                            "Delete user '%s' is forbidden", user.name());
            this.deleteBelongsByUserOrGroup(id);
            this.invalidateUserCache();
            this.invalidatePasswordCache(id);
            return this.metaManager.deleteUser(id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    /**
     * findUser: not verifyUserPermission
     */
    @Override
    public HugeUser findUser(String name, boolean required) {
        Id username = IdGenerator.of(name);
        HugeUser user = this.usersCache.get(username);
        if (user == null) {
            try {
                user = this.metaManager.findUser(name);
                if (user != null) {
                    this.usersCache.update(username, user);
                }
            } catch (IOException e) {
                throw new HugeException("IOException occurs when " +
                                        "deserialize user", e);
            } catch (ClassNotFoundException e) {
                throw new HugeException("ClassNotFoundException occurs when " +
                                        "deserialize user", e);
            }
        }

        return user;
    }

    @Override
    public HugeUser getUser(Id id, boolean required) {
        HugeUser user = this.findUser(id.asString(), false);
        E.checkArgument(user != null, "The user is not existed");
        if (required && !user.name().equals(currentUsername())) {
            verifyUserPermission("", HugePermission.READ, user);
        }
        return user;
    }

    @Override
    public List<HugeUser> listUsers(List<Id> ids, boolean required) {
        try {
            if (required) {
                return verifyUserPermission("", HugePermission.READ,
                                            this.metaManager.listUsers(ids));
            }
            return this.metaManager.listUsers(ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    @Override
    public List<HugeUser> listUsersByGroup(String group, long limit,
                                           boolean required) {
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongByTarget(ALL_GRAPH_SPACES,
                                                        IdGenerator.of(group),
                                                        HugeBelong.UG, limit);

            if (required) {
                verifyUserPermission("", HugePermission.ADMIN,
                                     belongs);
            }
            List<HugeUser> result = new ArrayList<>();
            for (HugeBelong belong : belongs) {
                result.add(this.metaManager.findUser(
                        belong.source().asString()));
            }

            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    @Override
    public List<HugeUser> listAllUsers(long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission("", HugePermission.READ,
                                            this.metaManager.listAllUsers(limit));
            }
            return this.metaManager.listAllUsers(limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    private void tryInitAdminRole() {
        try {
            HugeRole role = this.metaManager.findRole(ALL_GRAPH_SPACES,
                                                      IdGenerator.of(
                                                              DEFAULT_ADMIN_ROLE_KEY));
            if (role == null) {
                role = new HugeRole(DEFAULT_ADMIN_ROLE_KEY,
                                    ALL_GRAPH_SPACES);
                role.nickname("系统管理员");
                this.updateCreator(role);
                role.create(role.update());
                this.metaManager.createRole(ALL_GRAPH_SPACES, role);
            }

            HugeTarget target = this.metaManager.findTarget(ALL_GRAPH_SPACES,
                                                            IdGenerator.of(
                                                                    DEFAULT_ADMIN_TARGET_KEY));
            if (target == null) {
                target = new HugeTarget(DEFAULT_ADMIN_TARGET_KEY,
                                        ALL_GRAPH_SPACES, ALL_GRAPHS);
                this.updateCreator(target);
                target.create(target.update());
                this.metaManager.createTarget(ALL_GRAPH_SPACES, target);
            }

            String accessId =
                    this.metaManager.accessId(DEFAULT_ADMIN_ROLE_KEY,
                                              DEFAULT_ADMIN_TARGET_KEY,
                                              HugePermission.ADMIN);
            HugeAccess access = this.metaManager.findAccess(ALL_GRAPH_SPACES,
                                                            IdGenerator.of(accessId));
            if (access == null) {
                access = new HugeAccess(ALL_GRAPH_SPACES,
                                        IdGenerator.of(DEFAULT_ADMIN_ROLE_KEY),
                                        IdGenerator.of(DEFAULT_ADMIN_TARGET_KEY),
                                        HugePermission.ADMIN);
                this.updateCreator(access);
                access.create(access.update());
                this.metaManager.createAccess(ALL_GRAPH_SPACES, access);
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "init space op manager role", e);
        }
    }

    private void tryInitDefaultRole(String graphSpace,
                                    String roleName,
                                    String graph) {
        try {
            HugeRole role = this.metaManager.findRole(
                    graphSpace, IdGenerator.of(roleName));
            if (role == null) {
                role = new HugeRole(roleName, graphSpace);
                role.nickname(HugeDefaultRole.getNickname(roleName));
                this.updateCreator(role);
                role.create(role.update());
                this.metaManager.createRole(graphSpace, role);
            }

            String targetName = (ALL_GRAPHS.equals(graph)) ?
                                HugeDefaultRole.DEFAULT_SPACE_TARGET_KEY :
                                getGraphTargetName(graph);
            String description = (ALL_GRAPHS.equals(graph)) ?
                                 "图空间全部资源" : graph + "-图全部资源";
            HugeTarget target = this.metaManager.findTarget(
                    graphSpace, IdGenerator.of(targetName));
            if (target == null) {
                Map<String, List<HugeResource>> spaceResources =
                        new HashMap<>();
                spaceResources.put("ALL", ImmutableList.of(
                        new HugeResource(ResourceType.ALL, null, null)));
                target = new HugeTarget(targetName,
                                        graphSpace, graph,
                                        spaceResources);
                target.description(description);
                this.updateCreator(target);
                target.create(target.update());
                this.metaManager.createTarget(graphSpace, target);
            }

            createDefaultAccesses(graphSpace, roleName, targetName);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "init space default role", e);
        }
    }

    public String getGraphTargetName(String graph) {
        return graph + "_" + HugeDefaultRole.DEFAULT_SPACE_TARGET_KEY;
    }

    private void createDefaultAccesses(String graphSpace, String role,
                                       String targetName)
            throws IOException, ClassNotFoundException {
        List<HugePermission> perms;
        if (HugeDefaultRole.SPACE.toString().equals(role)) {
            perms = List.of(HugePermission.SPACE);
        } else if (HugeDefaultRole.ANALYST.toString().equals(role)) {
            perms = Arrays.asList(HugePermission.READ,
                                  HugePermission.WRITE,
                                  HugePermission.DELETE,
                                  HugePermission.EXECUTE);
        } else if (HugeDefaultRole.isObserver(role)) {
            perms = List.of(HugePermission.READ);
        } else {
            throw new HugeException("Unsupported default role");
        }

        for (HugePermission perm : perms) {
            String accessId =
                    this.metaManager.accessId(role,
                                              targetName,
                                              perm);
            HugeAccess access = this.metaManager.findAccess(graphSpace,
                                                            IdGenerator.of(
                                                                    accessId));
            if (access == null) {
                access = new HugeAccess(graphSpace,
                                        IdGenerator.of(role),
                                        IdGenerator.of(targetName),
                                        perm);
                this.updateCreator(access);
                access.create(access.update());
                this.metaManager.createAccess(graphSpace, access);
            }
        }
    }

    private void tryInitDefaultSpace(String graphSpace) {
        try {
            HugeRole role = this.metaManager.findRole(
                    graphSpace, IdGenerator.of(DEFAULT_SETTER_ROLE_KEY));
            if (role != null) {
                return;
            }
            role = new HugeRole(DEFAULT_SETTER_ROLE_KEY, graphSpace);
            this.updateCreator(role);
            role.create(role.update());
            this.metaManager.createRole(graphSpace, role);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "init space manager role", e);
        }
    }

    private void tryInitDefaultGraph(String graphSpace, String graph) {
        try {
            HugeRole role = this.metaManager.findRole(
                    graphSpace,
                    IdGenerator.of(graph + DEFAULT_SETTER_ROLE_KEY));
            if (role != null) {
                return;
            }
            role = new HugeRole(graph + DEFAULT_SETTER_ROLE_KEY, graphSpace);
            this.updateCreator(role);
            role.create(role.update());
            this.metaManager.createRole(graphSpace, role);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "init space manager role", e);
        }
    }

    @Override
    public Id createSpaceManager(String graphSpace, String user) {
        String role = HugeDefaultRole.SPACE.toString();
        try {
            HugeBelong belong;
            if (HugeGroup.isGroup(user)) {
                belong = new HugeBelong(
                        graphSpace, null, IdGenerator.of(user),
                        IdGenerator.of(role),
                        HugeBelong.GR);
            } else {
                belong = new HugeBelong(
                        graphSpace, IdGenerator.of(user), null,
                        IdGenerator.of(role),
                        HugeBelong.UR);
            }

            this.tryInitDefaultRole(graphSpace,
                                    role,
                                    ALL_GRAPHS);
            this.updateCreator(belong);
            belong.create(belong.update());
            return this.metaManager.createBelong(graphSpace, belong);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "create space manager", e);
        }
    }

    @Override
    public void deleteSpaceManager(String graphSpace, String user) {
        try {
            String belongId =
                    this.metaManager.belongId(
                            user, HugeDefaultRole.SPACE.toString());
            this.metaManager.deleteBelong(graphSpace,
                                          IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "delete space manager", e);
        }
    }

    @Override
    public List<String> listSpaceManager(String graphSpace) {
        List<String> spaceManagers = new ArrayList<>();
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongByTarget(
                            graphSpace, IdGenerator.of(
                                    HugeDefaultRole.SPACE.toString()),
                            HugeBelong.ALL, -1);
            for (HugeBelong belong : belongs) {
                spaceManagers.add(belong.source().asString());
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "list space manager", e);
        }
        return spaceManagers;
    }

    @Override
    public boolean isSpaceManager(String user) {
        List<String> spaces = this.listGraphSpace();
        for (String space : spaces) {
            if (isSpaceManager(space, user)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSpaceManager(String graphSpace, String user) {
        try {
            List<HugeGroup> groups = this.listGroupsByUser(user, -1, false);
            for (HugeGroup group : groups) {
                String belongIdG =
                        this.metaManager.belongId(
                                group.name(),
                                HugeDefaultRole.SPACE.toString(),
                                HugeBelong.GR);
                if (this.metaManager.existBelong(graphSpace,
                                                 IdGenerator.of(belongIdG))) {
                    return true;
                }
            }

            String belongId =
                    this.metaManager.belongId(
                            user, HugeDefaultRole.SPACE.toString());
            return this.metaManager.existBelong(graphSpace,
                                                IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when check if is space " +
                                    "manager.", e);
        }
    }

    @Override
    public Id createAdminManager(String user) {
        try {
            HugeBelong belong = new HugeBelong(ALL_GRAPH_SPACES,
                                               IdGenerator.of(user),
                                               IdGenerator.of(DEFAULT_ADMIN_ROLE_KEY));
            this.tryInitAdminRole();
            this.updateCreator(belong);
            belong.create(belong.update());
            return this.metaManager.createBelong(ALL_GRAPH_SPACES, belong);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "create space op manager", e);
        }
    }

    @Override
    public void deleteAdminManager(String user) {
        try {
            String belongId =
                    this.metaManager.belongId(user,
                                              DEFAULT_ADMIN_ROLE_KEY);
            this.metaManager.deleteBelong(ALL_GRAPH_SPACES,
                                          IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "delete space op manager", e);
        }
    }

    @Override
    public List<String> listAdminManager() {
        Set<String> adminManagers = new HashSet<>();
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongByTarget(
                            ALL_GRAPH_SPACES,
                            IdGenerator.of(DEFAULT_ADMIN_ROLE_KEY),
                            HugeBelong.ALL, -1);
            for (HugeBelong belong : belongs) {
                adminManagers.add(belong.source().asString());
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "list admin manager", e);
        }

        // Add DEFAULT admin
        adminManagers.add("admin");

        return new ArrayList<>(adminManagers);
    }

    @Override
    public boolean isAdminManager(String user) {

        if ("admin".equals(user)) {
            return true;
        }

        try {
            String belongId =
                    this.metaManager.belongId(user, DEFAULT_ADMIN_ROLE_KEY);
            return this.metaManager.existBelong(ALL_GRAPH_SPACES,
                                                IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "check whether is manager", e);
        }
    }

    @Override
    public Id createSpaceDefaultRole(String graphSpace, String user,
                                     HugeDefaultRole role) {
        return createDefaultRole(graphSpace, user, role, ALL_GRAPHS);
    }

    @Override
    public Id createDefaultRole(String graphSpace, String user,
                                HugeDefaultRole role, String graph) {
        String roleName = (role.isGraphRole()) ?
                          getGraphDefaultRole(graph, role.toString()) : role.toString();
        try {
            HugeBelong belong;
            if (HugeGroup.isGroup(user)) {
                belong = new HugeBelong(graphSpace, null, IdGenerator.of(user),
                                        IdGenerator.of(roleName), HugeBelong.GR);
            } else {
                belong = new HugeBelong(graphSpace, IdGenerator.of(user), null,
                                        IdGenerator.of(roleName),
                                        HugeBelong.UR);
            }

            this.tryInitDefaultRole(graphSpace, roleName, graph);
            this.updateCreator(belong);
            belong.create(belong.update());
            return this.metaManager.createBelong(graphSpace, belong);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "create " + role + ".", e);
        }
    }

    public void deleteDefaultRole(String graphSpace, String owner,
                                  String role) {
        try {
            String belongId;
            if (HugeGroup.isGroup(owner)) {
                belongId = this.metaManager.belongId(owner, role,
                                                     HugeBelong.GR);
            } else {
                belongId = this.metaManager.belongId(owner, role,
                                                     HugeBelong.UR);
            }
            this.metaManager.deleteBelong(graphSpace, IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "delete " + role + ".", e);
        }
    }

    @Override
    public void deleteDefaultRole(String graphSpace, String owner,
                                  HugeDefaultRole role) {
        deleteDefaultRole(graphSpace, owner, role.toString());
    }

    @Override
    public void deleteDefaultRole(String graphSpace, String owner,
                                  HugeDefaultRole role, String graph) {
        String roleName = getGraphDefaultRole(graph, role.toString());
        deleteDefaultRole(graphSpace, owner, roleName);
    }

    @Override
    public boolean isDefaultRole(String user, HugeDefaultRole role) {
        List<String> spaces = this.listGraphSpace();
        for (String space : spaces) {
            if (isDefaultRole(space, user, role)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isDefaultRole(String graphSpace, String owner,
                                 HugeDefaultRole role) {
        return isDefaultRole(graphSpace, owner, role.toString());
    }

    @Override
    public boolean isDefaultRole(String graphSpace, String graph, String owner,
                                 HugeDefaultRole role) {
        String roleName = getGraphDefaultRole(graph, role.toString());
        return isDefaultRole(graphSpace, owner, roleName);
    }

    public boolean isDefaultRole(String graphSpace, String owner,
                                 String role) {
        try {
            String belongId;
            if (HugeGroup.isGroup(owner)) {
                belongId = this.metaManager.belongId(owner, role,
                                                     HugeBelong.GR);
                return this.metaManager.existBelong(graphSpace,
                                                    IdGenerator.of(belongId));
            }

            List<HugeGroup> groups = this.listGroupsByUser(owner, -1, false);
            for (HugeGroup group : groups) {
                String belongIdG = this.metaManager.belongId(group.name(),
                                                             role,
                                                             HugeBelong.GR);
                if (this.metaManager.existBelong(graphSpace,
                                                 IdGenerator.of(belongIdG))) {
                    return true;
                }
            }

            belongId = this.metaManager.belongId(owner, role);

            return this.metaManager.existBelong(graphSpace,
                                                IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when check if is " +
                                    role + ".", e);
        }
    }

    @Override
    public void initSpaceDefaultRoles(String graphSpace) {
        this.tryInitDefaultRole(graphSpace,
                                HugeDefaultRole.SPACE.toString(),
                                ALL_GRAPHS);
        this.tryInitDefaultRole(graphSpace,
                                HugeDefaultRole.ANALYST.toString(),
                                ALL_GRAPHS);
    }

    @Override
    public void createGraphDefaultRole(String graphSpace, String graph) {
        this.tryInitDefaultRole(
                graphSpace,
                getGraphDefaultRole(graph,
                                    HugeDefaultRole.OBSERVER.toString()),
                graph);
    }

    @Override
    public String getGraphDefaultRole(String graph, String role) {
        return graph + "_" + role;
    }

    @Override
    public void deleteGraphDefaultRole(String graphSpace, String graph) {
        String roleName =
                getGraphDefaultRole(graph,
                                    HugeDefaultRole.OBSERVER.toString());
        deleteRole(graphSpace, IdGenerator.of(roleName), false);
        String targetName = getGraphTargetName(graph);
        deleteTarget(graphSpace, IdGenerator.of(targetName), false);
    }

    @Override
    public Id createRole(String graphSpace, HugeRole role,
                         boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, role);
            }
            this.updateCreator(role);
            role.create(role.update());
            Id result = this.metaManager.createRole(graphSpace, role);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize role", e);
        }
    }

    @Override
    public HugeRole updateRole(String graphSpace, HugeRole role,
                               boolean required) {
        this.invalidateUserCache();
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, role);
            }
            this.updateCreator(role);
            return this.metaManager.updateRole(graphSpace, role);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize role", e);
        }
    }

    @Override
    public HugeRole deleteRole(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.DELETE,
                                     this.metaManager.getRole(graphSpace,
                                                              id));
            }

            List<HugeBelong> belongs = this.listBelongByTarget(graphSpace, id,
                                                               HugeBelong.ALL,
                                                               -1, false);
            for (HugeBelong belong : belongs) {
                this.deleteBelong(graphSpace, belong.id(), false);
            }

            List<HugeAccess> accesses = this.listAccessByRole(graphSpace, id,
                                                              -1, false);
            for (HugeAccess access : accesses) {
                this.deleteAccess(graphSpace, access.id(), false);
            }

            HugeRole result = this.metaManager.deleteRole(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize role", e);
        }
    }

    @Override
    public HugeRole getRole(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.getRole(graphSpace, id));
            }
            return this.metaManager.getRole(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize role", e);
        }
    }

    @Override
    public List<HugeRole> listRoles(String graphSpace, List<Id> ids,
                                    boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listRoles(graphSpace, ids));
            }
            return this.metaManager.listRoles(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize role", e);
        }
    }

    @Override
    public List<HugeRole> listAllRoles(String graphSpace, long limit,
                                       boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listAllRoles(graphSpace, limit));
            }
            return this.metaManager.listAllRoles(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize role", e);
        }
    }

    @Override
    public Id setDefaultSpace(String graphSpace, String user) {
        try {
            HugeBelong belong = new HugeBelong(graphSpace,
                                               IdGenerator.of(user),
                                               IdGenerator.of(
                                                       DEFAULT_SETTER_ROLE_KEY));
            this.tryInitDefaultSpace(graphSpace);
            this.updateCreator(belong);
            belong.create(belong.update());
            return this.metaManager.createBelong(graphSpace, belong);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "set default space", e);
        }
    }

    @Override
    public String getDefaultSpace(String user) {
        List<String> spaces = this.listGraphSpace();
        for (String graphSpace : spaces) {
            List<HugeBelong> belongs = listBelongBySource(graphSpace,
                                                          IdGenerator.of(user),
                                                          HugeBelong.UR, -1,
                                                          false);
            for (HugeBelong belong : belongs) {
                String role = belong.target().asString();
                if (role.equals(DEFAULT_SETTER_ROLE_KEY)) {
                    return graphSpace;
                }
            }
        }

        return null;
    }

    @Override
    public void unsetDefaultSpace(String graphSpace, String user) {
        String belongId = this.metaManager.belongId(user,
                                                    DEFAULT_SETTER_ROLE_KEY);
        try {
            this.metaManager.deleteBelong(graphSpace, IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when unset default " +
                                    "space", e);
        }
    }

    @Override
    public Id setDefaultGraph(String graphSpace, String graph, String user) {
        try {
            HugeBelong belong = new HugeBelong(graphSpace,
                                               IdGenerator.of(user),
                                               IdGenerator.of(graph +
                                                              DEFAULT_SETTER_ROLE_KEY));
            this.tryInitDefaultGraph(graphSpace, graph);
            this.updateCreator(belong);
            belong.create(belong.update());
            return this.metaManager.createBelong(graphSpace, belong);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "set default graph", e);
        }
    }

    @Override
    public String getDefaultGraph(String graphSpace, String user) {
        List<HugeBelong> belongs = listBelongBySource(graphSpace,
                                                      IdGenerator.of(user),
                                                      HugeBelong.UR, -1,
                                                      false);
        for (HugeBelong belong : belongs) {
            String role = belong.target().asString();
            if (role.endsWith(DEFAULT_SETTER_ROLE_KEY) &&
                role.length() != DEFAULT_SETTER_ROLE_KEY.length()) {
                return role.substring(0, role.lastIndexOf(
                        DEFAULT_SETTER_ROLE_KEY));
            }
        }
        return null;
    }

    @Override
    public void unsetDefaultGraph(String graphSpace, String graph,
                                  String user) {
        String role = graph + DEFAULT_SETTER_ROLE_KEY;
        String belongId = this.metaManager.belongId(user, role);
        try {
            this.metaManager.deleteBelong(graphSpace, IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when unset default " +
                                    "graph", e);
        }
    }

    @Override
    public Id createGroup(HugeGroup group, boolean required) {
        try {
            if (required) {
                verifyUserPermission("", HugePermission.ADMIN, group);
            }
            this.updateCreator(group);
            group.create(group.update());
            Id result = this.metaManager.createGroup(group);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize group", e);
        }
    }

    @Override
    public HugeGroup updateGroup(HugeGroup group, boolean required) {
        try {
            if (required) {
                verifyUserPermission("", HugePermission.ADMIN, group);
            }
            this.updateCreator(group);
            group.create(group.update());
            HugeGroup result = this.metaManager.updateGroup(group);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize group", e);
        }
    }

    @Override
    public HugeGroup deleteGroup(Id id, boolean required) {
        try {
            if (required) {
                verifyUserPermission("", HugePermission.ADMIN,
                                     this.metaManager.findGroup(id.asString()));
            }
            this.deleteBelongsByUserOrGroup(id);
            HugeGroup result = this.metaManager.deleteGroup(id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public HugeGroup findGroup(String name, boolean required) {
        try {
            if (required) {
                verifyUserPermission("", HugePermission.ADMIN,
                                     this.metaManager.findGroup(name));
            }
            HugeGroup result = this.metaManager.findGroup(name);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public List<HugeGroup> listGroups(long limit, boolean required) {
        try {
            if (required) {
                verifyUserPermission("", HugePermission.ADMIN,
                                     this.metaManager.listGroups(limit));
            }
            List<HugeGroup> result = this.metaManager.listGroups(limit);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public List<HugeGroup> listGroupsByUser(String user, long limit,
                                            boolean required) {
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongBySource(ALL_GRAPH_SPACES,
                                                        IdGenerator.of(user),
                                                        HugeBelong.UG, limit);

            if (required) {
                verifyUserPermission("", HugePermission.ADMIN, belongs);
            }
            List<HugeGroup> result = new ArrayList<>();
            for (HugeBelong belong : belongs) {
                result.add(this.metaManager.findGroup(
                        belong.target().asString()));
            }

            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get group list by user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get group list by user", e);
        }
    }

    @Override
    public Id createTarget(String graphSpace, HugeTarget target,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, target);
            }
            this.updateCreator(target);
            target.create(target.update());
            Id result = this.metaManager.createTarget(graphSpace, target);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize target", e);
        }
    }

    @Override
    public HugeTarget updateTarget(String graphSpace, HugeTarget target,
                                   boolean required) {
        try {
            this.updateCreator(target);
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, target);
            }
            HugeTarget result = this.metaManager.updateTarget(graphSpace, target);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize target", e);
        }
    }

    @Override
    public HugeTarget deleteTarget(String graphSpace, Id id,
                                   boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.DELETE,
                                     this.metaManager.getTarget(graphSpace,
                                                                id));
            }
            List<HugeAccess> accesses = this.listAccessByTarget(graphSpace, id,
                                                                -1, false);
            for (HugeAccess access : accesses) {
                this.deleteAccess(graphSpace, access.id(), false);
            }
            HugeTarget target = this.metaManager.deleteTarget(graphSpace, id);
            this.invalidateUserCache();
            return target;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public HugeTarget getTarget(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.getTarget(graphSpace, id));
            }
            return this.metaManager.getTarget(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids,
                                        boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listTargets(graphSpace, ids));
            }
            return this.metaManager.listTargets(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public List<HugeTarget> listAllTargets(String graphSpace, long limit,
                                           boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listAllTargets(graphSpace, limit));
            }
            return this.metaManager.listAllTargets(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public Id createBelong(String graphSpace, HugeBelong belong,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, belong);
            }
            this.updateCreator(belong);
            belong.create(belong.update());
            this.invalidateUserCache();
            return this.metaManager.createBelong(graphSpace, belong);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "create belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "create belong", e);
        }
    }

    @Override
    public HugeBelong updateBelong(String graphSpace, HugeBelong belong,
                                   boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, belong);
            }
            this.updateCreator(belong);
            HugeBelong result = this.metaManager.updateBelong(graphSpace, belong);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "update belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "update belong", e);
        }
    }

    @Override
    public HugeBelong deleteBelong(String graphSpace, Id id,
                                   boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.DELETE,
                                     this.metaManager.getBelong(graphSpace,
                                                                id));
            }
            HugeBelong result = this.metaManager.deleteBelong(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "delete belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "delete belong", e);
        }
    }

    @Override
    public HugeBelong getBelong(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.getBelong(graphSpace, id));
            }
            return this.metaManager.getBelong(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong", e);
        }
    }

    @Override
    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids,
                                       boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listBelong(graphSpace, ids));
            }
            return this.metaManager.listBelong(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by ids", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by ids", e);
        }
    }

    @Override
    public List<HugeBelong> listAllBelong(String graphSpace, long limit,
                                          boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listAllBelong(graphSpace, limit));
            }
            return this.metaManager.listAllBelong(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get all belong list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get all belong list", e);
        }
    }

    @Override
    public List<HugeBelong> listBelongBySource(String graphSpace, Id user,
                                               String link, long limit,
                                               boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listBelongBySource(graphSpace, user,
                                                                                link,
                                                                                limit));
            }
            return this.metaManager.listBelongBySource(graphSpace, user, link,
                                                       limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by user", e);
        }
    }

    @Override
    public List<HugeBelong> listBelongByTarget(String graphSpace, Id target,
                                               String link, long limit,
                                               boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listBelongByTarget(graphSpace, target,
                                                                                link,
                                                                                limit));
            }
            return this.metaManager.listBelongByTarget(graphSpace, target,
                                                       link, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by role", e);
        }
    }

    @Override
    public Id createAccess(String graphSpace, HugeAccess access,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, access);
            }
            this.updateCreator(access);
            access.create(access.update());
            Id result = this.metaManager.createAccess(graphSpace, access);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "create access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "create access", e);
        }
    }

    @Override
    public HugeAccess updateAccess(String graphSpace, HugeAccess access,
                                   boolean required) {
        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.WRITE, access);
            }
            this.updateCreator(access);
            HugeAccess result = this.metaManager.updateAccess(graphSpace, access);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "update access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "update access", e);
        }
    }

    @Override
    public HugeAccess deleteAccess(String graphSpace, Id id,
                                   boolean required) {

        try {
            if (required) {
                verifyUserPermission(graphSpace, HugePermission.DELETE,
                                     this.metaManager.getAccess(graphSpace,
                                                                id));
            }
            HugeAccess result = this.metaManager.deleteAccess(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "delete access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "delete access", e);
        }
    }

    @Override
    public HugeAccess getAccess(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.getAccess(graphSpace, id));
            }
            return this.metaManager.getAccess(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access", e);
        }
    }

    @Override
    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids,
                                       boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listAccess(graphSpace, ids));
            }
            return this.metaManager.listAccess(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list", e);
        }
    }

    @Override
    public List<HugeAccess> listAllAccess(String graphSpace, long limit,
                                          boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listAllAccess(graphSpace, limit));
            }
            return this.metaManager.listAllAccess(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get all access list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get all access list", e);
        }
    }

    @Override
    public List<HugeAccess> listAccessByRole(String graphSpace, Id role,
                                             long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listAccessByRole(graphSpace, role,
                                                                              limit));
            }
            return this.metaManager.listAccessByRole(graphSpace, role,
                                                     limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list by role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list by role", e);
        }
    }

    @Override
    public List<HugeAccess> listAccessByTarget(String graphSpace, Id target,
                                               long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(graphSpace, HugePermission.READ,
                                            this.metaManager.listAccessByTarget(graphSpace, target,
                                                                                limit));
            }
            return this.metaManager.listAccessByTarget(graphSpace,
                                                       target, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list by target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list by target", e);
        }
    }

    /*
     * For non-default role permissions, copy targets and accesses to subgraph
     * for default roles, copy belongs to default roles of subgraph
     * */
    @Override
    public void copyPermissionsToOther(String graphSpace, String origin,
                                       String nickname, boolean auth) {
        copyPermissionsToOther(graphSpace, origin, graphSpace, nickname, auth);
    }

    @Override
    public void copyPermissionsToOther(String originSpace, String origin,
                                       String otherSpace, String nickname,
                                       boolean auth) {
        List<HugeTarget> targets = this.listAllTargets(originSpace, -1,
                                                       false);
        for (HugeTarget target : targets) {
            if (!target.graph().equals(origin) ||
                HugeDefaultRole.isDefaultTarget(target.name())) {
                continue;
            }
            String subName = nickname + "_" + target.name();
            HugeTarget targetSub = new HugeTarget(subName, otherSpace,
                                                  nickname,
                                                  target.description());
            targetSub.resources(target.resources());
            targetSub.id(this.createTarget(otherSpace, targetSub,
                                           false));

            List<HugeAccess> accesses = this.listAccessByTarget(
                    originSpace, target.id(), -1, false);
            for (HugeAccess access : accesses) {
                HugeAccess accessSub = new HugeAccess(otherSpace,
                                                      access.source(),
                                                      targetSub.id());
                accessSub.permission(access.permission());
                accessSub.description(access.description());
                accessSub.id(this.createAccess(otherSpace, accessSub, false));
            }
        }

        if (auth) {
            // copy graph default role belongs
            // graph default roles : observer
            Id observer = IdGenerator.of(origin + "_" +
                                         HugeDefaultRole.OBSERVER);
            Id subObserver = IdGenerator.of(nickname + "_" +
                                            HugeDefaultRole.OBSERVER);

            List<HugeBelong> belongsUR =
                    this.listBelongByTarget(originSpace, observer,
                                            HugeBelong.UR, -1, false);
            for (HugeBelong belong : belongsUR) {
                HugeBelong belongSub = new HugeBelong(otherSpace, belong.source(),
                                                      null, subObserver,
                                                      belong.link());
                belong.id(this.createBelong(otherSpace, belongSub, false));
            }

            List<HugeBelong> belongsGR =
                    this.listBelongByTarget(originSpace, observer,
                                            HugeBelong.GR, -1, false);
            for (HugeBelong belong : belongsGR) {
                HugeBelong belongSub = new HugeBelong(otherSpace,
                                                      null,
                                                      belong.source(),
                                                      subObserver,
                                                      belong.link());
                belong.id(this.createBelong(otherSpace, belongSub, false));
            }
        }
    }

    @Override
    public void updateNicknamePermission(String graphSpace, String origin,
                                         String nickname, boolean auth) {
        if (auth) {
            this.createGraphDefaultRole(graphSpace, nickname);
        }
        this.copyPermissionsToOther(graphSpace, origin, nickname, auth);
        if (auth) {
            this.deleteGraphDefaultRole(graphSpace, origin);
        }
    }

    @Override
    public List<String> listWhiteIp() {
        return this.metaManager.getWhiteIpList();
    }

    @Override
    public void setWhiteIpList(List<String> whiteIpList) {
        this.metaManager.setWhiteIpList(whiteIpList);
    }

    @Override
    public boolean getWhiteIpStatus() {
        return this.metaManager.getWhiteIpStatus();
    }

    @Override
    public void setWhiteIpStatus(boolean status) {
        this.metaManager.setWhiteIpStatus(status);
    }

    @Override
    public List<String> listGraphSpace() {
        return metaManager.listGraphSpace();
    }

    @Override
    public HugeUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");

        HugeUser user = this.findUser(name, false);
        if (user == null) {
            return null;
        }

        if (password.equals(this.pwdCache.get(user.id()))) {
            return user;
        }

        if (StringEncoding.checkPassword(password, user.password())) {
            this.pwdCache.update(user.id(), password);
            return user;
        }
        return null;
    }

    @Override
    public RolePermission rolePermission(AuthElement element) {
        return this.rolePermissionInner(element);
    }

    public RolePermission rolePermissionInner(AuthElement element) {
        if (element instanceof HugeUser) {
            return this.rolePermission((HugeUser) element);
        } else if (element instanceof HugeGroup) {
            return this.rolePermission((HugeGroup) element);
        } else if (element instanceof HugeTarget) {
            return this.rolePermission((HugeTarget) element);
        }

        List<HugeAccess> accesses = new ArrayList<>();
        if (element instanceof HugeBelong) {
            HugeBelong belong = (HugeBelong) element;
            accesses.addAll(this.listAccessByRole(belong.graphSpace(),
                                                  belong.target(), -1, false));
        } else if (element instanceof HugeRole) {
            HugeRole role = (HugeRole) element;
            accesses.addAll(this.listAccessByRole(role.graphSpace(),
                                                  role.id(), -1, false));
        } else if (element instanceof HugeAccess) {
            HugeAccess access = (HugeAccess) element;
            accesses.add(access);
        } else {
            E.checkArgument(false, "Invalid type for role permission: %s",
                            element);
        }

        return this.rolePermission(accesses);
    }

    private RolePermission rolePermission(HugeUser user) {
        if (user.role() != null && user.role().map() != null &&
            user.role().map().size() != 0) {
            // Return cached role (40ms => 10ms)
            return user.role();
        }

        // Collect accesses by user
        RolePermission role = (isAdminManager(user.name())) ?
                              RolePermission.admin() : new RolePermission();
        List<String> graphSpaces = this.listGraphSpace();
        List<HugeGroup> groups = this.listGroupsByUser(user.name(), -1, false);
        for (String graphSpace : graphSpaces) {
            List<HugeBelong> belongs = this.listBelongBySource(graphSpace,
                                                               user.id(),
                                                               HugeBelong.ALL,
                                                               -1, false);
            for (HugeGroup group : groups) {
                List<HugeBelong> belongsG =
                        this.listBelongBySource(graphSpace, group.id(),
                                                HugeBelong.ALL, -1, false);
                belongs.addAll(belongsG);
            }
            for (HugeBelong belong : belongs) {
                List<HugeAccess> accesses = this.listAccessByRole(graphSpace,
                                                                  belong.target(), -1, false);
                for (HugeAccess access : accesses) {
                    HugePermission accessPerm = access.permission();
                    HugeTarget target = this.getTarget(graphSpace,
                                                       access.target(), false);
                    role.add(graphSpace, target.graph(),
                             accessPerm, target.resources());
                }
            }
        }

        user.role(role);
        this.usersCache.update(IdGenerator.of(user.name()), user);
        return role;
    }

    private RolePermission rolePermission(HugeGroup group) {
        List<String> graphSpaces = this.listGraphSpace();
        RolePermission role = new RolePermission();
        for (String graphSpace : graphSpaces) {
            List<HugeBelong> belongs = this.listBelongBySource(graphSpace,
                                                               group.id(),
                                                               HugeBelong.ALL,
                                                               -1, false);
            for (HugeBelong belong : belongs) {
                List<HugeAccess> accesses = this.listAccessByRole(graphSpace,
                                                                  belong.target(), -1, false);
                for (HugeAccess access : accesses) {
                    HugePermission accessPerm = access.permission();
                    HugeTarget target = this.getTarget(graphSpace,
                                                       access.target(), false);
                    role.add(graphSpace, target.graph(),
                             accessPerm, target.resources());
                }
            }
        }
        return role;
    }

    private RolePermission rolePermission(List<HugeAccess> accesses) {
        // Mapping of: graph -> action -> resource
        RolePermission role = new RolePermission();
        for (HugeAccess access : accesses) {
            HugePermission accessPerm = access.permission();
            HugeTarget target = this.getTarget(access.graphSpace(),
                                               access.target(), false);
            role.add(target.graphSpace(), target.graph(),
                     accessPerm, target.resources());
        }
        return role;
    }

    private RolePermission rolePermission(HugeTarget target) {
        RolePermission role = new RolePermission();
        // TODO: improve for the actual meaning
        role.add(target.graphSpace(), target.graph(), HugePermission.READ, target.resources());
        return role;
    }

    @Override
    public String loginUser(String username, String password,
                            long expire)
            throws AuthenticationException {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            String msg = "Incorrect username or password";
            throw new AuthenticationException(msg);
        }

        Map<String, ?> payload =
                ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                username,
                                AuthConstant.TOKEN_USER_ID,
                                user.id.asString(),
                                AuthConstant.TOKEN_USER_PASSWORD,
                                user.password());
        expire = expire == 0L ? AUTH_TOKEN_EXPIRE : expire;
        String token = this.tokenGenerator.create(payload, expire * 1000);
        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public void logoutUser(String token) {
        this.tokenCache.invalidate(IdGenerator.of(token));
    }

    @Override
    public String createToken(String username) {
        // createToken for computer
        HugeUser user = this.findUser(username, false);
        if (user == null) {
            return null;
        }

        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 username,
                                                 AuthConstant.TOKEN_USER_ID,
                                                 user.id.asString());
        String token = this.tokenGenerator.create(payload, AUTH_TOKEN_EXPIRE * 1000);
        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public Id createKgUser(HugeUser user) {
        try {
            user.creator("KG");
            user.update(new Date());
            user.create(user.update());
            this.metaManager.createUser(user);
            return IdGenerator.of(user.name());
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize user", e);
        }
    }

    @Override
    public String createToken(String username, long expire) {
        HugeUser user = this.findUser(username, false);
        if (user == null) {
            return null;
        }

        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 username,
                                                 AuthConstant.TOKEN_USER_ID,
                                                 user.id.asString());
        String token = this.tokenGenerator.create(payload, expire * 1000);
        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public void processEvent(MetaManager.AuthEvent event) {
        this.invalidateUserCache();
        this.invalidatePasswordCache(IdGenerator.of(event.id()));
        if (event.op().equals("DELETE") && event.type().equals("USER")) {

        } else if (event.op().equals("DELETE") && event.type().equals("GROUP")) {

        } else if (event.op().equals("UPDATE") && event.type().equals("TARGET")) {

        } else if (event.op().equals("DELETE") && event.type().equals("TARGET")) {

        } else if (event.op().equals("CREATE") && event.type().equals("BELONG")) {

        } else if (event.op().equals("DELETE") && event.type().equals("BELONG")) {

        } else if (event.op().equals("CREATE") && event.type().equals("ACCESS")) {

        } else if (event.op().equals("DELETE") && event.type().equals("ACCESS")) {

        }
    }

    @Override
    public UserWithRole validateUser(String username, String password) {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            return new UserWithRole(username);
        }
        RolePermission role;
        if (user.role() != null) {
            // role from cache
            role = new RolePermission(user.role().roles());
        } else {
            // generate user role from pd
            role = this.rolePermission(user);
        }
        return new UserWithRole(user.id, username, role);
    }

    @Override
    public UserWithRole validateUser(String token) {
        String username = this.tokenCache.get(IdGenerator.of(token));
        Claims payload = null;

        try {
            payload = this.tokenGenerator.verify(token);
        } catch (Throwable t) {
            LOG.error(String.format("Failed to verify token:[ %s ], cause:", token), t);
            return new UserWithRole(username);
        }

        boolean needBuildCache = false;
        if (username == null) {
            username = (String) payload.get(AuthConstant.TOKEN_USER_NAME);
            needBuildCache = true;
        }

        HugeUser user = this.findUser(username, false);
        if (user == null) {
            // todo: why return a new user
            return new UserWithRole(username);
        } else if (needBuildCache) {
            long expireAt = payload.getExpiration().getTime();
            long bornTime = this.tokenCache.expire() -
                            (expireAt - System.currentTimeMillis());
            this.tokenCache.update(IdGenerator.of(token), username,
                                   Math.negateExact(bornTime));
        }

        if (payload.get(AuthConstant.TOKEN_USER_PASSWORD) != null) {
            String tokenPassword =
                    (String) payload.get(AuthConstant.TOKEN_USER_PASSWORD);
            if (tokenPassword.equals(user.password())) {
                RolePermission role;
                if (user.role() != null) {
                    // role from cache
                    role = new RolePermission(user.role().roles());
                } else {
                    // generate user role from pd
                    role = this.rolePermission(user);
                }
                return new UserWithRole(user.id, username, role);
            }

            return new UserWithRole(username);
        }

        return new UserWithRole(user.id(), username, this.rolePermission(user));
    }

    @Override
    public String username() {
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when verifying resource permission");
        return context.user().username();
    }

    public void initAdmin() {
        HugeUser user = new HugeUser("admin");
        user.nickname("超级管理员");
        user.password(StringEncoding.hashPassword("admin"));
        user.creator(RestServer.EXECUTOR);
        user.phone("18888886666");
        user.email("admin@hugegraph.com");
        user.description("None");
        user.update(new Date());
        user.create(new Date());
        user.avatar("/image.png");
        try {
            this.metaManager.createUser(user);
            this.metaManager.initDefaultGraphSpace();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
    }
}
