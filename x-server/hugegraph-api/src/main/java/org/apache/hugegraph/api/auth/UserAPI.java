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

package org.apache.hugegraph.api.auth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("auth/users")
@Singleton
public class UserAPI extends API {

    private static final Logger LOGGER = Log.logger(RestServer.class);

    private static void checkUserName(String name) {
        E.checkArgument(!StringUtils.isEmpty(name) &&
                        name.matches(USER_NAME_PATTERN),
                        "The name is 4-16 characters " +
                        "and can only contain letters, " +
                        "numbers or underscores");
    }

    private static void checkUserNickname(String nickname) {
        E.checkArgument(!StringUtils.isEmpty(nickname) &&
                        nickname.matches(USER_NICKNAME_PATTERN),
                        "Invalid nickname '%s', valid name is " +
                        "up to 16 letters, Chinese or underscore " +
                        "characters, and cannot start and end with an" +
                        " underscore", nickname);
    }

    private static void checkUserPassword(String password) {
        E.checkArgument(!StringUtils.isEmpty(password) &&
                        password.matches(USER_PASSWORD_PATTERN),
                        "The password is 5-16 characters, " +
                        "which can be letters, numbers or " +
                        "special symbols");
    }

    protected static Id parseId(String id) {
        return IdGenerator.of(id);
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String create(@Context GraphManager manager, JsonUser jsonUser) {
        LOGGER.debug("Create user: {}", jsonUser);
        checkCreatingBody(jsonUser);

        HugeUser user = jsonUser.build();
        AuthManager authManager = manager.authManager();
        user.id(authManager.createUser(user, true));
        // TODO: duplicated log
        //LOGGER.getAuditLogger().logCreateUser(user.idString(), manager.authManager().username());
        return manager.serializer().writeAuthElement(user);
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @Path("batch")
    @RolesAllowed({"admin"})
    public Map<String, Object> createBatch(@Context GraphManager manager,
                                           List<Map<String, String>> data) {
        LOGGER.debug("Create batch users.");
        AuthManager authManager = manager.authManager();
        List<Map<String, String>> resultList = new ArrayList<>(data.size());
        for (Map<String, String> info : data) {
            Map<String, String> res = new HashMap<>();
            if (StringUtils.isEmpty(info.get("user_name"))) {
                res.put("result", "error: parameter 'user_name' not found.");
                continue;
            }
            HugeUser user = new HugeUser(info.get("user_name"));
            user.nickname(info.get("user_nickname"));
            user.password(info.get("user_password"));
            user.description(info.get("user_description"));

            res.put("user_name", user.name());
            try {
                checkUserCreate(user);
                user.password(StringEncoding.hashPassword(user.password()));
                user.id(authManager.createUser(user, true));
                res.put("result", "successfully created");
            } catch (Exception e) {
                if (e.getMessage().contains("existed")) {
                    res.put("result", "error: user_name existed");
                } else {
                    res.put("result", "error: " + e.getMessage());
                }
            }

            resultList.add(res);
            // TODO: duplicated log
            //LOGGER.getAuditLogger().logCreateUser(
            //        user.idString(), manager.authManager().username());
        }
        return ImmutableMap.of("result", resultList);
    }

    public void checkUserCreate(HugeUser user) {
        checkUserName(user.name());
        checkUserPassword(user.password());
        if (StringUtils.isEmpty(user.nickname())) {
            user.nickname(user.name());
        }
        checkUserNickname(user.nickname());
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("id") String id,
                         JsonUser jsonUser) {
        LOGGER.debug("Update user: {}", jsonUser);
        // todo adjust other superAdmins update admin
        // E.checkArgument(!"admin".equals(id),
        //                 "User 'admin' can't be updated");
        checkUpdatingBody(jsonUser);
        HugeUser user;
        AuthManager authManager = manager.authManager();
        try {
            user = authManager.getUser(UserAPI.parseId(id), true);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid user id: " + id);
        }
        user = jsonUser.build(user);
        if (Strings.isNotBlank(user.password())) {
            // TODO: duplicated log
            //LOGGER.getAuditLogger().logUpdatePassword(user.idString());
        }
        user = authManager.updateUser(user, true);
        // TODO: duplicated log
        //LOGGER.getAuditLogger().logUpdateUser(user.idString(), authManager.username());
        return manager.serializer().writeAuthElement(user);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @QueryParam("group") String group,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOGGER.debug("List users");

        AuthManager authManager = manager.authManager();
        boolean required = !isManager(authManager, authManager.username());

        List<HugeUser> users;
        if (StringUtils.isEmpty(group)) {
            users = authManager.listAllUsers(limit, required);
        } else {
            users = authManager.listUsersByGroup(group, limit, required);
        }

        return manager.serializer().writeAuthElements("users", users);
    }

    public boolean isManager(AuthManager authManager, String username) {
        return authManager.isAdminManager(username) ||
               authManager.isSpaceManager(username) ||
               authManager.isDefaultRole(username, HugeDefaultRole.ANALYST);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("id") String id) {
        LOGGER.debug("Get user: {}", id);

        AuthManager authManager = manager.authManager();
        boolean required = !isManager(authManager, authManager.username());

        HugeUser user = authManager.getUser(IdGenerator.of(id), required);
        return manager.serializer().writeAuthElement(user);
    }

    @GET
    @Timed
    @Path("{id}/role")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String role(@Context GraphManager manager,
                       @PathParam("id") String id) {
        LOGGER.debug("Get user role: {}", id);

        AuthManager authManager = manager.authManager();
        HugeUser user = authManager.getUser(IdGenerator.of(id), true);
        return manager.authManager().rolePermission(user).toJson();
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @PathParam("id") String id) {
        LOGGER.debug("Delete user: {}", id);
        E.checkArgument(!"admin".equals(id),
                        "User 'admin' can't be deleted");

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteUser(IdGenerator.of(id), true);
            // TODO: duplicated log
            //LOGGER.getAuditLogger().logDeleteUser(id, authManager.username());
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid user id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "user_creator",
                                   "user_create", "user_update"})
    private static class JsonUser implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("user_nickname")
        private String nickname;
        @JsonProperty("user_password")
        private String password;
        @JsonProperty("user_phone")
        private String phone;
        @JsonProperty("user_email")
        private String email;
        @JsonProperty("user_avatar")
        private String avatar;
        @JsonProperty("user_description")
        private String description;

        public static JsonUser fromHugeUser(HugeUser user) {
            JsonUser jsonUser = new JsonUser();
            jsonUser.name = user.name();
            jsonUser.nickname = user.nickname();
            jsonUser.password = user.password();
            jsonUser.description = user.description();
            return jsonUser;
        }

        public HugeUser build(HugeUser user) {
            E.checkArgument(StringUtils.isEmpty(this.name) || user.name().equals(this.name),
                            "The name of user can't be updated");
            if (!StringUtils.isEmpty(this.nickname)) {
                user.nickname(this.nickname);
            }
            if (!StringUtils.isEmpty(this.password) &&
                !StringEncoding.checkPassword(password, user.password())) {
                // do not change pw if get null or same pw as before
                user.password(StringEncoding.hashPassword(this.password));
            }
            if (!StringUtils.isEmpty(this.phone)) {
                user.phone(this.phone);
            }
            if (!StringUtils.isEmpty(this.email)) {
                user.email(this.email);
            }
            if (!StringUtils.isEmpty(this.avatar)) {
                user.avatar(this.avatar);
            }
            if (!StringUtils.isEmpty(this.description)) {
                user.description(this.description);
            }
            return user;
        }

        public HugeUser build() {
            HugeUser user = new HugeUser(this.name);
            user.nickname(this.nickname);
            user.password(StringEncoding.hashPassword(this.password));
            user.phone(this.phone);
            user.email(this.email);
            user.avatar(this.avatar);
            user.description(this.description);
            return user;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            checkUserName(this.name);
            checkUserPassword(this.password);
            if (StringUtils.isEmpty(this.nickname)) {
                this.nickname = this.name;
            }
            checkUserNickname(this.nickname);
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(!StringUtils.isEmpty(this.password) ||
                            !StringUtils.isEmpty(this.nickname) ||
                            !StringUtils.isEmpty(this.description),
                            "Expect one of user " +
                            "nickname/password/description");
            if (!StringUtils.isEmpty(this.password)) {
                checkUserPassword(this.password);
            }

            if (!StringUtils.isEmpty(this.nickname)) {
                checkUserNickname(this.nickname);
            }
        }
    }
}
