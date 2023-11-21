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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.security.sasl.AuthenticationException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.AuthenticationFilter;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthConstant;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.UserWithRole;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;

@Path("auth")
@Singleton
public class LoginAPI extends API {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    private static final Logger LOGGER = Log.logger(RestServer.class);

    @POST
    @Timed
    @Path("login")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String login(@Context GraphManager manager, JsonLogin jsonLogin) {
        LOGGER.debug("User login: {}", jsonLogin);
        checkCreatingBody(jsonLogin);

        try {
            AuthManager authManager = manager.authManager();
            String token = authManager.loginUser(jsonLogin.name,
                                                 jsonLogin.password,
                                                 jsonLogin.expire);
            LOGGER.info("[AUDIT] User {} login in via {} with path {}",
                        jsonLogin.name, "api", "/auth/login");
            return manager.serializer()
                          .writeMap(ImmutableMap.of("token", token));
        } catch (AuthenticationException e) {
            throw new NotAuthorizedException(e.getMessage(), e);
        }
    }

    @DELETE
    @Timed
    @Path("logout")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public void logout(@Context GraphManager manager,
                       @HeaderParam(HttpHeaders.AUTHORIZATION) String auth) {
        E.checkArgument(StringUtils.isNotEmpty(auth),
                        "Request header Authorization must not be null");
        LOGGER.debug("User logout: {}", auth);

        if (!auth.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                    "Only HTTP Bearer authentication is supported");
        }

        String username = manager.authManager().username();

        String token = auth.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                              .length());
        AuthManager authManager = manager.authManager();
        authManager.logoutUser(token);
        // TODO: duplicated log
        //LOGGER.getAuditLogger().logUserLogout(username);
    }

    @GET
    @Timed
    @Path("verify")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String verifyToken(@Context GraphManager manager,
                              @HeaderParam(HttpHeaders.AUTHORIZATION)
                              String token) {
        E.checkArgument(StringUtils.isNotEmpty(token),
                        "Request header Authorization must not be null");
        LOGGER.debug("verify token: {}", token);

        if (!token.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                    "Only HTTP Bearer authentication is supported");
        }

        token = token.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                        .length());
        AuthManager authManager = manager.authManager();
        UserWithRole userWithRole = authManager.validateUser(token);

        return manager.serializer()
                      .writeMap(ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                userWithRole.username(),
                                                AuthConstant.TOKEN_USER_ID,
                                                userWithRole.userId()));
    }

    @POST
    @Timed
    @Path("kgusers")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String kgUserCreate(@Context GraphManager manager,
                               JsonKgUser kgUserJson) {
        LOGGER.debug("Kg user create: {}", kgUserJson);
        checkCreatingBody(kgUserJson);

        AuthManager authManager = manager.authManager();
        HugeUser user = authManager.findUser(kgUserJson.name, false);
        E.checkArgument(user == null,
                        "Already exist kg user: %s", kgUserJson.name);

        user = new HugeUser(kgUserJson.name);
        user.password(StringEncoding.hashPassword(user.name()));
        user.description("KG user");
        user.id(authManager.createKgUser(user));
        // TODO: duplicated log
        //LOGGER.getAuditLogger().logCreateUser(user.idString(),
        //                                      manager.authManager().username());
        return manager.serializer().writeAuthElement(user);
    }

    @POST
    @Timed
    @Path("kglogin")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String kgLogin(@Context GraphManager manager,
                          KgJsonLogin jsonLogin) {
        LOGGER.debug("Kg user login: {}", jsonLogin);
        checkCreatingBody(jsonLogin);
        String content = String.format("%s:%s", jsonLogin.name,
                                       DATE_FORMAT.format(new Date()));
        String sign = DigestUtils.md5Hex(content).toLowerCase();
        E.checkArgument(sign.equals(jsonLogin.sign.toLowerCase()),
                        "Invalid signature");

        AuthManager authManager = manager.authManager();
        HugeUser user = authManager.findUser(jsonLogin.name, false);
        String token;
        if (user == null) {
            token = Strings.EMPTY;
        } else {
            token = authManager.createToken(jsonLogin.name, jsonLogin.expire);
        }
        // TODO: duplicated log
        //LOGGER.getAuditLogger().logUserLogin(jsonLogin.name, "kglogin",
        //                                     "/auth/kglogin");
        return manager.serializer()
                      .writeMap(ImmutableMap.of("token", token));
    }

    private static class JsonLogin implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("user_password")
        private String password;
        @JsonProperty("token_expire")
        private long expire;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name) &&
                            this.name.matches(USER_NAME_PATTERN),
                            "The name is 4-16 characters " +
                            "and can only contain letters, " +
                            "numbers or underscores");
            E.checkArgument(!StringUtils.isEmpty(this.password) &&
                            this.password.matches(USER_PASSWORD_PATTERN),
                            "The password is 5-16 characters, " +
                            "which can be letters, numbers or " +
                            "special symbols");
            E.checkArgument(this.expire >= 0 &&
                            this.expire <= Long.MAX_VALUE,
                            "The token_expire should be in " +
                            "[0, Long.MAX_VALUE]");
        }

        @Override
        public void checkUpdate() {
        }
    }

    private static class KgJsonLogin implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("token_expire")
        private long expire = 315360000;
        @JsonProperty("sign")
        private String sign;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name) &&
                            this.name.matches(USER_NAME_PATTERN),
                            "The name is 4-16 characters " +
                            "and can only contain letters, " +
                            "numbers or underscores");
            E.checkArgument(this.expire >= 0 &&
                            this.expire <= Long.MAX_VALUE,
                            "The token_expire should be in " +
                            "[0, Long.MAX_VALUE]");
        }

        @Override
        public void checkUpdate() {
        }
    }

    private static class JsonKgUser implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("token_expire")
        private long expire = 315360000;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name) &&
                            this.name.matches(USER_NAME_PATTERN),
                            "The name is 4-16 characters " +
                            "and can only contain letters, " +
                            "numbers or underscores");
            E.checkArgument(this.expire >= 0,
                            "The token_expire should be in " +
                            "[0, Long.MAX_VALUE]");
        }

        @Override
        public void checkUpdate() {
        }
    }
}
