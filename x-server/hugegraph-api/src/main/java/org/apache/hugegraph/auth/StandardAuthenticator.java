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

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;

public class StandardAuthenticator implements HugeAuthenticator {
    private static final byte NUL = 0;
    private static final String INITING_STORE = "initing_store";

    private AuthManager authManager = null;

    public static void initAdminUserIfNeeded(String confFile,
                                             List<String> metaEndpoints,
                                             String cluster,
                                             Boolean withCa,
                                             String caFile,
                                             String clientCaFile,
                                             String clientKeyFile)
            throws Exception {
        MetaManager metaManager = MetaManager.instance();
        HugeConfig config = new HugeConfig(confFile);
        if (!withCa) {
            caFile = null;
            clientCaFile = null;
            clientKeyFile = null;
        }

        metaManager.connect(cluster, MetaManager.MetaDriverType.ETCD, caFile,
                            clientCaFile, clientKeyFile, metaEndpoints);
        StandardAuthManager authManager = new StandardAuthManager(metaManager,
                                                                  config);
        authManager.initAdmin();
    }

    public static void initAdminUserIfNeeded(HugeConfig config,
                                             List<String> metaEndpoints,
                                             String cluster,
                                             Boolean withCa,
                                             String caFile,
                                             String clientCaFile,
                                             String clientKeyFile) {
        MetaManager metaManager = MetaManager.instance();
        if (!withCa) {
            caFile = null;
            clientCaFile = null;
            clientKeyFile = null;
        }

        metaManager.connect(cluster, MetaManager.MetaDriverType.ETCD, caFile,
                            clientCaFile, clientKeyFile, metaEndpoints);
        StandardAuthManager authManager =
                new StandardAuthManager(metaManager, config);
        authManager.initAdmin();
    }

    @Override
    public void setup(HugeConfig config) {
        String cluster = config.get(ServerOptions.CLUSTER);
        List<String> endpoints = config.get(ServerOptions.META_ENDPOINTS);
        boolean useCa = config.get(ServerOptions.META_USE_CA);
        String ca = null;
        String clientCa = null;
        String clientKey = null;
        if (useCa) {
            ca = config.get(ServerOptions.META_CA);
            clientCa = config.get(ServerOptions.META_CLIENT_CA);
            clientKey = config.get(ServerOptions.META_CLIENT_KEY);
        }
        MetaManager metaManager = MetaManager.instance();
        metaManager.connect(cluster, MetaManager.MetaDriverType.ETCD,
                            ca, clientCa, clientKey, endpoints);
        this.authManager = new StandardAuthManager(metaManager,
                                                   config);
    }

    /**
     * Verify if a user is legal
     *
     * @param username the username for authentication
     * @param password the password for authentication
     * @param token    the token for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public UserWithRole authenticate(String username, String password,
                                     String token) {
        UserWithRole userWithRole;
        if (StringUtils.isNotEmpty(token)) {
            userWithRole = this.authManager().validateUser(token);
        } else {
            E.checkArgumentNotNull(username,
                                   "The username parameter can't be null");
            E.checkArgumentNotNull(password,
                                   "The password parameter can't be null");
            userWithRole = this.authManager().validateUser(username, password);
        }

        RolePermission role = userWithRole.role();

        if (role == null) {
            role = ROLE_NONE;
        } else if (USER_ADMIN.equals(userWithRole.username())) {
            role = ROLE_ADMIN;
        } else {
            return userWithRole;
        }

        return new UserWithRole(userWithRole.userId(),
                                userWithRole.username(), role);
    }

    @Override
    public AuthManager authManager() {
        E.checkState(this.authManager != null,
                     "Must setup authManager first");
        return this.authManager;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        return new PlainTextSaslAuthenticator();
    }

    /**
     * Originate from tinkerpop's one
     */
    private class PlainTextSaslAuthenticator implements SaslNegotiator {
        private boolean complete = false;
        private String username;
        private String password;
        private String token;

        @Override
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
            decodeCredentials(clientResponse);
            complete = true;
            return null;
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            if (!complete) {
                throw new AuthenticationException("SASL negotiation not complete");
            }
            final Map<String, String> credentials = new HashMap<>();
            credentials.put(PROPERTY_USERNAME, username);
            credentials.put(PROPERTY_PASSWORD, password);

            credentials.put(KEY_TOKEN, token);

            return authenticate(credentials);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}.
         *
         * @param bytes encoded credentials string sent by the client
         */
        private void decodeCredentials(byte[] bytes) throws AuthenticationException {
            byte[] user = null;
            byte[] pass = null;
            int end = bytes.length;
            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] == NUL) {
                    if (pass == null) {
                        pass = Arrays.copyOfRange(bytes, i + 1, end);
                    } else if (user == null) {
                        user = Arrays.copyOfRange(bytes, i + 1, end);
                    }
                    end = i;
                }
            }

            if (null == user) {
                throw new AuthenticationException("Authentication ID must not be null");
            }
            if (null == pass) {
                throw new AuthenticationException("Password must not be null");
            }

            username = new String(user, StandardCharsets.UTF_8);
            password = new String(pass, StandardCharsets.UTF_8);

            /* The trick is here. >_*/
            if (password.isEmpty()) {
                token = username;
            }
        }
    }
}
