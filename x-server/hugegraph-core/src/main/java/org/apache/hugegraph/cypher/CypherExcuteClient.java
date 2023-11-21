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

package org.apache.hugegraph.cypher;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import jakarta.ws.rs.core.HttpHeaders;

/**
 * cypher 请求client
 */
public class CypherExcuteClient {

    private static final Logger LOG = Log.logger(CypherExcuteClient.class);

    private static final Charset UTF8 = Charset.forName(StandardCharsets.UTF_8.name());
    private final Base64.Decoder decoder = Base64.getUrlDecoder();
    private final String basic = "Basic ";
    private final String bearer = "Bearer ";

    private CypherManager cypherManager;

    public CypherExcuteClient() {
        this.cypherManager();
    }

    private CypherManager cypherManager() {
        if (this.cypherManager == null) {
            this.cypherManager = CypherManager.configOf("conf/remote-objects.yaml");
        }
        return this.cypherManager;
    }

    /**
     * 权限校验
     *
     * @param headers
     * @return
     */
    public CypherClient client(Object headers) {
        String auth = null;
        if (headers instanceof HttpHeaders) {
            HttpHeaders httpHeaders = (HttpHeaders) headers;
            auth = httpHeaders.getHeaderString("ICBC-Authorization");
            if (auth == null) {
                auth = httpHeaders.getHeaderString(HttpHeaders.AUTHORIZATION);
            }
        } else if (headers instanceof String) {
            auth = ((String) headers);
        }
        return vertifyAuth(auth);
    }

    private CypherClient vertifyAuth(String auth) {
        if (auth != null && !auth.isEmpty()) {
            auth = auth.split(",")[0];
        }

        if (auth == null) {
            throw new HugeException("The Cypher is being called without any authorization.");
        }

        if (auth.startsWith(basic)) {
            return this.clientViaBasic(auth);
        } else if (auth.startsWith(bearer)) {
            return this.clientViaToken(auth);
        }

        throw new HugeException("The Cypher is being called without any authorization.");
    }

    private CypherClient clientViaBasic(String auth) {
        Pair<String, String> userPass = this.toUserPass(auth);
        E.checkNotNull(userPass, "user-password-pair");

        return this.cypherManager().getClient(userPass.getLeft(), userPass.getRight());
    }

    private CypherClient clientViaToken(String auth) {
        return this.cypherManager().getClient(auth.substring(bearer.length()));
    }

    private Pair<String, String> toUserPass(String auth) {
        if (auth == null || auth.isEmpty()) {
            return null;
        }
        if (!auth.startsWith(basic)) {
            return null;
        }

        String[] split = null;

        try {
            String encoded = auth.substring(basic.length());
            byte[] userPass = this.decoder.decode(encoded);
            String authorization = new String(userPass, UTF8);
            split = authorization.split(":");
        } catch (Exception e) {
            LOG.error("Failed convert auth to credential.", e);
            return null;
        }

        if (split.length != 2) {
            return null;
        }

        return ImmutablePair.of(split[0], split[1]);
    }
}
