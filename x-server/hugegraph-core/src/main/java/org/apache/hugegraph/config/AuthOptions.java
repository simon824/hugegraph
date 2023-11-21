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

package org.apache.hugegraph.config;

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeDouble;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

public class AuthOptions extends OptionHolder {

    public static final ConfigOption<String> AUTH_TOKEN_SECRET =
            new ConfigOption<>(
                    "auth.token_secret",
                    "Secret key of HS256 algorithm.",
                    disallowEmpty(),
                    "FXQXbJtbCLxODc6tGci732pkH1cyf8Qg"
            );
    public static final ConfigOption<Double> AUTH_AUDIT_LOG_RATE =
            new ConfigOption<>(
                    "auth.audit_log_rate",
                    "The max rate of audit log output per user, " +
                    "default value is 1000 records per second.",
                    rangeDouble(0.0, Double.MAX_VALUE),
                    1000.0
            );
    public static final ConfigOption<Long> AUTH_PROXY_CACHE_EXPIRE =
            new ConfigOption<>(
                    "auth.proxy_cache_expire",
                    "The expiration time in seconds of auth cache in " +
                    "auth client.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (1 * 60L)
            );
    public static final ConfigOption<Long> AUTH_CACHE_CAPACITY =
            new ConfigOption<>(
                    "auth.cache_capacity",
                    "The max cache capacity of each auth cache item.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (1024 * 10L)
            );
    private static volatile AuthOptions instance;

    private AuthOptions() {
        super();
    }

    public static synchronized AuthOptions instance() {
        if (instance == null) {
            instance = new AuthOptions();
            instance.registerOptions();
        }
        return instance;
    }
}
