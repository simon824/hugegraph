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

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.net.URL;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * @author lynn.bond@hotmail.com on 2023/2/23
 */
@NotThreadSafe
public final class CypherManager {
    private static final Logger LOG = Log.logger(CypherManager.class);
    private String configurationFile;
    private YAMLConfiguration configuration;

    private CypherManager(String configurationFile) {
        this.configurationFile = configurationFile;
    }

    public static CypherManager configOf(String configurationFile) {
        E.checkArgument(configurationFile != null && !configurationFile.isEmpty(),
                        "The configurationFile parameter can't be null or empty");
        return new CypherManager(configurationFile);
    }

    private static YAMLConfiguration loadYaml(String configurationFile) {
        File yamlFile = getConfigFile(configurationFile);
        YAMLConfiguration yaml;

        try {
            Reader reader = new FileReader(yamlFile);
            yaml = new YAMLConfiguration();
            yaml.read(reader);
            //yaml.load(reader);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to load configuration file," +
                                                     " the file at %s.", configurationFile), e);
        }

        return yaml;
    }

    private static File getConfigFile(String configurationFile) {
        final File systemFile = new File(configurationFile);

        if (!systemFile.exists()) {
            final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            final URL resource = currentClassLoader.getResource(configurationFile);
            final File resourceFile = new File(resource.getFile());

            if (!resourceFile.exists()) {
                throw new IllegalArgumentException(
                        String.format("Configuration file at %s does not exist",
                                      configurationFile));
            }
            return resourceFile;

        }

        return systemFile;
    }

    public CypherClient getClient(String userName, String password) {
        E.checkArgument(userName != null && !userName.isEmpty(),
                        "The userName parameter can't be null or empty");
        E.checkArgument(password != null && !password.isEmpty(),
                        "The password parameter can't be null or empty");

        //TODO: cache the client and make client to hold connection.
        return new CypherClient(userName, password, () -> this.cloneConfig());
    }

    public CypherClient getClient(String token) {
        E.checkArgument(token != null && !token.isEmpty(),
                        "The token parameter can't be null or empty");

        //TODO: cache the client and make client to hold connection.
        return new CypherClient(token, () -> this.cloneConfig());
    }

    private Configuration cloneConfig() {
        if (this.configuration == null) {
            this.configuration = loadYaml(this.configurationFile);
        }

        return (Configuration) this.configuration.clone();
    }
}
