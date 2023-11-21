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

package org.apache.hugegraph.misc;

/**
 * @author lynn.bond@hotmail.com on 2023/2/20
 */
public class CypherViaClientTest {
/*
    public static void main(String[] args)  {
        //Cluster cluster = Cluster.open("conf/remote-objects.yaml");
        //Cluster cluster = Cluster.open(getConfig());
        Cluster cluster = Cluster.open(getYamlConfig());
        Client client = cluster.connect();

        //String cypherQuery = "MATCH (n) RETURN n.age";
        //String cypherQuery = "MATCH (n:person) where n.city ='Beijing' return n";
        //String cypherQuery = "MATCH (n:person) where  size(n.name)>4 return n";
        //String cypherQuery = "MATCH (n:person) where n.city ='Beijing' and size(n.city)>1
        return n";
        String cypherQuery = "match(n:person) return n.name,size(n.name)";
        RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("cypher")
                .add(Tokens.ARGS_GREMLIN, cypherQuery)
                .create();

        ResultSet results = null;
        try {
            results = client.submitAsync(request).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        Iterator<Result> iter = results.iterator();
        for (; iter.hasNext(); ) {
            Result res = iter.next();
            LinkedHashMap map = res.get(LinkedHashMap.class);
            System.out.println(map);
        }

        client.close();
        cluster.close();
    }

    private static Configuration getConfig() {
        BaseConfigurationEx conf = new BaseConfigurationEx();

        Configuration yaml = loadYaml();

        conf.addProperty("hosts", yaml.getProperty("hosts"));
        conf.addProperty("port", yaml.getProperty("port"));
        conf.addProperty("serializer.className", yaml.getProperty("serializer.className"));
        conf.addProperty("serializer.config.serializeResultToString", yaml.getProperty
        ("serializer.config.serializeResultToString"));

        conf.addPropertyDirect("serializer.config.ioRegistries", yaml.getProperty("serializer
        .config.ioRegistries"));
        conf.addProperty("username", "admin");
        conf.addProperty("password", "admin");

        return conf;
    }

    private static Configuration getYamlConfig() {
        Configuration yaml = loadYaml();
        yaml.addProperty("username", "admin");
        yaml.addProperty("password", "admin");
        return yaml;
    }

    private static Configuration loadYaml() {
        File yamlFile =  getYamlFile("conf/remote-objects.yaml");
        Reader reader = null;
        try {
            reader = new FileReader(yamlFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        YamlConfiguration yaml = new YamlConfiguration();
        try {
            yaml.load(reader);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        return yaml;

    }

    private static File getYamlFile(String configurationFile) {
        final File systemFile = new File(configurationFile);
        if (!systemFile.exists()) {
            final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            final URL resource = currentClassLoader.getResource(configurationFile);
            final File resourceFile = new File(resource.getFile());

            if (!resourceFile.exists()) {
                throw new IllegalArgumentException(String.format("Configuration file at %s does
                not exist", configurationFile));
            }
            return resourceFile;

        }
        return systemFile;
    }

    private static class BaseConfigurationEx extends BaseConfiguration {
        public void addPropertyDirect(String key, Object value) {
            super.addPropertyDirect(key, value);
        }
    }

 */
}


