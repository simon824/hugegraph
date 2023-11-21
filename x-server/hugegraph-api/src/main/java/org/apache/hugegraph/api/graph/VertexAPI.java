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

package org.apache.hugegraph.api.graph;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEPTH;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.security.RolesAllowed;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.api.filter.DecompressInterceptor.Decompress;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.SplicingIdGenerator;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.UpdateStrategy;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.PersonalRankTraverser;
import org.apache.hugegraph.traversal.optimize.QueryHolder;
import org.apache.hugegraph.traversal.optimize.Text;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

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

@Path("graphspaces/{graphspace}/graphs/{graph}/graph/vertices")
@Singleton
public class VertexAPI extends BatchAPI {

    /**
     * key: person_rank_{graphspace}_{graph}_sourceId
     */
    public static final String PERSON_RANK_CAL_KEY = "person_rank_%s_%s_%s";
    private static final Logger LOG = Log.logger(VertexAPI.class);
    private static final Map<String, Long> beforeGetHotGoodsDate = new HashMap<>();
    private static final Map<String, List<Map<String, Object>>> beforeGetHotGoodsVertices =
            new HashMap<>();
    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    public static Id checkAndParseVertexId(String idValue) {
        if (idValue == null) {
            return null;
        }
        boolean uuid = idValue.startsWith("U\"");
        if (uuid) {
            idValue = idValue.substring(1);
        }
        try {
            Object id = JsonUtil.fromJson(idValue, Object.class);
            return uuid ? Text.uuid((String) id) : HugeVertex.getIdValue(id);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "The vertex id must be formatted as Number/String/UUID" +
                    ", but got '%s'", idValue));
        }
    }

    private static void checkBatchSize(HugeConfig config,
                                       List<JsonVertex> vertices) {
        int max = config.get(ServerOptions.MAX_VERTICES_PER_BATCH);
        if (vertices.size() > max) {
            throw new IllegalArgumentException(String.format(
                    "Too many vertices for one time post, " +
                    "the maximum number is '%s'", max));
        }
        if (vertices.size() == 0) {
            throw new IllegalArgumentException(
                    "The number of vertices can't be 0");
        }
    }

    private static Id getVertexId(HugeGraph g, JsonVertex vertex) {
        VertexLabel vertexLabel = g.vertexLabel(vertex.label);
        String labelId = vertexLabel.id().asString();
        IdStrategy idStrategy = vertexLabel.idStrategy();
        E.checkArgument(idStrategy != IdStrategy.AUTOMATIC,
                        "Automatic Id strategy is not supported now");

        if (idStrategy == IdStrategy.PRIMARY_KEY) {
            List<Id> pkIds = vertexLabel.primaryKeys();
            List<Object> pkValues = new ArrayList<>(pkIds.size());
            for (Id pkId : pkIds) {
                String propertyKey = g.propertyKey(pkId).name();
                Object propertyValue = vertex.properties.get(propertyKey);
                E.checkArgument(propertyValue != null,
                                "The value of primary key '%s' can't be null",
                                propertyKey);
                pkValues.add(propertyValue);
            }

            String value = ConditionQuery.concatValues(pkValues);
            return SplicingIdGenerator.splicing(labelId, value);
        } else if (idStrategy == IdStrategy.CUSTOMIZE_UUID) {
            return Text.uuid(String.valueOf(vertex.id));
        } else {
            assert idStrategy == IdStrategy.CUSTOMIZE_NUMBER ||
                   idStrategy == IdStrategy.CUSTOMIZE_STRING;
            return HugeVertex.getIdValue(vertex.id);
        }
    }

    @POST
    @Timed(name = "single-create")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] create vertex: {}", graph, jsonVertex);
        checkCreatingBody(jsonVertex);

        HugeGraph g = graph(manager, graphSpace, graph);
        Vertex vertex = commit(g, () -> g.addVertex(jsonVertex.properties()));

        return manager.serializer().writeVertex(vertex);
    }

    @POST
    @Timed(name = "batch-create")
    @Decompress
    @Path("batch")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context HugeConfig config,
                         @Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         List<JsonVertex> jsonVertices) {
        LOG.debug("Graph [{}] create vertices: {}", graph, jsonVertices);
        checkCreatingBody(jsonVertices);
        checkBatchSize(config, jsonVertices);

        HugeGraph g = graph(manager, graphSpace, graph);

        return this.commit(config, g, jsonVertices.size(), () -> {
            List<Id> ids = new ArrayList<>(jsonVertices.size());
            for (JsonVertex vertex : jsonVertices) {
                ids.add((Id) g.addVertex(vertex.properties()).id());
            }
            return manager.serializer().writeIds(ids);
        });
    }

    /**
     * Batch update steps like:
     * 1. Get all newVertices' ID & combine first
     * 2. Get all oldVertices & update
     * 3. Add the final vertex together
     */
    @PUT
    @Timed(name = "batch-update")
    @Decompress
    @Path("batch")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context HugeConfig config,
                         @Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         BatchVertexRequest req) {
        BatchVertexRequest.checkUpdate(req);
        LOG.debug("Graph [{}] update vertices: {}", graph, req);
        checkUpdatingBody(req.jsonVertices);
        checkBatchSize(config, req.jsonVertices);

        HugeGraph g = graph(manager, graphSpace, graph);
        Map<Id, JsonVertex> map = new HashMap<>(req.jsonVertices.size());

        return this.commit(config, g, map.size(), () -> {
            /*
             * 1.Put all newVertices' properties into map (combine first)
             * - Consider primary-key & user-define ID mode first
             */
            req.jsonVertices.forEach(newVertex -> {
                Id newVertexId = getVertexId(g, newVertex);
                JsonVertex oldVertex = map.get(newVertexId);
                this.updateExistElement(oldVertex, newVertex,
                                        req.updateStrategies);
                map.put(newVertexId, newVertex);
            });

            // 2.Get all oldVertices and update with new vertices
            Object[] ids = map.keySet().toArray();
            Iterator<Vertex> oldVertices = g.vertices(ids);
            try {
                oldVertices.forEachRemaining(oldVertex -> {
                    JsonVertex newVertex = map.get(oldVertex.id());
                    this.updateExistElement(g, oldVertex, newVertex,
                                            req.updateStrategies);
                });
            } finally {
                CloseableIterator.closeIterator(oldVertices);
            }

            // 3.Add finalVertices and return them
            List<Vertex> vertices = new ArrayList<>(map.size());
            map.values().forEach(finalVertex -> {
                vertices.add(g.addVertex(finalVertex.properties()));
            });

            // If return ids, the ids.size() maybe different with the origins'
            return manager.serializer()
                          .writeVertices(vertices.iterator(), false);
        });
    }

    @PUT
    @Timed(name = "single-update")
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @PathParam("id") String idValue,
                         @QueryParam("action") String action,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] update vertex: {}", graph, jsonVertex);
        checkUpdatingBody(jsonVertex);

        Id id = checkAndParseVertexId(idValue);
        // Parse action param
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graphSpace, graph);
        HugeVertex vertex = (HugeVertex) g.vertex(id);
        VertexLabel vertexLabel = vertex.schemaLabel();

        for (String key : jsonVertex.properties.keySet()) {
            PropertyKey pkey = g.propertyKey(key);
            E.checkArgument(vertexLabel.properties().contains(pkey.id()),
                            "Can't update property for vertex '%s' because " +
                            "there is no property key '%s' in its vertex label",
                            id, key);
        }

        commit(g, () -> updateProperties(vertex, jsonVertex, append));

        return manager.serializer().writeVertex(vertex);
    }

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$graphspace=$graphspace $owner=$graph " +
                            "$action=vertex_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("label") String label,
                       @QueryParam("properties") String properties,
                       @QueryParam("keep_start_p")
                       @DefaultValue("false") boolean keepStartP,
                       @QueryParam("offset") @DefaultValue("0") long offset,
                       @QueryParam("page") String page,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] query vertices by label: {}, properties: {}, " +
                  "offset: {}, page: {}, limit: {}",
                  graph, label, properties, offset, page, limit);

        Map<String, Object> props = parseProperties(properties);
        if (page != null) {
            E.checkArgument(offset == 0,
                            "Not support querying vertices based on paging " +
                            "and offset together");
        }

        HugeGraph g = graph(manager, graphSpace, graph);

        GraphTraversal<Vertex, Vertex> traversal = g.traversal().V();
        if (label != null) {
            traversal = traversal.hasLabel(label);
        }

        // Convert relational operator like P.gt()/P.lt()
        for (Map.Entry<String, Object> prop : props.entrySet()) {
            Object value = prop.getValue();
            if (!keepStartP && value instanceof String &&
                ((String) value).startsWith(TraversalUtil.P_CALL)) {
                prop.setValue(TraversalUtil.parsePredicate((String) value));
            }
        }

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            traversal = traversal.has(entry.getKey(), entry.getValue());
        }

        if (page == null) {
            traversal = traversal.range(offset, offset + limit);
        } else {
            traversal = traversal.has(QueryHolder.SYSPROP_PAGE, page)
                                 .limit(limit);
        }

        try {
            return manager.serializer().writeVertices(traversal,
                                                      page != null);
        } finally {
            if (g.tx().isOpen()) {
                g.tx().close();
            }
        }
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @PathParam("id") String idValue) {
        LOG.debug("Graph [{}] get vertex by id '{}'", graph, idValue);

        Id id = checkAndParseVertexId(idValue);
        HugeGraph g = graph(manager, graphSpace, graph);
        try {
            Vertex vertex = g.vertex(id);
            return manager.serializer().writeVertex(vertex);
        } finally {
            if (g.tx().isOpen()) {
                g.tx().close();
            }
        }
    }

    /**
     * 计算personal rank的值 并且 回写到起点
     *
     * @param manager
     * @param graphSpace
     * @param graph
     * @param requestVertexParams
     * @return
     */
    @POST
    @Timed(name = "single-update")
    @Path("reply_write")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String updateForReplyWrite(@Context GraphManager manager,
                                      @PathParam("graphspace")
                                      String graphSpace,
                                      @PathParam("graph") String graph,
                                      RequestVertexParams requestVertexParams) {
        LOG.debug("Graph [{}] update vertex for reply write: {}", graph,
                  requestVertexParams);
        if (!requestVertexParams.useLabel) {
            requestVertexParams.labels = Lists.newArrayList();
        }
        if (Objects.isNull(requestVertexParams.baseProperty)) {
            requestVertexParams.baseProperty = false;
        }
        if (CollectionUtils.isEmpty(requestVertexParams.filterProperties)) {
            requestVertexParams.filterProperties.add("rank");
        }

        this.validParams(requestVertexParams);

        HashMap<String, Object> returnResult = new HashMap<>();
        returnResult.put("result", "success");

        String personalKey = String.format(PERSON_RANK_CAL_KEY,
                                           graphSpace, graph,
                                           requestVertexParams.source);
        PdMetaDriver pdMetaDriver = (PdMetaDriver) manager.meta().metaDriver();
        if (StringUtils.isNotEmpty(pdMetaDriver.get(personalKey))) {
            returnResult.put("message",
                             String.format("%s calculating personal rank"
                                     , requestVertexParams.source));
            return manager.serializer().writeMap(returnResult);
        }

        long starTime = System.currentTimeMillis();
        try {
            pdMetaDriver.putTTL(personalKey, "1", requestVertexParams.pdIntervalTime);

            Id id = HugeVertex.getIdValue(requestVertexParams.source);

            HugeGraph g = graph(manager, graphSpace, graph);
            HugeVertex vertex = (HugeVertex) g.vertex(id);
            VertexLabel vertexLabel = vertex.schemaLabel();
            Object aFilterValue = vertex.value(requestVertexParams.aFilterValueByName);

            PropertyKey pkey = g.propertyKey(requestVertexParams.replyField);
            E.checkArgument(vertexLabel.properties().contains(pkey.id()),
                            "Can't update property for vertex '%s' because " +
                            "there is no property key '%s' in its vertex label",
                            id, requestVertexParams.replyField);

            // 计算起点的personal rank
            PersonalRankTraverser traverser;
            traverser = new PersonalRankTraverser(g, requestVertexParams.alpha,
                                                  requestVertexParams.maxDegree,
                                                  requestVertexParams.maxDepth);
            Map<Id, Double> ranks = traverser.personalRankForBatch(id,
                                                                   requestVertexParams.labels,
                                                                   requestVertexParams.withLabel,
                                                                   requestVertexParams.direction);
            ranks = HugeTraverser.sortMap(ranks, requestVertexParams.sorted);
            if (ranks.isEmpty()) {
                returnResult.put("message", "ranks result is empty");
                return manager.serializer().writeMap(returnResult);
            }

            List<Vertex> vertexList =
                    filterVertexByProperties(g, requestVertexParams.bFilterValueByName,
                                             aFilterValue,
                                             ranks.keySet().stream().collect(
                                                     Collectors.toList()),
                                             requestVertexParams.limit);
            List<Map<String, Object>> personRankResults =
                    Lists.newArrayList();
            for (Vertex vertexTmp : vertexList) {
                Map<String, Object> vertexToMap =
                        covertVertexToMap(vertexTmp,
                                          requestVertexParams.filterProperties);
                personRankResults.add(vertexToMap);
            }

            int size = requestVertexParams.limit >= personRankResults.size() ?
                       personRankResults.size() : requestVertexParams.limit;
            personRankResults = personRankResults.subList(0, size);
            // 计算结果写入点属性
            if (requestVertexParams.writeVertexFlag) {
                String toJson = JsonUtil.toJson(personRankResults);
                JsonVertex jsonVertex = new JsonVertex();
                jsonVertex.label = vertexLabel.name();
                HashMap<String, Object> properties = new HashMap<>();
                properties.put(requestVertexParams.replyField, toJson);
                jsonVertex.properties = properties;
                commit(g, () -> updateProperties(vertex, jsonVertex, true));
            }

            DebugMeasure measure = new DebugMeasure();
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
            return manager.serializer(g, measure.getResult())
                          .writeList("personal_rank", personRankResults);
        } catch (Exception e) {
            pdMetaDriver.delete(personalKey);
            returnResult.put("result", "failed");
            returnResult.put("message", e.getMessage());

            LOG.error("Error updateForReplyWrite PersonalRankTraverser {}", e);

            return manager.serializer().writeMap(returnResult);
        } finally {
            LOG.info("Graph [{}] source:{} update vertex for reply write cost" +
                     " {}" +
                     " ms", graph, requestVertexParams.source,
                     System.currentTimeMillis() - starTime);
        }
    }

    /**
     * 根据属性值过滤符合条件的点
     *
     * @param graph
     * @param vertexIdList
     * @return
     */
    private List<Vertex> filterVertexByProperties(
            final HugeGraph graph, String propertyKey, Object propertyValue,
            List<Id> vertexIdList, int limit) {
        List<Vertex> personRankResults = Lists.newArrayList();

        List<List<Id>> queryIdArrayList = Lists.newArrayList();
        int perBatchSize = limit * 2;
        if (vertexIdList.size() >= perBatchSize) {
            queryIdArrayList = Lists.partition(vertexIdList, perBatchSize);
        } else {
            queryIdArrayList.add(vertexIdList);
        }
        for (List<Id> queryIdList : queryIdArrayList) {
            Iterator<Vertex> vertices =
                    graph.vertices(queryIdList.toArray());
            try {
                while (vertices.hasNext()) {
                    Vertex vertex = vertices.next();
                    Object value = vertex.value(propertyKey);
                    if (!compareEqualData(propertyValue, value)) {
                        continue;
                    }
                    personRankResults.add(vertex);
                    if (personRankResults.size() >= limit) {
                        break;
                    }
                }
            } finally {
                CloseableIterator.closeIterator(vertices);
            }
            if (personRankResults.size() >= limit) {
                break;
            }
        }
        return personRankResults;
    }

    public boolean compareEqualData(Object value1, Object value2) {
        if (Objects.isNull(value1) || Objects.isNull(value2)) {
            return false;
        }
        if (value1 instanceof String && value2 instanceof String) {
            return value1.toString().compareTo(value2.toString()) == 0;
        }
        if (value1 instanceof Integer && value2 instanceof Integer) {
            return ((Integer) value1).compareTo((Integer) value2) == 0;
        }
        return false;
    }

    /**
     * 将点信息 根据需要的属性进行过滤转换
     *
     * @param vertex
     * @param filterProperties
     * @return
     */
    private Map<String, Object> covertVertexToMap(Vertex vertex,
                                                  List<String> filterProperties) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", vertex.id());
        map.put("label", vertex.label());
        map.put("type", "vertex");

        Iterator<VertexProperty<Object>> properties = vertex.properties();
        Map<String, Object> propertiesMap = new HashMap<>();
        while (properties.hasNext()) {
            VertexProperty<Object> vertexProperty = properties.next();
            String key = vertexProperty.key();
            Object value = vertexProperty.value();
            if (filterProperties.contains(key)) {
                continue;
            }
            propertiesMap.put(key, value);
        }
        map.put("properties", propertiesMap);
        return map;
    }

    /**
     * 根据属性过滤
     *
     * @param vertexList
     * @param replyProperties
     * @param baseProperty
     * @return
     */
    private List<Map<String, Object>> getPropertyValueByPropertyName(
            List<Vertex> vertexList, List<String> replyProperties,
            boolean baseProperty, int limit) {
        List<Map<String, Object>> result = Lists.newArrayList();
        if (CollectionUtils.isEmpty(vertexList)) {
            return result;
        }
        for (Vertex vertexTmp : vertexList) {
            HashMap<String, Object> propertyMap = new HashMap<>();
            for (String property : replyProperties) {
                Object value = vertexTmp.value(property);
                propertyMap.put(property, value);
            }
            if (baseProperty) {
                HashMap<String, Object> map = new HashMap<>();
                map.put("id", vertexTmp.id());
                map.put("label", vertexTmp.label());
                map.put("type", "vertex");
                map.put("properties", propertyMap);
                result.add(map);
            } else {
                result.add(propertyMap);
            }
        }
        Integer size = result.size() >= limit ? limit : result.size();
        return result.subList(0, size);
    }

    /**
     * 根据id获取点信息
     *
     * @param idArray
     * @param g
     * @return
     */
    private List<Vertex> getVertexInfoByIdArray(
            Object[] idArray, HugeGraph g) {
        List<Vertex> result = Lists.newArrayList();
        if (Objects.isNull(idArray) || idArray.length == 0) {
            return result;
        }
        Iterator<Vertex> vertices = g.vertices(idArray);
        try {
            while (vertices.hasNext()) {
                Vertex vertexTmp = vertices.next();
                result.add(vertexTmp);
            }
        } finally {
            CloseableIterator.closeIterator(vertices);
        }
        return result;
    }

    /**
     * post 方式 根据vertex id 获取 vertex信息
     *
     * @param manager
     * @param graphSpace
     * @param graph
     * @param requestVertexParams
     * @return
     */
    @POST
    @Path("get_vertext_by_id")
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String getVertexById(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace,
                                @PathParam("graph") String graph,
                                RequestVertexParams requestVertexParams) {
        LOG.debug("Graph [{}] post vertex by id '{}'", graph,
                  requestVertexParams.id);

        E.checkArgumentNotNull(requestVertexParams,
                               "getVertexById requestVertexParams can't be " +
                               "null");
        E.checkArgumentNotNull(requestVertexParams.id,
                               "getVertexById requestVertexParams " +
                               "id can't be null");
        Id id = HugeVertex.getIdValue(requestVertexParams.id);
        HugeGraph g = graph(manager, graphSpace, graph);
        try {
            Vertex vertex = g.vertex(id);
            return manager.serializer().writeVertex(vertex);
        } finally {
            if (g.tx().isOpen()) {
                g.tx().close();
            }
        }
    }

    /**
     * 从指定机器目录读取热门商品信息
     *
     * @param manager
     * @param graphSpace
     * @param graph
     * @param requestVertexParams
     * @return
     * @throws IOException
     */
    @POST
    @Path("gethotgoods")
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String getHotGoods(@Context GraphManager manager,
                              @PathParam("graphspace") String graphSpace,
                              @PathParam("graph") String graph,
                              RequestVertexParams requestVertexParams) throws
                                                                       IOException {
        LOG.debug("Graph [{}] post get hot goods by requestVertexParams '{}'", graph,
                  requestVertexParams);
        if (Objects.isNull(requestVertexParams.baseProperty)) {
            requestVertexParams.baseProperty = true;
        }
        E.checkState(requestVertexParams.limit > 0, "hot goods path not null");
        E.checkState(requestVertexParams.replyField != null, "hot goods reply_field not null");
        String hotGoodsKey = String.format("%s-%s-%s-hot", graphSpace, graph,
                                           requestVertexParams.replyField);
        // 内存获取数据
        if (requestVertexParams.directReadHotGoods &&
            (Objects.nonNull(beforeGetHotGoodsDate.get(hotGoodsKey)) &&
             System.currentTimeMillis() - beforeGetHotGoodsDate.get(hotGoodsKey) <
             requestVertexParams.readFileIntervalTime) &&
            beforeGetHotGoodsVertices.containsKey(hotGoodsKey)) {
            List<Map<String, Object>> vertices =
                    beforeGetHotGoodsVertices.get(hotGoodsKey);
            return manager.serializer().writeList("vertices", vertices);
        }

        // 获取查找路径
        if (StringUtils.isEmpty(requestVertexParams.hotGoodsPath)) {
            String path = configProvider.get()
                                        .get(ServerOptions.PERSONAL_RANK_FILE_PATH);
            String suffix = configProvider.get()
                                          .get(ServerOptions.PERSONAL_RANK_FILE_SUFFIX);
            requestVertexParams.hotGoodsPath = String.format("%s/%s-%s.%s",
                                                             path, graphSpace, graph, suffix);
        }

        E.checkNotNull(requestVertexParams.hotGoodsPath, "hot goods path not null");

        // 物理文件读取路径  根据图空间-图名称 读取数据
        List<Map<String, Object>> hugeVertices = Lists.newArrayList();
        try {
            List<String> hotGoodsStr = Lists.newArrayList();
            if (requestVertexParams.readPd) {
                // 从pd读取数据 默认
                PdMetaDriver pdMetaDriver = (PdMetaDriver) manager.meta().metaDriver();
                hotGoodsStr = readHotGoodsFromPd(requestVertexParams,
                                                 pdMetaDriver, hotGoodsKey);
            } else {
                // 从文件读取数据
                hotGoodsStr = readHotGoodsFromFile(requestVertexParams);
            }

            String[] hotGoodsStrArray = hotGoodsStr.toArray(new String[0]);
            HugeGraph g = graph(manager, graphSpace, graph);
            List<Vertex> vertexInfoByIdArray =
                    getVertexInfoByIdArray(hotGoodsStrArray, g);

            hugeVertices = getPropertyValueByPropertyName(vertexInfoByIdArray,
                                                          requestVertexParams.replyProperties,
                                                          requestVertexParams.baseProperty,
                                                          requestVertexParams.limit);
            setBeforeGetHotGoods(hotGoodsKey, hugeVertices);
        } catch (Exception e) {
            throw e;
        }


        return manager.serializer()
                      .writeList("vertices", hugeVertices);
    }

    /**
     * 从pd读取热点商品ID
     *
     * @param requestVertexParams
     * @param pdMetaDriver
     * @param pdKey
     * @return
     */
    private List<String> readHotGoodsFromPd(RequestVertexParams requestVertexParams,
                                            PdMetaDriver pdMetaDriver, String pdKey) {
        if (StringUtils.isEmpty(requestVertexParams.pdKey)) {
            requestVertexParams.pdKey = pdKey;
        }
        List<String> hotGoodsStr = Lists.newArrayList();
        String hotGoodsData = pdMetaDriver.get(requestVertexParams.pdKey);
        if (StringUtils.isEmpty(hotGoodsData)) {
            return hotGoodsStr;
        }
        hotGoodsStr = Arrays.stream(hotGoodsData.split(",")).collect(
                Collectors.toList());
        int limitSize = (int) (requestVertexParams.limit * 1.2);

        Integer size = hotGoodsStr.size() >= limitSize ? limitSize :
                       hotGoodsStr.size();

        hotGoodsStr = hotGoodsStr.subList(0, size);
        return hotGoodsStr;
    }

    /**
     * 从文件读取热点商品ID
     *
     * @param requestVertexParams
     * @return
     * @throws IOException
     */
    private List<String> readHotGoodsFromFile(RequestVertexParams requestVertexParams) throws
                                                                                       IOException {
        try {
            int limitSize = (int) (requestVertexParams.limit * 1.2);
            List<String> hotGoodsStr = FileUtils.readLines(
                    new File(requestVertexParams.hotGoodsPath));
            Integer size = hotGoodsStr.size() >= limitSize ? limitSize :
                           hotGoodsStr.size();
            hotGoodsStr = hotGoodsStr.subList(0, size);
            return hotGoodsStr;
        } catch (Exception e) {
            LOG.error("read hot goods file error {}", e.getMessage());
            throw e;
        }
    }

    private void setBeforeGetHotGoods(String hotGoodsKey,
                                      List<Map<String, Object>> hugeVertices) {
        synchronized (this) {
            beforeGetHotGoodsDate.put(hotGoodsKey, System.currentTimeMillis());
            beforeGetHotGoodsVertices.put(hotGoodsKey, hugeVertices);
        }
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @PathParam("id") String idValue,
                       @QueryParam("label") String label) {
        LOG.debug("Graph [{}] remove vertex by id '{}'", graph, idValue);

        Id id = checkAndParseVertexId(idValue);
        HugeGraph g = graph(manager, graphSpace, graph);
        commit(g, () -> {
            try {
                g.removeVertex(label, id);
            } catch (NotFoundException e) {
                throw new IllegalArgumentException(String.format(
                        "No such vertex with id: '%s', %s", id, e));
            } catch (NoSuchElementException e) {
                throw new IllegalArgumentException(String.format(
                        "No such vertex with id: '%s'", id));
            }
        });
    }

    public void validParams(RequestVertexParams request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgumentNotNull(request.replyProperties, "The " +
                                                        "params " +
                                                        "replyProperties can't be " +
                                                        "null");
        E.checkArgument(request.source != null,
                        "The source vertex id of rank request can't be null");
        E.checkArgument(request.labels != null,
                        "The edge label of rank request can't be null");
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.maxDegree > 0L || request.maxDegree == NO_LIMIT,
                        "The max degree of rank request must be > 0 " +
                        "or == -1, but got: %s", request.maxDegree);
        E.checkArgument(request.limit > 0L || request.limit == NO_LIMIT,
                        "The limit of rank request must be > 0 or == -1, " +
                        "but got: %s", request.limit);
        E.checkArgument(request.maxDepth > 1L &&
                        request.maxDepth <= Long.parseLong(DEFAULT_MAX_DEPTH),
                        "The max depth of rank request must be " +
                        "in range (1, %s], but got '%s'",
                        DEFAULT_MAX_DEPTH, request.maxDepth);
        E.checkArgument(request.aFilterValueByName != null,
                        "The compare a_filter_value_by_name field of rank request can't be null");
        E.checkArgument(request.bFilterValueByName != null,
                        "The compare b_filter_value_by_name field of rank request can't be null");
    }

    private static class RequestVertexParams {
        // 点ID
        @JsonProperty("id")
        public Object id;
        // 点ID
        @JsonProperty("source")
        public Object source;
        // 边的label
        @JsonProperty("labels")
        public List<String> labels;
        // 是否使用label进行过滤
        @JsonProperty("useLabel")
        public Boolean useLabel = false;
        // person rank数据回写的字段
        @JsonProperty("reply_field")
        public String replyField;

        @JsonProperty("reply_properties")
        public List<String> replyProperties = Lists.newArrayList();

        @JsonProperty("filter_properties")
        public List<String> filterProperties = Lists.newArrayList();

        @JsonProperty("a_filter_value_by_name")
        public String aFilterValueByName;
        @JsonProperty("b_filter_value_by_name")
        public String bFilterValueByName;

        @JsonProperty("alpha")
        public float alpha = 0.85F;
        // 最大的度数
        @JsonProperty("max_depth")
        public Integer maxDepth = 3;
        // 是否排序
        @JsonProperty("sorted")
        public Boolean sorted = true;
        @JsonProperty("limit")
        public Integer limit = 10;
        // 热点商品数据路径
        @JsonProperty("hotGoodsPath")
        public String hotGoodsPath;
        // 是否直接从文件读取数据(false-在内存读取 true-在文件读取然后放入内存)
        @JsonProperty("directReadHotGoods")
        public Boolean directReadHotGoods = true;
        // 间隔时间 读取文件(距离上一次读取的时间) 默认10m
        @JsonProperty("readFileIntervalTime")
        public Long readFileIntervalTime = 600000L;
        // pd间隔时间(同一个id在pd存留的时间, 即同一个ID的计算 间隔多长时间不再进行重复计算) 默认10m
        @JsonProperty("pdIntervalTime")
        public Long pdIntervalTime = 600000L;
        // 是否返回基本属性
        @JsonProperty("baseProperty")
        public Boolean baseProperty;
        @JsonProperty("direction")
        public String direction;
        @JsonProperty("pd_key")
        public String pdKey;
        @JsonProperty("read_pd")
        public Boolean readPd = true;
        // 是否将结果写入点属性
        @JsonProperty("write_vertex_flag")
        public Boolean writeVertexFlag = true;
        // 每层迭代 最大的度数
        @JsonProperty("max_degree")
        private long maxDegree = 50L;
        @JsonProperty("with_label")
        private PersonalRankTraverser.WithLabel withLabel =
                PersonalRankTraverser.WithLabel.OTHER_LABEL;

        @Override
        public String toString() {
            return "RequestVertexParams{" +
                   "id=" + id +
                   ", source=" + source +
                   ", labels=" + labels +
                   ", useLabel=" + useLabel +
                   ", replyField='" + replyField + '\'' +
                   ", replyProperties=" + replyProperties +
                   ", alpha=" + alpha +
                   ", maxDepth=" + maxDepth +
                   ", maxDegree=" + maxDegree +
                   ", sorted=" + sorted +
                   ", limit=" + limit +
                   ", withLabel=" + withLabel +
                   ", hotGoodsPath='" + hotGoodsPath + '\'' +
                   ", directReadHotGoods=" + directReadHotGoods +
                   ", readFileIntervalTime=" + readFileIntervalTime +
                   ", pdIntervalTime=" + pdIntervalTime +
                   ", baseProperty=" + baseProperty +
                   ", direction='" + direction + '\'' +
                   '}';
        }
    }

    private static class BatchVertexRequest {

        @JsonProperty("vertices")
        public List<JsonVertex> jsonVertices;
        @JsonProperty("update_strategies")
        public Map<String, UpdateStrategy> updateStrategies;
        @JsonProperty("create_if_not_exist")
        public boolean createIfNotExist = true;

        private static void checkUpdate(BatchVertexRequest req) {
            E.checkArgumentNotNull(req, "BatchVertexRequest can't be null");
            E.checkArgumentNotNull(req.jsonVertices,
                                   "Parameter 'vertices' can't be null");
            E.checkArgument(req.updateStrategies != null &&
                            !req.updateStrategies.isEmpty(),
                            "Parameter 'update_strategies' can't be empty");
            E.checkArgument(req.createIfNotExist,
                            "Parameter 'create_if_not_exist' " +
                            "dose not support false now");
        }

        @Override
        public String toString() {
            return String.format("BatchVertexRequest{jsonVertices=%s," +
                                 "updateStrategies=%s,createIfNotExist=%s}",
                                 this.jsonVertices, this.updateStrategies,
                                 this.createIfNotExist);
        }
    }

    private static class JsonVertex extends JsonElement {

        @Override
        public void checkCreate(boolean isBatch) {
            this.checkUpdate();
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.properties,
                                   "The properties of vertex can't be null");

            for (Map.Entry<String, Object> e : this.properties.entrySet()) {
                String key = e.getKey();
                Object value = e.getValue();
                E.checkArgumentNotNull(value, "Not allowed to set value of " +
                                              "property '%s' to null for vertex '%s'",
                                       key, this.id);
            }
        }

        @Override
        public Object[] properties() {
            Object[] props = API.properties(this.properties);
            List<Object> list = new ArrayList<>(Arrays.asList(props));
            if (this.label != null) {
                list.add(T.label);
                list.add(this.label);
            }
            if (this.id != null) {
                list.add(T.id);
                list.add(this.id);
            }
            return list.toArray();
        }

        @Override
        public String toString() {
            return String.format("JsonVertex{label=%s, properties=%s}",
                                 this.label, this.properties);
        }
    }
}
