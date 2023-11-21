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

package org.apache.hugegraph.traversal.optimize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.Condition.Relation;
import org.apache.hugegraph.backend.query.Condition.RelationType;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.exception.ErrorCode;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableList;

public final class TraversalUtil {

    // TODO: shall we need 'P.', better to remove it for user friendly
    public static final String P_CALL = "P.";

    public static HugeGraph getGraph(Step<?, ?> step) {
        return (HugeGraph) step.getTraversal().getGraph().get();
    }

    public static void extractHasContainer(HugeGraphStep<?, ?> newStep,
                                           Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof HasStep) {
                HasContainerHolder holder = (HasContainerHolder) step;
                for (HasContainer has : holder.getHasContainers()) {
                    if (!GraphStep.processHasContainerIds(newStep, has)) {
                        newStep.addHasContainer(has);
                    }
                }
                TraversalHelper.copyLabels(step, step.getPreviousStep(), false);
                traversal.removeStep(step);
            }
        } while (step instanceof HasStep || step instanceof NoOpBarrierStep);
    }

    public static void extractHasContainer(HugeVertexStep<?> newStep,
                                           Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            if (step instanceof HasStep) {
                HasContainerHolder holder = (HasContainerHolder) step;
                for (HasContainer has : holder.getHasContainers()) {
                    newStep.addHasContainer(has);
                }
                TraversalHelper.copyLabels(step, step.getPreviousStep(), false);
                traversal.removeStep(step);
            }
            step = step.getNextStep();
        } while (step instanceof HasStep || step instanceof NoOpBarrierStep);
    }

    public static void extractOrder(Step<?, ?> newStep,
                                    Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof OrderGlobalStep) {
                QueryHolder holder = (QueryHolder) newStep;
                @SuppressWarnings("resource")
                OrderGlobalStep<?, ?> orderStep = (OrderGlobalStep<?, ?>) step;
                orderStep.getComparators().forEach(comp -> {
                    ElementValueComparator<?> comparator =
                            (ElementValueComparator<?>) comp.getValue1();
                    holder.orderBy(comparator.getPropertyKey(),
                                   (Order) comparator.getValueComparator());
                });
                TraversalHelper.copyLabels(step, newStep, false);
                traversal.removeStep(step);
            }
            step = step.getNextStep();
        } while (step instanceof OrderGlobalStep ||
                 step instanceof IdentityStep);
    }

    public static void extractRange(Step<?, ?> newStep,
                                    Traversal.Admin<?, ?> traversal,
                                    boolean extractOnlyLimit) {
        QueryHolder holder = (QueryHolder) newStep;
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof RangeGlobalStep) {
                @SuppressWarnings("unchecked")
                RangeGlobalStep<Object> range = (RangeGlobalStep<Object>) step;
                /*
                 * NOTE: keep the step to limit results after query from DB
                 * due to `limit`(in DB) may not be implemented accurately.
                 * but the backend driver should ensure `offset` accurately.
                 */
                // TraversalHelper.copyLabels(step, newStep, false);
                // traversal.removeStep(step);
                if (extractOnlyLimit) {
                    // May need to retain offset for multiple sub-queries
                    holder.setRange(0, range.getHighRange());
                } else {
                    long limit = holder.setRange(range.getLowRange(),
                                                 range.getHighRange());
                    RangeGlobalStep<Object> newRange = new RangeGlobalStep<>(
                            traversal, 0, limit);
                    TraversalHelper.replaceStep(range, newRange, traversal);
                }
            }
        } while (step instanceof RangeGlobalStep ||
                 step instanceof IdentityStep ||
                 step instanceof NoOpBarrierStep);
    }

    public static void extractCount(Step<?, ?> newStep,
                                    Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof CountGlobalStep) {
                QueryHolder holder = (QueryHolder) newStep;
                holder.setCount();
            }
        } while (step instanceof CountGlobalStep ||
                 step instanceof FilterStep ||
                 step instanceof IdentityStep ||
                 step instanceof NoOpBarrierStep);
    }

    public static void extractAggregateFunc(Step<?, ?> newStep,
                                            Traversal.Admin<?, ?> traversal) {
        PropertiesStep<?> propertiesStep = null;
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof PropertiesStep) {
                @SuppressWarnings("resource")
                PropertiesStep<?> propStep = (PropertiesStep<?>) step;
                if (propStep.getReturnType() == PropertyType.VALUE &&
                    propStep.getPropertyKeys().length == 1) {
                    propertiesStep = propStep;
                }
            } else if (propertiesStep != null &&
                       step instanceof ReducingBarrierStep) {
                AggregateFunc aggregateFunc;
                if (step instanceof CountGlobalStep) {
                    aggregateFunc = AggregateFunc.COUNT;
                } else if (step instanceof MaxGlobalStep) {
                    aggregateFunc = AggregateFunc.MAX;
                } else if (step instanceof MinGlobalStep) {
                    aggregateFunc = AggregateFunc.MIN;
                } else if (step instanceof MeanGlobalStep) {
                    aggregateFunc = AggregateFunc.AVG;
                } else if (step instanceof SumGlobalStep) {
                    aggregateFunc = AggregateFunc.SUM;
                } else {
                    aggregateFunc = null;
                }

                if (aggregateFunc != null) {
                    QueryHolder holder = (QueryHolder) newStep;
                    holder.setAggregate(aggregateFunc,
                                        propertiesStep.getPropertyKeys()[0]);
                    traversal.removeStep(step);
                    traversal.removeStep(propertiesStep);
                }
            }
        } while (step instanceof FilterStep ||
                 step instanceof PropertiesStep ||
                 step instanceof IdentityStep ||
                 step instanceof NoOpBarrierStep);
    }

    public static ConditionQuery fillConditionQuery(
            ConditionQuery query,
            List<HasContainer> hasContainers,
            HugeGraph graph) {
        HugeType resultType = query.resultType();

        for (HasContainer has : hasContainers) {
            Condition condition = convHas2Condition(has, resultType, graph);
            query.query(condition);
        }
        return query;
    }

    public static void fillConditionQuery(ConditionQuery query,
                                          Map<Id, Object> properties,
                                          HugeGraph graph) {
        for (Map.Entry<Id, Object> entry : properties.entrySet()) {
            Id key = entry.getKey();
            Object value = entry.getValue();
            PropertyKey pk = graph.propertyKey(key);
            if (value instanceof String &&
                ((String) value).startsWith(TraversalUtil.P_CALL)) {
                String predicate = (String) value;
                query.query(TraversalUtil.parsePredicate(pk, predicate));
            } else if (value instanceof Collection) {
                List<Object> validValues = new ArrayList<>();
                for (Object v : (Collection<?>) value) {
                    validValues.add(TraversalUtil.validPropertyValue(v, pk));
                }
                query.query(Condition.in(key, validValues));
            } else {
                Object validValue = TraversalUtil.validPropertyValue(value, pk);
                query.query(Condition.eq(key, validValue));
            }
        }
    }

    public static Condition conditionEdgeStep(Map<Id, Object> properties,
                                              HugeGraph graph) {
        Condition unionCondition = null;
        for (Map.Entry<Id, Object> entry : properties.entrySet()) {
            Id key = entry.getKey();
            Object value = entry.getValue();
            PropertyKey pk = graph.propertyKey(key);
            Condition condition = null;
            if (value instanceof String &&
                ((String) value).startsWith(TraversalUtil.P_CALL)) {
                String predicate = (String) value;
                condition = TraversalUtil.parsePredicate(pk, predicate);
            } else if (value instanceof Collection) {
                List<Object> validValues = new ArrayList<>();
                for (Object v : (Collection<?>) value) {
                    validValues.add(TraversalUtil.validPropertyValue(v, pk));
                }
                condition = Condition.in(key, validValues);
            } else {
                Object validValue = TraversalUtil.validPropertyValue(value, pk);
                condition = Condition.eq(key, validValue);
            }

            if (unionCondition == null) {
                unionCondition = condition;
            } else {
                unionCondition = unionCondition.or(condition);
            }
        }
        return unionCondition;
    }

    public static Condition fillIdPropCondition(Condition condition,
                                                Map<Id, Object> properties,
                                                HugeGraph graph) {
        for (Map.Entry<Id, Object> entry : properties.entrySet()) {
            Id key = entry.getKey();
            Object value = entry.getValue();
            PropertyKey pk = graph.propertyKey(key);
            if (value instanceof String &&
                ((String) value).startsWith(TraversalUtil.P_CALL)) {
                String predicate = (String) value;

                condition = TraversalUtil.parsePredicate(pk, predicate)
                                         .and(condition);
            } else if (value instanceof Collection) {
                List<Object> validValues = new ArrayList<>();
                for (Object v : (Collection<?>) value) {
                    validValues.add(TraversalUtil.validPropertyValue(v, pk));
                }
                condition = Condition.in(key, validValues).and(condition);
            } else {
                Object validValue = TraversalUtil.validPropertyValue(value, pk);
                condition = Condition.eq(key, validValue).and(condition);
            }
        }
        return condition;
    }

    public static ConditionQuery queryWithPermission(
            ConditionQuery query, List<Map<String, Object>> permissions,
            HugeGraph graph) {
        Condition condition = conditionWithPermission(permissions,
                                                      query.resultType(),
                                                      graph);
        return (condition == null) ? query : query.query(condition);
    }

    public static Condition conditionWithPermission(
            List<Map<String, Object>> permissions,
            HugeType type, HugeGraph graph) {
        /*
         * Generate condition based on permission of label and properties
         * */
        Condition permCondition = null;
        for (Map<String, Object> permission : permissions) {
            Condition ressCondition;
            if (permission.get("label") == null) {
                ressCondition = null;
            } else {
                String permLabel = permission.get("label").toString();
                if ("*".equals(permLabel)) {
                    ressCondition = null;
                } else {
                    Id label;
                    if (type == HugeType.VERTEX) {
                        label = graph.vertexLabel(permLabel).id();
                    } else if (type == HugeType.EDGE) {
                        label = graph.edgeLabel(permLabel).id();
                    } else {
                        throw new AssertionError(
                                String.format("Invalid query type: '%s', " +
                                              "valid type is " +
                                              "'V' or 'E'", type));
                    }

                    ressCondition = Condition.eq(HugeKeys.LABEL, label);
                }
            }

            Map<String, Object> properties = (Map) permission.get("properties");
            if (properties != null && !properties.isEmpty()) {
                ressCondition = TraversalUtil.fillPropCondition(ressCondition,
                                                                properties, graph);
            }
            if (permCondition == null) {
                permCondition = ressCondition;
            } else if (ressCondition != null) {
                permCondition = permCondition.or(ressCondition);
            }
        }

        return permCondition;
    }

    public static Condition fillPropCondition(Condition condition,
                                              Map<String, Object> properties,
                                              HugeGraph graph) {
        // TODO condition == null
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            Object value = entry.getValue();
            PropertyKey pk = graph.propertyKey(entry.getKey());
            if (value instanceof String &&
                ((String) value).startsWith(TraversalUtil.P_CALL)) {
                String predicate = (String) value;

                condition = TraversalUtil.parsePredicate(pk, predicate)
                                         .and(condition);
            } else if (value instanceof Collection) {
                List<Object> validValues = new ArrayList<>();
                for (Object v : (Collection<?>) value) {
                    validValues.add(TraversalUtil.validPropertyValue(v, pk));
                }
                condition = Condition.in(pk.id(), validValues).and(condition);
            } else {
                Object validValue = TraversalUtil.validPropertyValue(value, pk);
                condition = Condition.eq(pk.id(), validValue).and(condition);
            }
        }
        return condition;
    }

    public static Condition convHas2Condition(HasContainer has,
                                              HugeType type,
                                              HugeGraph graph) {
        P<?> p = has.getPredicate();
        E.checkArgument(p != null, "The predicate of has(%s) is null", has);
        BiPredicate<?, ?> bp = p.getBiPredicate();
        Condition condition;
        if (keyForContainsKeyOrValue(has.getKey())) {
            condition = convContains2Relation(graph, has);
        } else if (p instanceof TextP) {
            condition = convTextP2Relation(graph, type, has);
        } else if (bp instanceof Compare) {
            condition = convCompare2Relation(graph, type, has);
        } else if (bp instanceof RelationType) {
            condition = convRelationType2Relation(graph, type, has);
        } else if (bp instanceof Contains) {
            condition = convIn2Relation(graph, type, has);
        } else if (p instanceof AndP) {
            condition = convAnd(graph, type, has);
        } else if (p instanceof OrP) {
            condition = convOr(graph, type, has);
        } else {
            // TODO: deal with other Predicate
            throw newUnsupportedPredicate(p);
        }
        return condition;
    }

    public static Condition convAnd(HugeGraph graph,
                                    HugeType type,
                                    HasContainer has) {
        P<?> p = has.getPredicate();
        assert p instanceof AndP;
        @SuppressWarnings("unchecked")
        List<P<Object>> predicates = ((AndP<Object>) p).getPredicates();
        if (predicates.size() < 2) {
            throw newUnsupportedPredicate(p);
        }

        Condition cond = null;
        for (P<Object> predicate : predicates) {
            HasContainer newHas = new HasContainer(has.getKey(), predicate);
            Condition newCond = convHas2Condition(newHas, type, graph);
            if (cond == null) {
                cond = newCond;
            } else {
                cond = Condition.and(newCond, cond);
            }
        }
        return cond;
    }

    public static Condition convOr(HugeGraph graph,
                                   HugeType type,
                                   HasContainer has) {
        P<?> p = has.getPredicate();
        assert p instanceof OrP;
        @SuppressWarnings("unchecked")
        List<P<Object>> predicates = ((OrP<Object>) p).getPredicates();
        if (predicates.size() < 2) {
            throw newUnsupportedPredicate(p);
        }

        Condition cond = null;
        for (P<Object> predicate : predicates) {
            HasContainer newHas = new HasContainer(has.getKey(), predicate);
            Condition newCond = convHas2Condition(newHas, type, graph);
            if (cond == null) {
                cond = newCond;
            } else {
                cond = Condition.or(newCond, cond);
            }
        }
        return cond;
    }

    private static Relation convCompare2Relation(HugeGraph graph,
                                                 HugeType type,
                                                 HasContainer has) {
        assert type.isGraph();
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Compare;

        return isSysProp(has.getKey()) ?
               convCompare2SyspropRelation(graph, type, has) :
               convCompare2UserpropRelation(graph, type, has);
    }


    private static Relation convCompare2SyspropRelation(HugeGraph graph,
                                                        HugeType type,
                                                        HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Compare;

        HugeKeys key = token2HugeKey(has.getKey());
        E.checkNotNull(key, "token key");
        Object value = convSysValueIfNeeded(graph, type, key, has.getValue());

        switch ((Compare) bp) {
            case eq:
                return Condition.eq(key, value);
            case gt:
                return Condition.gt(key, value);
            case gte:
                return Condition.gte(key, value);
            case lt:
                return Condition.lt(key, value);
            case lte:
                return Condition.lte(key, value);
            case neq:
                return Condition.neq(key, value);
        }

        throw newUnsupportedPredicate(has.getPredicate());
    }

    private static Relation convCompare2UserpropRelation(HugeGraph graph,
                                                         HugeType type,
                                                         HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Compare;

        String key = has.getKey();
        PropertyKey pkey = graph.propertyKey(key);
        Id pkeyId = pkey.id();
        Object value = validPropertyValue(has.getValue(), pkey);

        switch ((Compare) bp) {
            case eq:
                return Condition.eq(pkeyId, value);
            case gt:
                return Condition.gt(pkeyId, value);
            case gte:
                return Condition.gte(pkeyId, value);
            case lt:
                return Condition.lt(pkeyId, value);
            case lte:
                return Condition.lte(pkeyId, value);
            case neq:
                return Condition.neq(pkeyId, value);
        }

        throw newUnsupportedPredicate(has.getPredicate());
    }

    private static Condition convTextP2Relation(HugeGraph graph,
                                                HugeType type,
                                                HasContainer has) {
        assert type.isGraph();
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof TextP;

        String key = has.getKey();
        PropertyKey pkey = graph.propertyKey(key);
        Id pkeyId = pkey.id();
        Object value = validPropertyValue(has.getValue(), pkey);
        RelationType rtype;
        String name = Whitebox.getInternalState(bp, "name");
        switch (name) {
            case "startingWith":
                rtype = RelationType.TEXT_PREFIX;
                break;
            case "endingWith":
                rtype = RelationType.TEXT_SUFFIX;
                break;
            case "containing":
                rtype = RelationType.TEXT_CONTAINS;
                break;
            case "notStartingWith":
                rtype = RelationType.TEXT_NOT_PREFIX;
                break;
            case "notEndingWith":
                rtype = RelationType.TEXT_NOT_SUFFIX;
                break;
            case "notContaining":
                rtype = RelationType.TEXT_NOT_CONTAINS;
                break;
            default:
                throw new AssertionError(
                        String.format("Unsupported TextP: %s", name));
        }
        return new Condition.UserpropRelation(pkeyId, rtype, value);
    }

    private static Condition convRelationType2Relation(HugeGraph graph,
                                                       HugeType type,
                                                       HasContainer has) {
        assert type.isGraph();
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof RelationType;

        String key = has.getKey();
        PropertyKey pkey = graph.propertyKey(key);
        Id pkeyId = pkey.id();
        Object value = validPropertyValue(has.getValue(), pkey);
        return new Condition.UserpropRelation(pkeyId, (RelationType) bp, value);
    }

    public static Condition convIn2Relation(HugeGraph graph,
                                            HugeType type,
                                            HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Contains;
        Collection<?> values = (Collection<?>) has.getValue();

        String originKey = has.getKey();
        if (values.size() > 1) {
            E.checkArgument(!originKey.equals(T.key) &&
                            !originKey.equals(T.value),
                            "Not support hasKey() or hasValue() with " +
                            "multiple values");
        }

        HugeKeys hugeKey = token2HugeKey(originKey);
        List<?> valueList;
        if (hugeKey != null) {
            valueList = convSysListValueIfNeeded(graph, type, hugeKey, values);
            switch ((Contains) bp) {
                case within:
                    return Condition.in(hugeKey, valueList);
                case without:
                    return Condition.nin(hugeKey, valueList);
            }
        } else {
            valueList = new ArrayList<>(values);
            String key = has.getKey();
            PropertyKey pkey = graph.propertyKey(key);

            switch ((Contains) bp) {
                case within:
                    return Condition.in(pkey.id(), valueList);
                case without:
                    return Condition.nin(pkey.id(), valueList);
            }
        }

        throw newUnsupportedPredicate(has.getPredicate());
    }

    public static Condition convContains2Relation(HugeGraph graph,
                                                  HasContainer has) {
        // Convert contains-key or contains-value
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        E.checkArgument(bp == Compare.eq, "CONTAINS query with relation " +
                                          "'%s' is not supported", bp);

        HugeKeys key = token2HugeKey(has.getKey());
        E.checkNotNull(key, "token key");
        Object value = has.getValue();

        if (keyForContainsKey(has.getKey())) {
            if (value instanceof String) {
                value = graph.propertyKey((String) value).id();
            }
            return Condition.containsKey(key, value);
        } else {
            assert keyForContainsValue(has.getKey());
            return Condition.containsValue(key, value);
        }
    }

    public static BackendException newUnsupportedPredicate(P<?> predicate) {
        return new BackendException("Unsupported predicate: '%s'", predicate);
    }

    public static HugeKeys string2HugeKey(String key) {
        HugeKeys hugeKey = token2HugeKey(key);
        return hugeKey != null ? hugeKey : HugeKeys.valueOf(key);
    }

    public static HugeKeys token2HugeKey(String key) {
        if (key.equals(T.label.getAccessor())) {
            return HugeKeys.LABEL;
        } else if (key.equals(T.id.getAccessor())) {
            return HugeKeys.ID;
        } else if (keyForContainsKeyOrValue(key)) {
            return HugeKeys.PROPERTIES;
        }
        return null;
    }

    public static boolean keyForContainsKeyOrValue(String key) {
        return key.equals(T.key.getAccessor()) ||
               key.equals(T.value.getAccessor());
    }

    public static boolean keyForContainsKey(String key) {
        return key.equals(T.key.getAccessor());
    }

    public static boolean keyForContainsValue(String key) {
        return key.equals(T.value.getAccessor());
    }

    @SuppressWarnings("unchecked")
    public static <V> Iterator<V> filterResult(
            List<HasContainer> hasContainers,
            Iterator<? extends Element> iterator) {
        Iterator<?> result = new FilterIterator<>(iterator, elem -> {
            return HasContainer.testAll(elem, hasContainers);
        });
        return (Iterator<V>) result;
    }

    public static Iterator<Edge> filterResult(Vertex vertex,
                                              Directions dir,
                                              Iterator<Edge> edges) {
        return new FilterIterator<>(edges, edge -> {
            return dir == Directions.OUT && vertex.equals(edge.outVertex()) ||
                   dir == Directions.IN && vertex.equals(edge.inVertex());
        });
    }

    public static void convAllHasSteps(Traversal.Admin<?, ?> traversal) {
        // Extract all has steps in traversal
        @SuppressWarnings("rawtypes")
        List<HasStep> steps =
                TraversalHelper.getStepsOfAssignableClassRecursively(
                        HasStep.class, traversal);
        /*
         * The graph may be null.
         * For example:
         *   g.V().hasLabel('person').union(__.<Vertex>has("birth", dates[0]))
         * Here "__.has" will create a new traversal (due to gremlin-core
         * version update to 3.5.1), but the graph is null
         */
        if (steps.isEmpty() || !traversal.getGraph().isPresent()) {
            return;
        }
        HugeGraph graph = (HugeGraph) traversal.getGraph().get();
        for (HasStep<?> step : steps) {
            TraversalUtil.convHasStep(graph, step);
        }
    }

    public static void convHasStep(HugeGraph graph, HasStep<?> step) {
        HasContainerHolder holder = step;
        for (HasContainer has : holder.getHasContainers()) {
            convPredicateValue(graph, has);
        }
    }

    private static void convPredicateValue(HugeGraph graph,
                                           HasContainer has) {
        // No need to convert if key is sysprop
        if (isSysProp(has.getKey())) {
            return;
        }
        PropertyKey pkey = graph.propertyKey(has.getKey());
        updatePredicateValue(has.getPredicate(), pkey);
    }

    private static void updatePredicateValue(P<?> predicate, PropertyKey pkey) {
        List<P<Object>> leafPredicates = new ArrayList<>();
        collectPredicates(leafPredicates, ImmutableList.of(predicate));
        for (P<Object> pred : leafPredicates) {
            Object value = validPropertyValue(pred.getValue(), pkey);
            pred.setValue(value);
        }
    }

    private static boolean isSysProp(String key) {
        if (QueryHolder.SYSPROP_PAGE.equals(key)) {
            return true;
        }
        // Return true if key is ~id, ~label, ~key and ~value
        return token2HugeKey(key) != null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void collectPredicates(List<P<Object>> results,
                                          List<P<?>> predicates) {
        for (P<?> p : predicates) {
            if (p instanceof ConnectiveP) {
                collectPredicates(results, ((ConnectiveP) p).getPredicates());
            } else {
                results.add((P<Object>) p);
            }
        }
    }

    private static Object convSysValueIfNeeded(HugeGraph graph,
                                               HugeType type,
                                               HugeKeys key,
                                               Object value) {
        if (key == HugeKeys.LABEL && !(value instanceof Id)) {
            value = SchemaLabel.getLabelId(graph, type, value);
        } else if (key == HugeKeys.ID && !(value instanceof Id)) {
            value = HugeElement.getIdValue(type, value);
        }
        return value;
    }

    private static List<?> convSysListValueIfNeeded(HugeGraph graph,
                                                    HugeType type,
                                                    HugeKeys key,
                                                    Collection<?> values) {
        List<Object> newValues = new ArrayList<>(values.size());
        for (Object value : values) {
            newValues.add(convSysValueIfNeeded(graph, type, key, value));
        }
        return newValues;
    }

    public static Query.Order convOrder(Order order) {
        return order == Order.desc ? Query.Order.DESC : Query.Order.ASC;
    }

    private static <V> V validPropertyValue(V value, PropertyKey pkey) {
        if (pkey.cardinality().single() && value instanceof Collection &&
            !pkey.dataType().isBlob()) {
            // Expect single but got collection, like P.within([])
            Collection<?> collection = (Collection<?>) value;
            Collection<Object> validValues = new ArrayList<>();
            for (Object element : collection) {
                Object validValue = pkey.validValue(element);
                if (validValue == null) {
                    validValues = null;
                    break;
                }
                validValues.add(validValue);
            }
            if (validValues == null) {
                List<Class<?>> classes = new ArrayList<>();
                for (Object v : (Collection<?>) value) {
                    classes.add(v == null ? null : v.getClass());
                }
                E.checkArgument(false,
                                ErrorCode.INVALID_PROP_VALUE.format(
                                        value, pkey.dataType(), pkey.name(),
                                        classes));
            }

            @SuppressWarnings("unchecked")
            V validValue = (V) validValues;
            return validValue;
        }

        V validValue;
        if (pkey.cardinality().multiple() && !(value instanceof Collection)) {
            // Expect non-single but got single, like P.contains(value)
            List<V> values = CollectionUtil.toList(value);
            values = pkey.validValue(values);
            validValue = values != null ? values.get(0) : null;
        } else {
            validValue = pkey.validValue(value);
        }

        if (validValue == null) {
            E.checkArgument(false,
                            ErrorCode.INVALID_PROP_VALUE.format(
                                    value, pkey.dataType(), pkey.name(),
                                    value == null ? null : value.getClass()));
        }
        return validValue;
    }

    public static void retriveSysprop(List<HasContainer> hasContainers,
                                      Function<HasContainer, Boolean> func) {
        hasContainers.removeIf(container -> {
            return container.getKey().startsWith("~") && func.apply(container);
        });
    }

    public static String page(GraphTraversal<?, ?> traversal) {
        QueryHolder holder = firstPageStep(traversal);
        E.checkState(holder != null,
                     "Invalid paging traversal: %s", traversal.getClass());
        Object page = holder.metadata(PageInfo.PAGE);
        if (page == null) {
            return null;
        }
        /*
         * Page is instance of PageInfo if traversal with condition like:
         * g.V().has("x", 1).has("~page", "").
         * Page is instance of PageState if traversal without condition like:
         * g.V().has("~page", "")
         */
        assert page instanceof PageInfo || page instanceof PageState;
        return page.toString();
    }

    public static QueryHolder rootStep(GraphTraversal<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (step instanceof QueryHolder) {
                return (QueryHolder) step;
            }
        }
        return null;
    }

    public static QueryHolder firstPageStep(GraphTraversal<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (step instanceof QueryHolder &&
                ((QueryHolder) step).queryInfo().paging()) {
                return (QueryHolder) step;
            }
        }
        return null;
    }

    public static boolean testProperty(Property<?> prop, Object expected) {
        Object actual = prop.value();
        P<Object> predicate;
        if (expected instanceof String &&
            ((String) expected).startsWith(TraversalUtil.P_CALL)) {
            predicate = TraversalUtil.parsePredicate(((String) expected));
        } else {
            predicate = ConditionP.eq(expected);
        }
        updatePredicateValue(predicate, ((HugeProperty<?>) prop).propertyKey());
        return predicate.test(actual);
    }

    public static Map<Id, Object> transProperties(HugeGraph graph,
                                                  Map<String, Object> props) {
        Map<Id, Object> pks = new HashMap<>(props.size());
        for (Map.Entry<String, Object> e : props.entrySet()) {
            PropertyKey pk = graph.propertyKey(e.getKey());
            pks.put(pk.id(), e.getValue());
        }
        return pks;
    }

    public static P<Object> parsePredicate(String predicate) {
        /*
         * Extract P from json string like {"properties": {"age": "P.gt(18)"}}
         * the `predicate` may actually be like "P.gt(18)"
         */
        Pattern pattern = Pattern.compile("^P\\.([a-z]+)\\(([\\S ]*?)\\)" +
                                          "((\\.not\\(\\))?)$");
        Matcher matcher = pattern.matcher(predicate);
        if (!matcher.find()) {
            throw new HugeException("Invalid predicate: %s", predicate);
        }

        String method = matcher.group(1);
        String value = matcher.group(2);
        P<Object> p;
        switch (method) {
            case "eq":
                p = P.eq(predicateNumber(value));
                break;
            case "neq":
                p = P.neq(predicateNumber(value));
                break;
            case "lt":
                p = P.lt(predicateNumber(value));
                break;
            case "lte":
                p = P.lte(predicateNumber(value));
                break;
            case "gt":
                p = P.gt(predicateNumber(value));
                break;
            case "gte":
                p = P.gte(predicateNumber(value));
                break;
            case "between":
                Number[] params = predicateNumbers(value, 2);
                p = P.between(params[0], params[1]);
                break;
            case "inside":
                params = predicateNumbers(value, 2);
                p = P.inside(params[0], params[1]);
                break;
            case "outside":
                params = predicateNumbers(value, 2);
                p = P.outside(params[0], params[1]);
                break;
            case "within":
                p = P.within(predicateArgs(value));
                break;
            case "textcontains":
                p = ConditionP.textContains(predicateArg(value));
                break;
            case "contains":
                // Just for inner use case like auth filter
                p = ConditionP.contains(predicateArg(value));
                break;
            default:
                throw new NotSupportException("predicate '%s'", method);
        }
        if (".not()".equals(matcher.group(3))) {
            p = P.not(p);
        }
        return p;
    }

    public static Condition parsePredicate(PropertyKey pk, String predicate) {
        Pattern pattern = Pattern.compile("^P\\.([a-z]+)\\(([\\S ]*?)\\)" +
                                          "((\\.not\\(\\))?)$");
        Matcher matcher = pattern.matcher(predicate);
        if (!matcher.find()) {
            throw new HugeException("Invalid predicate: %s", predicate);
        }

        String method = matcher.group(1);
        String value = matcher.group(2);
        Object validValue;
        Condition condition;
        //System.out.printf("method = %s, value = %s", method, value);
        switch (method) {
            case "eq":
                validValue = validPropertyValue(predicateNumber(value), pk);
                condition = Condition.eq(pk.id(), validValue);
                break;
            case "neq":
                validValue = validPropertyValue(predicateNumber(value), pk);
                condition = Condition.neq(pk.id(), validValue);
                break;
            case "lt":
                validValue = validPropertyValue(predicateNumber(value), pk);
                condition = Condition.lt(pk.id(), validValue);
                break;
            case "lte":
                validValue = validPropertyValue(predicateNumber(value), pk);
                condition = Condition.lte(pk.id(), validValue);
            case "gt":
                validValue = validPropertyValue(predicateNumber(value), pk);
                condition = Condition.gt(pk.id(), validValue);
                break;
            case "gte":
                validValue = validPropertyValue(predicateNumber(value), pk);
                condition = Condition.gte(pk.id(), validValue);
                break;
            case "between":
                Number[] params = predicateNumbers(value, 2);
                Object v1 = validPropertyValue(params[0], pk);
                Object v2 = validPropertyValue(params[1], pk);
                condition = Condition.and(Condition.gte(pk.id(), v1),
                                          Condition.lt(pk.id(), v2));
                break;
            case "inside":
                params = predicateNumbers(value, 2);
                v1 = validPropertyValue(params[0], pk);
                v2 = validPropertyValue(params[1], pk);
                condition = Condition.and(Condition.gt(pk.id(), v1),
                                          Condition.lt(pk.id(), v2));
                break;
            case "outside":
                params = predicateNumbers(value, 2);
                v1 = validPropertyValue(params[0], pk);
                v2 = validPropertyValue(params[1], pk);
                condition = Condition.and(Condition.lt(pk.id(), v1),
                                          Condition.gt(pk.id(), v2));
                break;
            case "within":
                // TODO: Shall we use set?
                List<?> values = predicateArgs(value);
                condition = Condition.in(pk.id(), values);
                break;
            case "textcontains":
                validValue = validPropertyValue(value, pk);
                condition = Condition.textContains(pk.id(), (String) validValue);
                break;
            case "contains":
                validValue = validPropertyValue(value, pk);
                condition = Condition.contains(pk.id(), validValue);
                break;
            default:
                throw new NotSupportException("predicate '%s'", method);
        }
        if (".not()".equals(matcher.group(3))) {
            condition = condition.not();
        }
        return condition;
    }

    private static Number predicateNumber(String value) {
        try {
            return JsonUtil.fromJson(value, Number.class);
        } catch (Exception e) {
            // Try to parse date
            if (e.getMessage().contains("not a valid number") ||
                e.getMessage().contains("Unexpected character ('-'")) {
                try {
                    if (value.startsWith("\"")) {
                        value = JsonUtil.fromJson(value, String.class);
                    }
                    return DateUtil.parse(value).getTime();
                } catch (Exception ignored) {
                }
            }

            throw new HugeException(
                    "Invalid value '%s', expect a number", e, value);
        }
    }

    private static Number[] predicateNumbers(String value, int count) {
        List<Object> values = predicateArgs(value);
        if (values.size() != count) {
            throw new HugeException("Invalid numbers size %s, expect %s",
                                    values.size(), count);
        }
        for (int i = 0; i < count; i++) {
            Object v = values.get(i);
            if (v instanceof Number) {
                continue;
            }
            try {
                v = predicateNumber(v.toString());
            } catch (Exception ignored) {
                // pass
            }
            if (v instanceof Number) {
                values.set(i, v);
                continue;
            }
            throw new HugeException(
                    "Invalid value '%s', expect a list of number", value);
        }
        return values.toArray(new Number[0]);
    }

    @SuppressWarnings("unchecked")
    private static <V> V predicateArg(String value) {
        try {
            return (V) JsonUtil.fromJson(value, Object.class);
        } catch (Exception e) {
            throw new HugeException(
                    "Invalid value '%s', expect a single value", e, value);
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> List<V> predicateArgs(String value) {
        try {
            return JsonUtil.fromJson("[" + value + "]", List.class);
        } catch (Exception e) {
            throw new HugeException(
                    "Invalid value '%s', expect a list", e, value);
        }
    }
}