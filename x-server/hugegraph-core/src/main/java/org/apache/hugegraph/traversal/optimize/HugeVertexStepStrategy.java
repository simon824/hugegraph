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

import java.util.List;
import java.util.Objects;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeVertexStepStrategy
        extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
        implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 491355700217483162L;

    private static final HugeVertexStepStrategy INSTANCE;

    static {
        INSTANCE = new HugeVertexStepStrategy();
    }

    private HugeVertexStepStrategy() {
        // pass
    }

    public static HugeVertexStepStrategy instance() {
        return INSTANCE;
    }

    private static boolean hasTreeOrPathStep(final Traversal.Admin<?, ?> traversal) {

        Traversal.Admin<?, ?> rootTraversal =
                TraversalHelper.getRootTraversal(traversal);

        return TraversalHelper.hasStepOfClass(TreeStep.class, rootTraversal) ||
               TraversalHelper.hasStepOfClass(PathStep.class, rootTraversal);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        List<VertexStep> steps = TraversalHelper.getStepsOfClass(
                VertexStep.class, traversal);

        // 判断是否有limit类似的Step
        boolean flag = isContainRangeGlobalStep(traversal);

        // 判断是否包含EdgeOtherVertexStep
        boolean isContainEdgeOtherVertexStep = isContainEdgeOtherVertexStep(traversal);

        // 判断最后一步是否是VertexStep
        boolean isEndVertexStep = isEndVertexStep(traversal);

        int stepSize = steps.size();
        int endStepIndex = stepSize - 1;
        for (int i = 0; i < stepSize; i++) {
            boolean endStepFlag = false;
            VertexStep originStep = steps.get(i);
            if (i == endStepIndex && isEndVertexStep) {
                endStepFlag = true;
            }
            HugeVertexStep<?> newStep = new HugeVertexStep<>(originStep, flag
                    , endStepFlag, isContainEdgeOtherVertexStep);
            TraversalHelper.replaceStep(originStep, newStep, traversal);

            TraversalUtil.extractHasContainer(newStep, traversal);

            // TODO: support order-by optimize
            // TraversalUtil.extractOrder(newStep, traversal);

            TraversalUtil.extractRange(newStep, traversal, true);

            TraversalUtil.extractCount(newStep, traversal);

            if (hasTreeOrPathStep(traversal)) {
                newStep.setBatchPropertyPrefetching(true);
            }
        }
    }

    private boolean isContainRangeGlobalStep(final Traversal.Admin<?, ?> traversal) {
        // 判断是否有limit类似的Step
        List<RangeGlobalStep> stepsOfClass = TraversalHelper.getStepsOfClass(
                RangeGlobalStep.class, traversal);
        return stepsOfClass.size() > 0;

    }

    private boolean isContainEdgeOtherVertexStep(final Traversal.Admin<?, ?> traversal) {
        // 判断是否有EdgeOtherVertexStep类似的Step
        List<EdgeOtherVertexStep> stepsOfClass = TraversalHelper.getStepsOfClass(
                EdgeOtherVertexStep.class, traversal);
        return stepsOfClass.size() > 0;

    }

    private boolean isEndVertexStep(final Traversal.Admin<?, ?> traversal) {
        boolean isEndVertexStep = false;
        if (traversal.getSteps().size() == 0) {
            return false;
        }

        Step endStep = traversal.getSteps().get(traversal.getSteps().size() - 1);
        if (Objects.nonNull(endStep) && endStep instanceof VertexStep) {
            isEndVertexStep = true;
        }
        return isEndVertexStep;
    }
}
