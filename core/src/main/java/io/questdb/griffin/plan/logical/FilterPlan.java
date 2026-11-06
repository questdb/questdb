/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.plan.logical;

import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class FilterPlan extends ForwardingPlan {
    public static final ObjectFactory<FilterPlan> FACTORY = FilterPlan::new;
    private Algorithm algorithm;
    private BoundExpression predicate;

    @Override
    public void clear() {
        super.clear();
        algorithm = null;
        predicate = null;
    }

    /**
     * How the generator applies the predicate over its input, which operator planning records; null for a filter fused
     * into the scan under it, whose scan records it, see {@link ScanPlan#getResidualAlgorithm}.
     */
    public Algorithm getAlgorithm() {
        checkPhysicalRead("FilterPlan.algorithm");
        return algorithm;
    }

    public BoundExpression getPredicate() {
        return predicate;
    }

    public FilterPlan of(LogicalPlan input, BoundExpression predicate, int position) {
        configure(input, position);
        this.predicate = Objects.requireNonNull(predicate);
        deriveOutput();
        return this;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public void visitFacts(PlanFactRecorder visitor) {
        super.visitFacts(visitor);
        visitor.fact("algorithm", algorithm);
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        predicate = PlanReads.expression(predicate, visitor);
    }

    /**
     * How the generator applies a predicate over its input: it folds a constant, keeping the input when the constant
     * is true and replacing it with an empty factory when it is false, gates the input once on a runtime constant, or
     * builds a filter, on one thread, in parallel over the page frames of the factory under it, where it applies the
     * LIMIT it is given, or, for the keep flag of SUBSAMPLE, within the light cached window under it, which selects the
     * rows its sole row-selecting function keeps.
     */
    public enum Algorithm {
        FOLDED_FALSE, FOLDED_TRUE, GATE, PARALLEL, SERIAL, WINDOW_KEEP_FLAG;

        /**
         * Whether the generator folds the predicate, building no filter.
         */
        public boolean isFolded() {
            return this == FOLDED_TRUE || this == FOLDED_FALSE;
        }

        /**
         * Whether the generator builds a filter that evaluates the predicate on each row.
         */
        public boolean isFiltering() {
            return this == PARALLEL || this == SERIAL || this == WINDOW_KEEP_FLAG;
        }
    }
}
