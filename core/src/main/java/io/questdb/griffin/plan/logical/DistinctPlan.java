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

/**
 * Removes duplicate full input tuples without changing column identities.
 */
public final class DistinctPlan extends ForwardingPlan {
    public static final ObjectFactory<DistinctPlan> FACTORY = DistinctPlan::new;
    private Algorithm algorithm;

    @Override
    public void clear() {
        super.clear();
        algorithm = null;
    }

    /**
     * How the generator removes the duplicates, which operator planning records; null before it planned the DISTINCT.
     */
    public Algorithm getAlgorithm() {
        checkPhysicalRead("DistinctPlan.algorithm");
        return algorithm;
    }

    public DistinctPlan of(LogicalPlan input, int position) {
        configure(input, position);
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

    /**
     * How the generator removes duplicate tuples: over an input with random access and a designated timestamp, it
     * compares the tuples of each timestamp alone and keeps the input's order; otherwise it hashes every tuple, and may
     * stop at the LIMIT over it.
     */
    public enum Algorithm {
        HASH, TIME_SERIES
    }
}
