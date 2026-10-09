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

public final class LimitPlan extends ForwardingPlan {
    public static final ObjectFactory<LimitPlan> FACTORY = LimitPlan::new;
    private Application application;
    private BoundExpression hi;
    private BoundExpression lo;

    @Override
    public void clear() {
        super.clear();
        application = null;
        hi = null;
        lo = null;
    }

    /**
     * Which operator applies the LIMIT, or null before order planning decided it.
     */
    public Application getApplication() {
        return application;
    }

    public BoundExpression getHi() {
        return hi;
    }

    public BoundExpression getLo() {
        return lo;
    }

    public LimitPlan of(LogicalPlan input, BoundExpression lo, BoundExpression hi, int position) {
        configure(input, position);
        this.lo = lo;
        this.hi = hi;
        return this;
    }

    public void setApplication(Application application) {
        this.application = application;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        lo = PlanReads.expression(lo, visitor);
        hi = PlanReads.expression(hi, visitor);
    }

    /**
     * The operator that applies a LIMIT.
     */
    public enum Application {
        /**
         * A LIMIT operator over the input.
         */
        OPERATOR,
        /**
         * The parallel filter of the input, which stops at the LIMIT.
         */
        INPUT,
        /**
         * The sort under the LIMIT, which keeps only the rows it selects.
         */
        SORT
    }
}
