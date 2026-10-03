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

public final class LimitPlan extends UnaryPlan {
    public static final ObjectFactory<LimitPlan> FACTORY = LimitPlan::new;
    private BoundExpression hi;
    private BoundExpression lo;

    @Override
    public void clear() {
        super.clear();
        hi = null;
        lo = null;
    }

    /**
     * Sets the output to the input's columns and designated timestamp: a limit only drops rows.
     */
    public void deriveOutput() {
        getOutput().copyFrom(getInput().getOutput());
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
}
