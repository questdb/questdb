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

package io.questdb.griffin.engine.functions;

import io.questdb.griffin.PlanSink;

/**
 * A comparison a NOT folds into, so that {@code NOT (a < b)} costs one call per row like {@code a >= b}. A negated
 * comparison is not its complement: a NULL operand makes {@code a >= b} false and {@code NOT (a < b)} true, so
 * {@code getBool} returns the complement of its result when {@link #isComplemented} is set. Implementations test the
 * flag before they compare, {@code isComplemented ? !isMatch(rec) : isMatch(rec)}, which leaves each branch the plain
 * comparison to compile: XOR-ing the flag into the result slowed the uncomplemented DOUBLE comparison several times.
 */
public abstract class ComparisonFunction extends NegatableBooleanFunction {
    protected boolean isComplemented;

    public void complement() {
        isComplemented = !isComplemented;
    }

    @Override
    public final void toPlan(PlanSink sink) {
        if (isComplemented) {
            sink.val("not (");
            toComparisonPlan(sink);
            sink.val(')');
        } else {
            toComparisonPlan(sink);
        }
    }

    protected abstract void toComparisonPlan(PlanSink sink);
}
