/*******************************************************************************
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;

/**
 * Abstract base class for binary equality comparison functions.
 */
public abstract class AbstractEqBinaryFunction extends NegatableBooleanFunction implements BinaryFunction {
    /**
     * The left argument function.
     */
    protected final Function left;
    /**
     * The right argument function.
     */
    protected final Function right;

    /**
     * Constructs a new binary equality function.
     *
     * @param left  the left argument function
     * @param right the right argument function
     */
    public AbstractEqBinaryFunction(Function left, Function right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Function getLeft() {
        return left;
    }

    @Override
    public Function getRight() {
        return right;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(left);
        if (negated) {
            sink.val('!');
        }
        sink.val('=').val(right);
    }
}
