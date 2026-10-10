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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Mutable;

/**
 * A compilation-owned expression description. Populate before publication and do
 * not change it until pool reset; rewrites acquire a replacement expression.
 */
public abstract sealed class BoundExpression implements Mutable
        permits BindVariableExpression, ColumnExpression, ConstantExpression, CursorExpression, FunctionExpression, OuterColumnExpression, TypeExpression {
    /**
     * The aggregate function of the call requires its input in ascending designated timestamp order.
     */
    public static final int ASCENDING_TIMESTAMP = 512;
    public static final int CONSTANT = 1;
    /**
     * The aggregate function of the call can stop reading its input early.
     */
    public static final int EARLY_EXIT = 256;
    /**
     * The window function of the call needs more than one pass over its input.
     */
    public static final int MULTI_PASS = 64;
    public static final int NON_DETERMINISTIC = 4;
    /**
     * The function, or a function under it, does not support parallel execution.
     */
    public static final int NO_PARALLELISM = 128;
    /**
     * The function, or a function under it, does not support random access to its records.
     */
    public static final int NO_RANDOM_ACCESS = 32;
    /**
     * The function, or a function under it, returns random values.
     */
    public static final int RANDOM = 16;
    /**
     * The window function of the call can enumerate the rows its BOOLEAN result keeps, so the light cached window
     * factory can select them itself.
     */
    public static final int ROW_SELECTING = 2048;
    public static final int RUNTIME_CONSTANT = 2;
    public static final int STABLE_WITHIN_EXECUTION = 8;
    /**
     * The aggregate function of the call requires the designated timestamp of its input as its second argument.
     */
    public static final int TIMESTAMP_ARGUMENT = 1024;
    private int dataType = ColumnType.UNDEFINED;
    private int functionFlags;
    private int position = -1;

    /**
     * The flags of a bound expression whose function is {@code function}.
     */
    public static int functionFlags(Function function) {
        return (function.isConstant() ? CONSTANT : 0)
                | (function.isRuntimeConstant() ? RUNTIME_CONSTANT : 0)
                | (function.isNonDeterministic() ? NON_DETERMINISTIC : 0)
                | (function.isStableWithinExecution() ? STABLE_WITHIN_EXECUTION : 0)
                | (function.isRandom() ? RANDOM : 0)
                | (function.supportsRandomAccess() ? 0 : NO_RANDOM_ACCESS)
                | (function instanceof WindowFunction window ? windowFlags(window) : 0)
                | (function.supportsParallelism() ? 0 : NO_PARALLELISM)
                | (function instanceof GroupByFunction aggregate ? aggregateFlags(aggregate) : 0);
    }

    @Override
    public void clear() {
        dataType = ColumnType.UNDEFINED;
        functionFlags = 0;
        position = -1;
    }

    public int getDataType() {
        return dataType;
    }

    public int getFunctionFlags() {
        return functionFlags;
    }

    public int getPosition() {
        return position;
    }

    /**
     * Visits the expression and then, unless the visitor skips them, the arguments of a call, depth first; returns
     * false when the visitor stops the walk.
     */
    public final boolean walk(ExpressionVisitor visitor) {
        final int action = visitor.visit(this);
        if (action == TreeWalk.STOP) {
            return false;
        }
        if (action == TreeWalk.CONTINUE && this instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                final BoundExpression argument = call.argumentAt(i);
                if (argument != null && !argument.walk(visitor)) {
                    return false;
                }
            }
        }
        return true;
    }

    protected void configure(int dataType, int position) {
        this.dataType = dataType;
        this.position = position;
        this.functionFlags = STABLE_WITHIN_EXECUTION;
    }

    protected void configure(int dataType, int position, int functionFlags) {
        configure(dataType, position);
        this.functionFlags = functionFlags;
    }

    private static int aggregateFlags(GroupByFunction aggregate) {
        return (aggregate.isEarlyExitSupported() ? EARLY_EXIT : 0)
                | (aggregate.isAscendingTimestampRequired() ? ASCENDING_TIMESTAMP : 0)
                | (aggregate.isTimestampArgumentRequired() ? TIMESTAMP_ARGUMENT : 0);
    }

    private static int windowFlags(WindowFunction window) {
        return (window.getPassCount() != WindowFunction.ZERO_PASS ? MULTI_PASS : 0)
                | (window.isRowSelecting() ? ROW_SELECTING : 0);
    }

}
