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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;

/**
 * A BOOLEAN function whose value a column-wise evaluator, such as the parallel window's row kernels
 * ({@code io.questdb.griffin.engine.window.AsyncWindowRowKernel}), can compute for a batch of rows
 * from its arguments' values, as its {@code getBool()} computes it for one row. The class that
 * declares {@link #getColumnwiseBooleanOp()} declares {@code getBool()} too, so that a subclass with
 * another {@code getBool()} does not claim this one's.
 */
public interface ColumnwiseBooleanOp extends Function {
    /**
     * {@code a = b} of two BOOLEAN arguments, {@code !=} when negated:
     * {@code negated != (left.getBool(rec) == right.getBool(rec))}.
     */
    int OP_EQ = 3;
    /**
     * {@code a IS NULL}, {@code IS NOT NULL} when negated: the argument read with the getter of
     * {@link #getColumnwiseOperandType()}, NULL as {@code Numbers.isNull()} judges a FLOAT or DOUBLE,
     * or the type's NULL sentinel for INT, LONG and TIMESTAMP.
     */
    int OP_IS_NULL = 1;
    /**
     * {@code a OR b}, as {@code left.getBool(rec) || right.getBool(rec)}.
     */
    int OP_OR = 4;

    /**
     * The argument of {@link #OP_IS_NULL} (any index); otherwise the left argument for index 0, the
     * right one for index 1.
     */
    Function getColumnwiseArg(int index);

    /**
     * One of the {@code OP_*} constants.
     */
    int getColumnwiseBooleanOp();

    /**
     * The type, as a {@link io.questdb.cairo.ColumnType} tag, whose getter reads the argument of
     * {@link #OP_IS_NULL} (FLOAT, DOUBLE, INT, LONG or TIMESTAMP); BOOLEAN for the others.
     */
    int getColumnwiseOperandType();

    /**
     * Whether the value is negated: {@code IS NOT NULL}, {@code !=}; false for AND and OR.
     */
    boolean isColumnwiseNegated();
}
