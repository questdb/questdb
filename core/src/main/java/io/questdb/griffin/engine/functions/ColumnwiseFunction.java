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

import io.questdb.cairo.sql.Function;

/**
 * A scalar function whose value the column-wise batch evaluator of the parallel GROUP BY,
 * {@link io.questdb.griffin.engine.groupby.GroupByBatchKernels}, can compute for a whole batch of
 * rows at a time instead of one virtual call per row.
 * <p>
 * The function names the operation its own getter performs; the evaluator holds a loop for each
 * (operation, operand getter, result type) it supports, written to give the same result bit for bit
 * as the getter, including NULL handling and integer overflow. An implementation must therefore
 * declare exactly what its getter does:
 * <ul>
 *     <li>{@link #OP_ADD}, {@link #OP_SUB}, {@link #OP_MUL} and {@link #OP_DIV} are binary
 *     functions whose getter reads both operands with the getter of the function's own type.
 *     INT and LONG return NULL when either operand is NULL (and DIV also for a zero divisor), and
 *     wrap on overflow. FLOAT and DOUBLE apply the IEEE operation, and DIV turns a non-finite
 *     quotient into NaN.</li>
 *     <li>{@link #OP_CAST} is a unary function whose getter reads its argument with the getter of
 *     {@link #getColumnwiseOperandType()} and converts it with QuestDB's explicit cast rule for that
 *     pair of types (see {@code GroupByBatchKernels.castSupported}).</li>
 * </ul>
 * The evaluator uses an implementation only when the method is declared by the function's own
 * class, so a subclass with different arithmetic does not inherit the claim. Anything that does
 * not implement this interface, or whose operation and types the evaluator has no loop for, keeps
 * the per-row path.
 */
public interface ColumnwiseFunction extends Function {
    int OP_ADD = 1;
    int OP_CAST = 5;
    int OP_DIV = 4;
    int OP_MUL = 3;
    int OP_SUB = 2;

    /**
     * Returns the operation this function's getter performs, one of the {@code OP_*} constants.
     */
    int getColumnwiseOp();

    /**
     * Returns the type, as a {@link io.questdb.cairo.ColumnType} tag, whose getter this function
     * reads its operands with. Binary arithmetic reads them with its own type's getter.
     */
    default int getColumnwiseOperandType() {
        return getType();
    }
}
