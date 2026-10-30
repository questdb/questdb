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

/**
 * A comparison whose value a column-wise evaluator, such as the parallel window's row kernels
 * ({@code io.questdb.griffin.engine.window.AsyncWindowRowKernel}), can compute for a batch of rows
 * at a time instead of one virtual call per row. As with {@link ColumnwiseFunction}, the function
 * names the operation its own {@code getBool()} performs, and the evaluator holds a loop written to
 * give the same answer bit for bit:
 * <ul>
 *     <li>{@link #CMP_LT}: {@code Numbers.lessThan(left, right, negated)} over INT or LONG operands
 *     read with that type's getter (a NULL on either side is false, unless both are equal); over
 *     DOUBLE operands, {@code Numbers.equals(l, r)} decides equality, then
 *     {@code negated ? (eq || l > r) : (!eq && l < r)};</li>
 *     <li>{@link #CMP_EQ}: {@code negated != (left == right)} over INT or LONG operands, and
 *     {@code negated != Numbers.equals(l, r)} over DOUBLE operands.</li>
 * </ul>
 * An evaluator uses an implementation only when {@code getBool()} is declared by the class that
 * declares {@link #getColumnwiseComparison()}, so that a subclass with another comparison does not
 * inherit the claim.
 */
public interface ColumnwiseComparison extends BinaryFunction {
    int CMP_EQ = 2;
    int CMP_LT = 1;

    /**
     * The comparison this function's {@code getBool()} performs, one of the {@code CMP_*}
     * constants.
     */
    int getColumnwiseComparison();

    /**
     * The type, as a {@link io.questdb.cairo.ColumnType} tag (INT, LONG or DOUBLE), whose getter
     * the comparison reads both operands with.
     */
    int getColumnwiseOperandType();

    /**
     * Whether the comparison is negated: {@code >=} for {@link #CMP_LT}, {@code !=} for
     * {@link #CMP_EQ}.
     */
    boolean isNegated();
}
