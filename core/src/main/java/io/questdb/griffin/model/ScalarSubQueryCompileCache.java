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

package io.questdb.griffin.model;

import io.questdb.cairo.sql.Function;
import io.questdb.std.Misc;

/**
 * Hand-off slot for a scalar sub-query that was compiled as a speculative designated-timestamp
 * pruning bound and then declined.
 * <p>
 * {@code WhereClauseParser} has to compile a {@code QUERY} bound in full before it can tell whether
 * pruning is legal: only the generated factory answers
 * {@code checkCursorFunctionReturnsSingleTimestamp()} and {@code isStableWithinExecution()}. When the
 * answer is "no" the predicate stays a residual row filter, and the residual is compiled by a
 * separate path ({@code SqlCodeGenerator.generateFilter0} -&gt;
 * {@code FunctionParser.createCursorFunction}) that has no knowledge of the speculative compile.
 * Without this slot the declined factory is freed and the very same sub-query is generated a second
 * time, which doubles compile time at every level of a nested chain.
 * <p>
 * The slot carries the compiled sub-query function itself, not a value: the residual takes ownership
 * and calls {@code init()} on it exactly as it would on a function it had compiled itself, so the
 * sub-query is still evaluated by the residual, per execution, with unchanged semantics. That is what
 * makes this safe on the declined path, where the bound is by definition not provably stable and must
 * NOT be frozen into a shared value (see {@link ScalarTimestampBoundHolder}, which is the opposite
 * case - a bound that did prune and therefore may publish one frozen value).
 * <p>
 * Ownership: the slot is created and owned by the {@code WhereClauseParser} that parked the function,
 * and freed when that parser's generation completes, so an unconsumed entry cannot leak an open
 * factory. {@link #take()} transfers ownership to the caller and empties the slot, so exactly one
 * consumer can reuse the compile; any further re-compile of the same predicate (for example a
 * per-worker filter clone, which shares this slot by reference through
 * {@code ExpressionNode.deepClone}) simply finds it empty and generates its own copy, which is
 * required anyway because a sub-query factory cannot be shared across worker threads.
 */
public class ScalarSubQueryCompileCache {
    private Function compiled;

    /**
     * Releases anything still parked. Safe to call more than once, and leaves the slot empty rather
     * than dangling, so a late {@link #take()} through a node that still references this slot returns
     * null and the caller compiles its own copy instead of reusing freed memory.
     */
    public void free() {
        compiled = Misc.free(compiled);
    }

    /**
     * Parks a compiled sub-query function, taking ownership of it.
     */
    public void put(Function compiled) {
        // A node is parked at most once per generation; free defensively rather than leak if a
        // future caller ever parks twice.
        if (this.compiled != null && this.compiled != compiled) {
            Misc.free(this.compiled);
        }
        this.compiled = compiled;
    }

    /**
     * Transfers the parked function to the caller, or returns null when the slot is empty.
     */
    public Function take() {
        final Function f = compiled;
        compiled = null;
        return f;
    }
}
