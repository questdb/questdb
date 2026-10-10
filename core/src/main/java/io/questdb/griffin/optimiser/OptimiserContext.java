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

package io.questdb.griffin.optimiser;

import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.CallBinder;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.PlanTables;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TableFunctionSources;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The query level the optimiser rewrites: the expression services of the binder that produced the plan and
 * the allocator of the column ids that rewrites define. {@link SqlOptimiser} resets it per query and its
 * passes read it.
 */
final class OptimiserContext implements Mutable {
    private final ObjList<BoundExpression> callArguments;
    private final CallBinder callBinder;
    private final CharacterStore characterStore;
    private final TableFunctionSources functionSources;
    private final FunctionInstantiator instantiator;
    private final BoundExpressionRewriter rewriter;
    private final PlanNodePools planNodes;
    private final PlanTables planTables;
    private SqlExecutionContext executionContext;

    OptimiserContext(
            BoundExpressionRewriter rewriter,
            CallBinder callBinder,
            FunctionInstantiator instantiator,
            TableFunctionSources functionSources,
            PlanNodePools planNodes,
            PlanTables planTables,
            CharacterStore characterStore,
            ObjList<BoundExpression> callArguments
    ) {
        this.planNodes = planNodes;
        this.planTables = planTables;
        this.rewriter = rewriter;
        this.callBinder = callBinder;
        this.instantiator = instantiator;
        this.functionSources = functionSources;
        this.characterStore = characterStore;
        this.callArguments = callArguments;
    }

    @Override
    public void clear() {
        executionContext = null;
    }

    /**
     * Binds the call {@code name} over the already-bound {@link #getCallArguments}, which it clears, see
     * {@link CallBinder#bindCall}.
     */
    BoundExpression bindCall(CharSequence name, int position, OutputSchema input) throws SqlException {
        final BoundExpression bound = callBinder.bindCall(name, position, callArguments, input, executionContext);
        callArguments.clear();
        return bound;
    }

    /**
     * Binds the call {@code name} over the two already-bound arguments.
     */
    BoundExpression bindCall(CharSequence name, int position, BoundExpression left, BoundExpression right, OutputSchema input) throws SqlException {
        callArguments.clear();
        callArguments.add(left);
        callArguments.add(right);
        return bindCall(name, position, input);
    }

    /**
     * The arguments the next {@link #bindCall(CharSequence, int, OutputSchema)} binds.
     */
    ObjList<BoundExpression> getCallArguments() {
        return callArguments;
    }

    SqlExecutionContext getExecutionContext() {
        return executionContext;
    }

    TableFunctionSources getFunctionSources() {
        return functionSources;
    }

    BoundExpressionRewriter getRewriter() {
        return rewriter;
    }

    /**
     * The access facts of the table the scan reads, from the snapshot code generation reads too.
     */
    TableAccessInfo getTableAccessInfo(ScanPlan scan) {
        return planTables.of(scan);
    }

    boolean isNullRejecting(FunctionExpression call, int columnArgument) {
        return instantiator.isNullRejecting(call, columnArgument, executionContext);
    }

    /**
     * Allocates a column id no other column of the statement has.
     */
    int newColumnId() {
        return planNodes.nextColumnId();
    }

    void of(SqlExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    /**
     * The name, or the name with the lowest numeric suffix, that no column of the output has in any case.
     */
    CharSequence uniqueName(OutputSchema output, CharSequence name) {
        if (!output.hasColumnName(name)) {
            return name;
        }
        for (int suffix = 1; ; suffix++) {
            final CharacterStoreEntry entry = characterStore.newEntry();
            entry.put(name).put(suffix);
            final CharSequence candidate = entry.toImmutable();
            if (!output.hasColumnName(candidate)) {
                return candidate;
            }
        }
    }
}
