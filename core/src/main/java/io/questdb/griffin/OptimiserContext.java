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

package io.questdb.griffin;

import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The query level the optimiser rewrites: the expression services of the binder that produced the plan and
 * the allocator of the column ids that rewrites define. {@link SqlOptimiser} resets it per query and its
 * passes read it.
 */
final class OptimiserContext implements Mutable {
    private SqlExecutionContext executionContext;
    private FunctionBinder functionBinder;
    private TableFunctionSources functionSources;
    private FunctionInstantiator instantiator;
    private int nextColumnId;
    private BoundExpressionRewriter rewriter;

    @Override
    public void clear() {
        executionContext = null;
        functionBinder = null;
        functionSources = null;
        instantiator = null;
        nextColumnId = 0;
        rewriter = null;
    }

    /**
     * Binds a call over already-bound arguments, see {@link FunctionBinder#bindCall}.
     */
    BoundExpression bindCall(CharSequence name, int position, ObjList<? extends BoundExpression> args, OutputSchema input) throws SqlException {
        return functionBinder.bindCall(name, position, args, input, executionContext);
    }

    TableFunctionSources getFunctionSources() {
        return functionSources;
    }

    BoundExpressionRewriter getRewriter() {
        return rewriter;
    }

    boolean isNullRejecting(FunctionExpression call, int columnArgument) {
        return instantiator.isNullRejecting(call, columnArgument, executionContext);
    }

    /**
     * Allocates a column id no other column of the query level uses.
     */
    int newColumnId() {
        return nextColumnId++;
    }

    /**
     * Starts a query level: {@code nextColumnId} is the first id its bound plan does not use.
     */
    void of(
            BoundExpressionRewriter rewriter,
            FunctionBinder functionBinder,
            FunctionInstantiator instantiator,
            TableFunctionSources functionSources,
            int nextColumnId,
            SqlExecutionContext executionContext
    ) {
        this.rewriter = rewriter;
        this.functionBinder = functionBinder;
        this.instantiator = instantiator;
        this.functionSources = functionSources;
        this.nextColumnId = nextColumnId;
        this.executionContext = executionContext;
    }
}
