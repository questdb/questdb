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

package io.questdb.cairo;

import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;

/**
 * Minimal background-job {@code SqlExecutionContext} for compiling and evaluating composite
 * EXPRESSION partition-dimension expressions inside {@link TableWriter} (composite-partitioning
 * Plan 4e Task 2) -- mirrors {@code io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext}'s role
 * as a no-real-user-session execution context for a background compile/eval path, trimmed to what
 * {@code FunctionParser#parseFunction} and a compiled {@code Function}'s own {@code newInstance}/
 * {@code init} actually touch for this feature's safe subset (deterministic, non-cursor scalar
 * expressions over one table's own columns -- see {@code
 * CreateTableOperationBuilderImpl#assertDeterministic} for the DDL-time half of this gate).
 * <p>
 * {@link #allowNonDeterministicFunctions()} is hard-wired to always return {@code false} (never
 * settable) so {@code FunctionParser}'s own function-registry-level check ({@code
 * FunctionParser#checkAndCreateFunction}, strictly stronger than the DDL-time name-based deny-list
 * -- it consults the REAL {@code Function#isNonDeterministic()}) rejects any non-deterministic
 * function reachable in a composite EXPRESSION dimension, independent of and in addition to the
 * DDL-time gate.
 * <p>
 * No table reader is ever wired in (unlike the mat-view context, which needs one for base-table
 * SELECT execution): this context exists purely to bind/evaluate a scalar expression against a
 * {@link TableWriter}'s OWN {@code TableWriterMetadata} and O3 write buffers directly, never
 * through a real query plan -- see {@code TableWriter#ensureCompositeExpressionFunctionsCompiled}.
 */
public class CompositeExpressionSqlExecutionContext extends SqlExecutionContextImpl {

    public CompositeExpressionSqlExecutionContext(CairoEngine engine) {
        super(engine, 0);
        // Base class leaves bindVariableService unset (null) unless a subclass provides one; some
        // function factories touch sqlExecutionContext.getBindVariableService() unconditionally even
        // when no bind variable is actually present in the expression tree, so wire in a real (if
        // never-populated) instance rather than risk a bare NPE -- mirrors
        // MatViewRefreshSqlExecutionContext's identical constructor-time assignment.
        this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
    }

    @Override
    public boolean allowNonDeterministicFunctions() {
        return false;
    }

    @Override
    public void setAllowNonDeterministicFunction(boolean value) {
        // no-op -- always false, mirrors MatViewRefreshSqlExecutionContext's identical override.
    }
}
