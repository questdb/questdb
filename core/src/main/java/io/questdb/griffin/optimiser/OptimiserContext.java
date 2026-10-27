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
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.PlanTables;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TableFunctionSources;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The query level the optimiser rewrites: the expression services of the binder that produced the plan and
 * the allocator of the column ids that rewrites define. {@link SqlOptimiser} resets it per query and its
 * passes read it.
 */
final class OptimiserContext implements Mutable {
    private final CallBinder callBinder;
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
            PlanTables planTables
    ) {
        this.planNodes = planNodes;
        this.planTables = planTables;
        this.rewriter = rewriter;
        this.callBinder = callBinder;
        this.instantiator = instantiator;
        this.functionSources = functionSources;
    }

    @Override
    public void clear() {
        executionContext = null;
    }

    /**
     * Holds a second key of the step on the master column of its key at {@code keyIndex} as an equality between the
     * two slave columns of the step's input, {@code output}: the input rows that match the master column on both keys
     * are the rows whose two columns are equal, so the step filters its input on that equality and keys on the first
     * pair alone. The step must drop its unmatched input rows. A second key on the same slave column adds nothing.
     */
    void addKeyFilter(JoinInput step, int keyIndex, int slaveId, OutputSchema output, int position) throws SqlException {
        final int keySlaveId = step.getKeys().getQuick(keyIndex).getSlaveColumnId();
        if (keySlaveId == slaveId) {
            return;
        }
        final BoundExpression equality = bindEquality(slaveId, keySlaveId, output, position);
        step.setKeyFilter(step.getKeyFilter() == null ? equality : rewriter.combineConjunction(step.getKeyFilter(), equality, position));
    }

    /**
     * Binds the call {@code name} over the already-bound {@link #getCallArguments}, which it clears, see
     * {@link CallBinder#bindCall}.
     */
    BoundExpression bindCall(CharSequence name, int position, OutputSchema input) throws SqlException {
        return callBinder.bindCall(name, position, input, executionContext);
    }

    /**
     * Binds the call {@code name} over the two already-bound arguments.
     */
    BoundExpression bindCall(CharSequence name, int position, BoundExpression left, BoundExpression right, OutputSchema input) throws SqlException {
        return callBinder.bindCall(name, position, left, right, input, executionContext);
    }

    /**
     * Binds the equality of the two columns of {@code output}.
     */
    BoundExpression bindEquality(int leftId, int rightId, OutputSchema output, int position) throws SqlException {
        return bindCall("=", position, column(output, leftId, position), column(output, rightId, position), output);
    }

    /**
     * Binds {@code IS NULL} over the column of {@code output}, as the parser spells it: {@code = NULL}.
     */
    BoundExpression bindIsNull(int columnId, OutputSchema output, int position) throws SqlException {
        return bindCall("=", position, column(output, columnId, position), planNodes.constants.next().ofNull(position), output);
    }

    /**
     * The arguments the next {@link #bindCall(CharSequence, int, OutputSchema)} binds.
     */
    ObjList<BoundExpression> getCallArguments() {
        return callBinder.getCallArguments();
    }

    SqlExecutionContext getExecutionContext() {
        return executionContext;
    }

    FunctionInstantiator getInstantiator() {
        return instantiator;
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

    /**
     * Whether the predicate drops every row whose columns it reads are all NULL, see
     * {@link FunctionInstantiator#isNullRejecting}.
     */
    boolean isNullRejecting(BoundExpression predicate) {
        return instantiator.isNullRejecting(predicate, executionContext);
    }

    /**
     * Whether a decorrelation domain of the statement re-reads a join input it shares with its master.
     */
    boolean hasSharedSources() {
        return planNodes.hasSharedSources();
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
     * An upper bound of the rows the factory of the plan returns, -1 when unknown: the row count of the table whose scan
     * bounds them, see {@link PhysicalProperties#rowBoundScan}, capped by the LIMITs along the way, see
     * {@link PhysicalProperties#rowBoundLimit}, which estimates the count of a LIMIT only execution knows.
     */
    long rowBound(LogicalPlan plan) {
        final ScanPlan scan = PhysicalProperties.rowBoundScan(plan);
        final long limit = PhysicalProperties.rowBoundLimit(plan);
        if (scan != null) {
            return Math.min(getTableAccessInfo(scan).getRowCount(), limit);
        }
        return limit < Long.MAX_VALUE ? limit : -1;
    }

    private ColumnExpression column(OutputSchema output, int columnId, int position) {
        return planNodes.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position);
    }
}
