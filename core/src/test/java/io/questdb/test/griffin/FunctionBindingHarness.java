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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.FunctionBinder;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.std.IntHashSet;
import io.questdb.std.ObjList;

import java.io.Closeable;

/**
 * Binds, rewrites and instantiates expressions through one stand-alone {@link FunctionBinder} and its context's stages.
 */
public final class FunctionBindingHarness implements Closeable {
    private final FunctionBinder binder;
    private final FunctionInstantiator instantiator;
    private final BoundExpressionRewriter rewriter;

    public FunctionBindingHarness(FunctionParser parser) {
        this.binder = FunctionBinder.newStandalone(parser);
        this.instantiator = binder.getFunctionInstantiator();
        this.rewriter = binder.getExpressionRewriter();
    }

    public BoundExpression bind(ExpressionNode node, OutputSchema input, CharSequence inputAlias, SqlExecutionContext executionContext) throws SqlException {
        return binder.bind(node, input, inputAlias, executionContext);
    }

    public BoundExpression bind(ExpressionNode node, OutputSchema input, CharSequence inputAlias, int preferredType, SqlExecutionContext executionContext) throws SqlException {
        return binder.bind(node, input, inputAlias, preferredType, executionContext);
    }

    public BoundExpression bind(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<ColumnExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return binder.bind(node, input, inputAlias, replacementNodes, replacementColumns, executionContext);
    }

    public FunctionExpression bindAggregate(ExpressionNode node, OutputSchema input, CharSequence inputAlias, SqlExecutionContext executionContext) throws SqlException {
        return binder.bindAggregate(node, input, inputAlias, executionContext);
    }

    public BoundExpression bindCall(CharSequence name, int position, ObjList<? extends BoundExpression> args, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        return binder.bindCall(name, position, args, input, executionContext);
    }

    public BoundExpression bindGroupByExpression(ExpressionNode node, OutputSchema input, CharSequence inputAlias, SqlExecutionContext executionContext) throws SqlException {
        return binder.bindGroupByExpression(node, input, inputAlias, executionContext);
    }

    public BoundExpression bindPredicate(ExpressionNode node, OutputSchema input, CharSequence inputAlias, SqlExecutionContext executionContext) throws SqlException {
        return binder.bindPredicate(node, input, inputAlias, executionContext);
    }

    public BoundExpression bindPredicate(ExpressionNode node, OutputSchema input, CharSequence inputAlias, IntHashSet nativeTimestampIds, SqlExecutionContext executionContext) throws SqlException {
        return binder.bindPredicate(node, input, inputAlias, nativeTimestampIds, executionContext);
    }

    public BoundExpression bindPredicate(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            IntHashSet nativeTimestampIds,
            int preferredRootType,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return binder.bindPredicate(node, input, inputAlias, nativeTimestampIds, preferredRootType, executionContext);
    }

    public FunctionExpression bindWindow(ExpressionNode node, OutputSchema input, CharSequence inputAlias, SqlExecutionContext executionContext) throws SqlException {
        return binder.bindWindow(node, input, inputAlias, executionContext);
    }

    public void clear() {
        binder.clearExpressions();
    }

    @Override
    public void close() {
        binder.clearExpressions();
    }

    public FunctionExpression commuteEquality(FunctionExpression original) {
        return rewriter.commuteEquality(original);
    }

    public BoundExpression copyRemappedColumns(BoundExpression expression, ProjectPlan projection) {
        return rewriter.copyRemappedColumns(expression, projection);
    }

    public Function instantiate(BoundExpression expression, OutputSchema input) {
        return instantiator.instantiate(expression, input);
    }

    public Function instantiate(BoundExpression expression, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        return instantiator.instantiate(expression, input, executionContext);
    }

    public Function instantiate(BoundExpression expression, OutputSchema input, RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        return instantiator.instantiate(expression, input, metadata, executionContext);
    }

    public Function instantiateAggregate(FunctionExpression expression, OutputSchema input, RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        return instantiator.instantiateAggregate(expression, input, metadata, executionContext);
    }

    public WindowFunction instantiateWindow(FunctionExpression expression, OutputSchema input, RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        return instantiator.instantiateWindow(expression, input, metadata, executionContext);
    }

    public boolean isGroupBy(CharSequence name) {
        return binder.isGroupBy(name);
    }

    public BoundExpression remapColumns(BoundExpression expression, ProjectPlan projection) {
        return rewriter.remapColumns(expression, projection);
    }
}
