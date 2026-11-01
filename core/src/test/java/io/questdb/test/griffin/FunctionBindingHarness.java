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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.bind.FunctionBinder;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;

import java.io.Closeable;

/**
 * Binds, rewrites and instantiates expressions through one stand-alone {@link FunctionBinder} and its context's stages.
 */
public final class FunctionBindingHarness implements Closeable {
    private final FunctionBinder binder;
    private final SqlCompilerImpl compiler;
    private final FunctionInstantiator instantiator;
    private final BoundExpressionRewriter rewriter;

    public FunctionBindingHarness(CairoEngine engine, FunctionParser parser) {
        this.compiler = new SqlCompilerImpl(engine);
        try {
            this.binder = FunctionBinder.newStandalone(compiler, parser);
        } catch (Throwable th) {
            compiler.close();
            throw th;
        }
        this.instantiator = binder.getFunctionInstantiator();
        this.rewriter = binder.getExpressionRewriter();
    }

    public static ExpressionNode binary(String name, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.lhs = left;
        node.rhs = right;
        node.paramCount = 2;
        return node;
    }

    public static ExpressionNode call(String name, ObjList<ExpressionNode> arguments) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.paramCount = arguments.size();
        for (int i = arguments.size() - 1; i >= 0; i--) {
            node.args.add(arguments.getQuick(i));
        }
        return node;
    }

    public static ExpressionNode cast(ExpressionNode value, String type) {
        return binary("cast", value, constant(type));
    }

    public static ExpressionNode constant(String token) {
        return constant(token, 0);
    }

    public static ExpressionNode constant(String token, int position) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, token, 0, position);
    }

    public static ExpressionNode literal(String token) {
        return literal(token, 0);
    }

    public static ExpressionNode literal(String token, int position) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, token, 0, position);
    }

    public static GenericRecordMetadata metadata(int type, boolean isFull) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        if (isFull) {
            metadata.add(new TableColumnMetadata("unused", ColumnType.INT));
        }
        metadata.add(new TableColumnMetadata("physical", type, IndexType.NONE, 0, false, null));
        return metadata;
    }

    public static ExpressionNode parameter(String token) {
        return parameter(token, 0);
    }

    public static ExpressionNode parameter(String token, int position) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.BIND_VARIABLE, token, 0, position);
    }

    public static FunctionParser parser(CairoEngine engine, ObjList<Function> constructed) {
        final CairoConfiguration configuration = engine.getConfiguration();
        return new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
            @Override
            public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                           ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                final Function function = super.createFunction(overload, position, name, args, positions, context);
                constructed.add(function);
                return function;
            }
        });
    }

    public static Record record(int expectedIndex, long value) {
        return new Record() {
            @Override
            public long getTimestamp(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return value;
            }
        };
    }

    public static Record record(int expectedIndex, String value) {
        final Utf8String bytes = value != null ? new Utf8String(value) : null;
        return new Record() {
            @Override
            public Utf8Sequence getVarcharA(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return bytes;
            }

            @Override
            public Utf8Sequence getVarcharB(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return bytes;
            }

            @Override
            public int getVarcharSize(int columnIndex) {
                Assert.assertEquals(expectedIndex, columnIndex);
                return bytes != null ? bytes.size() : TableUtils.NULL_LEN;
            }
        };
    }

    public static OutputSchema schema(RecordMetadata metadata, int firstId) {
        final OutputSchema schema = new OutputSchema();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            schema.add(firstId + i, metadata.getColumnName(i), metadata.getColumnType(i), true);
            schema.setSymbolTableStatic(i, metadata.isSymbolTableStatic(i));
        }
        return schema;
    }

    public static ExpressionNode unary(String name, ExpressionNode argument) {
        final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.rhs = argument;
        node.paramCount = 1;
        return node;
    }

    public static OutputSchema wideSchema(int type) {
        final OutputSchema schema = new OutputSchema();
        for (int i = 0; i < 48; i++) {
            schema.add(i, "unused" + i, ColumnType.INT, true);
        }
        return schema.add(70, "value", type, true);
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
        try {
            binder.clearExpressions();
        } finally {
            compiler.close();
        }
    }

    public FunctionExpression commuteEquality(FunctionExpression original) {
        return rewriter.commuteEquality(original);
    }

    public BoundExpression copyRemappedColumns(BoundExpression expression, ProjectPlan projection) {
        return rewriter.copyRemappedColumns(expression, projection);
    }

    public BoundExpressionRewriter getRewriter() {
        return rewriter;
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
