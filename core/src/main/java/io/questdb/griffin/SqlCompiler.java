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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.InsertModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public interface SqlCompiler extends QuietCloseable, Mutable {

    CompiledQuery compile(CharSequence sqlText, SqlExecutionContext ctx) throws SqlException;

    void compileBatch(CharSequence batchText, SqlExecutionContext sqlExecutionContext, BatchCallback batchCallback) throws Exception;

    /**
     * SPI for operation execution. Typical execution will rely on the compiler infrastructure, such paths, engine, configuration etc.
     * We use compiler to avoid cluttering the operation (which is immutable copy of user's request).
     *
     * @param op               the operation to execute
     * @param executionContext the context, required for logging and also for recompiling the operation's SQL text
     * @return true if the operation was performed, false if it was a no-op (e.g. IF EXISTS on a missing entity, or IF NOT EXISTS on an existing one)
     * @throws SqlException   in case of known, typically validation, errors
     * @throws CairoException in case of unexpected, typically runtime, errors
     */
    boolean execute(final Operation op, SqlExecutionContext executionContext) throws SqlException, CairoException;

    ExecutionModel generateExecutionModel(CharSequence sqlText, SqlExecutionContext executionContext) throws SqlException;

    /**
     * Parses a single expression text into an {@link ExpressionNode} tree without
     * running it through the optimiser. Useful when callers persist expression
     * fragments and need to recompile them later against a known
     * {@link io.questdb.cairo.sql.RecordMetadata}.
     */
    ExpressionNode parseExpression(CharSequence expression) throws SqlException;

    RecordCursorFactory generateSelectWithRetries(
            @Transient IQueryModel queryModel,
            @Nullable @Transient InsertModel insertModel,
            @Transient SqlExecutionContext executionContext,
            boolean generateProgressLogger
    ) throws SqlException;

    BytecodeAssembler getAsm();

    CairoEngine getEngine();

    QueryBuilder query();

    /**
     * Parses {@code expression} into a bare {@link ExpressionNode}: a pure syntax parse -- no query
     * optimizer, no table/column resolution. {@code model} is only consulted by the parser for the
     * rare expression shapes that need one (e.g. a lambda/sub-query) and may be {@code null} for an
     * ordinary scalar expression. Unlike a full {@code SELECT ... FROM <table>} compile-and-optimize
     * (which resolves columns through that query's own {@code RecordMetadata}), this does no column
     * resolution at all -- the caller is free to bind the returned node against any {@code
     * RecordMetadata} it chooses afterwards (e.g. via {@link FunctionParser}), so the result is safe to
     * use outside a test context whenever production code needs to turn expression TEXT back into an
     * {@link ExpressionNode} without forcing a full query compile.
     */
    ExpressionNode parseStandaloneExpression(CharSequence expression, IQueryModel model) throws SqlException;

    @TestOnly
    void setEnableJitNullChecks(boolean value);

    @TestOnly
    void setFullFatJoins(boolean fullFatJoins);

    @TestOnly
    ExpressionNode testParseExpression(CharSequence expression, IQueryModel model) throws SqlException;

    @TestOnly
    void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException;
}
