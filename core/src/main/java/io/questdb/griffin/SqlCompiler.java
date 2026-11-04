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
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public interface SqlCompiler extends QuietCloseable, Mutable {

    /**
     * Adds to the dependency map of a view the columns that the reads of the view read and the plan of the view's
     * own SELECT does not, see {@link SqlUtil.ViewWildcardSources#hasReadPlannedOtherwise()}. The plan of the
     * view's own SELECT reads only the columns of a sub-query, CTE or view the view selects, and those its timestamp
     * and its order need. A read that names only outputs of the view whose columns do not need such a sub-query
     * gives it no columns to keep, when only the SELECT lists above the sub-query give it columns: the SELECT
     * joins it without a key, such as with a CROSS JOIN, or an ASOF, LT or SPLICE JOIN without ON, or selects from
     * it alone. The plan of such a read computes every column of the sub-query, as the plan of
     * {@code SELECT n FROM v} does for a view {@code SELECT count() n, sum(a) s FROM (SELECT * FROM x)}. A read
     * that counts the rows of the view drops the ORDER BY of a sub-query, and a temporal join or a SAMPLE BY that
     * took its timestamp from it takes the timestamp of the source of the sub-query instead. This compiles the
     * SELECT again, as a sub-query of a read that counts its rows, with every sub-query some read leaves without
     * columns kept whole, and collects what that plan reads, including a {@code *} for each wildcard it keeps
     * whole. It adds those columns marked, see {@link SqlUtil#addOtherReadDependencies}: the view lends them only to
     * a read that sets none of its OVERRIDABLE variables, which could otherwise read any of them through the view.
     * Call it on a compiler of its own after the SELECT of the view is optimised and before its plan is
     * generated, with what that compile recorded: this takes the values of the PIVOT IN sub-queries from it
     * rather than running them again, and finds a table out of date when it scans one of another id than that
     * compile.
     *
     * @param viewSql          the SELECT of the view
     * @param executionContext the context to compile the SELECT in
     * @param dependencies     the dependency map of the view, which this adds to
     * @param compileRecord    what the compile that optimised the SELECT of the view to plan it recorded
     */
    void collectOtherReadDependencies(
            CharSequence viewSql,
            SqlExecutionContext executionContext,
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies,
            SqlUtil.ViewCompileRecord compileRecord
    ) throws SqlException;

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

    /**
     * Generates the plan of a query model. When a table the plan reads is out of date, this
     * method recompiles the model from this compiler's own SQL text and tries again, so the model
     * must come from the statement this compiler is compiling. For any other model, use
     * {@link #generateSelectWithoutRetries}.
     */
    RecordCursorFactory generateSelectWithRetries(
            @Transient IQueryModel queryModel,
            @Nullable @Transient InsertModel insertModel,
            @Transient SqlExecutionContext executionContext,
            boolean generateProgressLogger
    ) throws SqlException;

    /**
     * Generates the plan of a query model once. When a table the plan reads is out of date, this
     * method throws {@link io.questdb.cairo.sql.TableReferenceOutOfDateException} and leaves the
     * retry to the caller. Use it for a model this compiler did not parse, such as a sub-query
     * another compiler's optimiser produced: {@link #generateSelectWithRetries} would replace that
     * model with whatever statement this compiler parsed last.
     */
    RecordCursorFactory generateSelectWithoutRetries(
            @Transient IQueryModel queryModel,
            @Transient SqlExecutionContext executionContext,
            boolean generateProgressLogger
    ) throws SqlException;

    /**
     * Parses and optimises the SELECT of a view, like {@link #generateExecutionModel}, and collects its
     * dependencies: the objects it names, see {@link SqlUtil#collectTableAndColumnReferences}, and the
     * columns it reads from each table, see {@link SqlUtil#collectTableColumnReads}, those of the reads of
     * the view the plan of the SELECT does not read included, see {@link #collectOtherReadDependencies}.
     *
     * @param viewSql          the SELECT of the view
     * @param executionContext the context to compile the SELECT in
     * @param dependencies     the dependency map of the view, which this adds to
     * @return the optimised model of the SELECT
     */
    ExecutionModel generateViewExecutionModel(
            CharSequence viewSql,
            SqlExecutionContext executionContext,
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies
    ) throws SqlException;

    BytecodeAssembler getAsm();

    CairoEngine getEngine();

    QueryBuilder query();

    @TestOnly
    void setEnableJitNullChecks(boolean value);

    @TestOnly
    void setFullFatJoins(boolean fullFatJoins);

    @TestOnly
    ExpressionNode testParseExpression(CharSequence expression, IQueryModel model) throws SqlException;

    @TestOnly
    void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException;
}
