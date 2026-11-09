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
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.InsertModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public interface SqlCompiler extends QuietCloseable, Mutable {

    /**
     * Returns the SQL of a materialized view with the explicit column names that CREATE MATERIALIZED VIEW
     * requires, for SHOW CREATE: each generated column name aliased to the view column it feeds, and each bare
     * zero-argument function name that the view's refresh reads as a call written with parentheses. Returns null
     * when the SQL needs no change, and when it no longer compiles or the result would not reproduce the view's
     * columns, so the caller shows the SQL as stored.
     *
     * @param viewDefinition   the definition of the view
     * @param viewColumnNames  the names of the view's columns, in the order the view stores them
     * @param executionContext the context to compile the SQL with, which the method leaves as it found it
     */
    @Nullable
    String addExplicitColumnNames(
            MatViewDefinition viewDefinition,
            ObjList<CharSequence> viewColumnNames,
            SqlExecutionContext executionContext
    );

    /**
     * Adds the bare select-list names of a materialized view's own SQL text to the sink: the unqualified column
     * references in the select lists of the text, which the view's refresh reads as calls to the zero-argument
     * functions of the same names when no column of that name exists. The models of a regular view that the SQL
     * reads are not part of the text, and the method skips them. It only parses the SQL.
     *
     * @param viewDefinition   the definition of the view
     * @param executionContext the context to parse the SQL with, which the method leaves as it found it
     * @param sink             receives the names
     * @throws SqlException when the SQL does not parse
     */
    void collectMatViewBareSelectNames(
            MatViewDefinition viewDefinition,
            SqlExecutionContext executionContext,
            LowerCaseCharSequenceHashSet sink
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

    /**
     * Closes the table-name-function factories the compiler attached to its query models and never
     * transferred to a returned {@link RecordCursorFactory}. Code generation empties the model's slot
     * as it takes ownership, so a factory the caller still holds is invisible here.
     * <p>
     * This is the narrow alternative to {@link #clear()} for a lifecycle boundary: it releases what
     * the compiler still owns without dropping the SQL text or the flyweight {@link CompiledQuery} a
     * caller may still be reading. It reports a close failure rather than throwing, so a caller can
     * always finish returning or disposing of the compiler.
     */
    void freeUntransferredTableNameFunctions();

    /**
     * Returns the upper bound T, in the unit of the designated timestamp column, when {@code predicate} is
     * {@code <ts> < T} or {@code <ts> <= T} on that column and T references no column. T lets the row-expiry
     * cleanup classify a whole partition from its bounds, with no survivor scan. A shape that does not match,
     * or an evaluation problem, gives {@link io.questdb.std.Numbers#LONG_NULL}, and the caller then scans.
     */
    long expiryTimestampThreshold(
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            CharSequence predicate,
            CharSequence timestampColumn
    );

    /**
     * Returns true when the background cleanup job frees disk space for the EXPIRE ROWS policy
     * {@code predicate}, i.e. when physical deletion is safe: a row it classifies as expired now can never
     * re-enter the keep-set. Eligible scalar {@code WHEN} predicates qualify when they are clock-free or
     * reduce to a designated-timestamp threshold ({@code ts < now()} / {@code ts <= T}). Structural KEEP and
     * raw window policies do not: a later materialized-view refresh can remove a winner and reveal an older
     * fallback that cleanup had physically deleted. A scalar predicate that references a non-deterministic clock in a
     * non-threshold position (e.g. {@code ts > now()}) is also not monotonic because it can un-expire rows as
     * time advances.
     * <p>
     * The check is conservative — any doubt (parse/bind issue, a non-deterministic function it cannot prove
     * monotonic) returns false, so cleanup is SKIPPED and the authoritative read filter alone enforces
     * retention. Disk is not reclaimed for such a policy, but query results stay correct. {@code metadata} is
     * used to bind scalar predicates.
     */
    boolean isExpiryCleanupReclaiming(
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            CharSequence predicate
    );

    ExecutionModel generateExecutionModel(CharSequence sqlText, SqlExecutionContext executionContext) throws SqlException;

    /**
     * Parses and optimises the SELECT of a view like {@link #generateExecutionModel}, and collects the
     * tables and columns it reads into {@code outDependencies}, replacing what the map held. The set is
     * the one CREATE VIEW records, so a caller that stores a view definition grants the view the same
     * authority CREATE VIEW would.
     */
    ExecutionModel generateViewExecutionModel(
            CharSequence viewSql,
            SqlExecutionContext executionContext,
            LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> outDependencies
    ) throws SqlException;

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

    BytecodeAssembler getAsm();

    CairoEngine getEngine();

    QueryBuilder query();

    /**
     * Validates an EXPIRE ROWS predicate structurally by parsing and binding it against {@code metadata}
     * (the columns the object will have) and checking the result is a boolean expression, without touching
     * any table. Used by CREATE TABLE / CREATE MATERIALIZED VIEW to reject a bad predicate before the
     * object is created. Any parse/bind error is rewritten as a clear SqlException at {@code position}.
     */
    ExpiryValidationResult validateExpiryPredicateOnMetadata(
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            CharSequence predicate,
            int position
    ) throws SqlException;

    /**
     * Validates the SQL of a materialized view under the caller's context, for {@link MatViewSqlValidator}: parses
     * it and, with {@code isCompiling} set, compiles it as the view's refresh reads it, then reports to the
     * validator what it found. A SQL that does not parse or compile reports its error; the method throws only an
     * authorization error, a cancellation or a timeout.
     *
     * @param viewDefinition              the definition of the view
     * @param isCompiling                 false to only parse the SQL
     * @param isReadingBareNamesAsColumns true to read every bare name as a column, as CREATE does, and fail a bare
     *                                    name of a zero-argument function with the parentheses the call needs;
     *                                    false to read bare names as the view's refresh does
     * @param executionContext            the context to compile the SQL with, which the method leaves as it found it
     * @param validator                   receives the outcome
     */
    void validateMatViewSql(
            MatViewDefinition viewDefinition,
            boolean isCompiling,
            boolean isReadingBareNamesAsColumns,
            SqlExecutionContext executionContext,
            MatViewSqlValidator validator
    ) throws SqlException;

    @TestOnly
    void setEnableJitNullChecks(boolean value);

    @TestOnly
    void setFullFatJoins(boolean fullFatJoins);

    @TestOnly
    ExpressionNode testParseExpression(CharSequence expression, IQueryModel model) throws SqlException;

    @TestOnly
    void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException;
}
