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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
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

    void freeResourcesInFlight();

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
     * Compiles the SELECT of a view into its factory, for the job that checks the view's state and metadata: like
     * {@link #generateExecutionModel}, it raises no compile-view events, so checking a view queues no check of the
     * views it reads. The planner the context selects compiles it.
     */
    RecordCursorFactory generateViewFactory(CharSequence viewSql, SqlExecutionContext executionContext) throws SqlException;

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
     * Generates the query this compiler prepared last, which {@code queryModel} is the model of: the statement
     * {@link #generateExecutionModel} or {@code compile} returned. Recompiles the statement when a table it reads
     * changed since it was prepared.
     */
    RecordCursorFactory generateSelectWithRetries(
            @Transient QueryModel queryModel,
            @Transient SqlExecutionContext executionContext,
            boolean generateProgressLogger
    ) throws SqlException;

    BytecodeAssembler getAsm();

    CairoEngine getEngine();

    QueryBuilder query();

    /**
     * Brings every structure the compiler keeps between statements back to its configured floor, as if the compiler
     * had cleared through two windows of empty statements, see {@link io.questdb.std.RetainedCapacity}. The compiler
     * pool calls it on a compiler that has been idle past the pool's inactive TTL, which no statement ages.
     */
    void releaseRetainedCapacity();

    /**
     * Validates and classifies the scalar EXPIRE ROWS predicate of a materialized view, binding it against
     * {@code viewMetadata}, the columns of the view, without touching any table. The predicate has to be one boolean
     * expression consuming the whole text, with no root-level aggregate and no bind variables, whose cleanup-sweep
     * keep-filter binds and whose ordering threshold on the designated timestamp is not a constant NULL. Any error is
     * rewritten as a clear SqlException at {@code position}.
     * <p>
     * A CREATE or ALTER gives {@code viewName}, the name of the view: the keep-filter of the policy then binds exactly
     * as every read of the view binds it, with qualified names resolved against the view's name, so a policy this
     * accepts binds in every read. At CREATE the view does not exist yet, and {@code viewMetadata} lists the columns
     * it is created with. The cleanup job and {@code materialized_views()} classify a stored policy with a null
     * {@code viewName}: a policy an older version stored may fail the read's rules, and refusing to classify it would
     * change what they do with it.
     */
    ExpiryValidationResult validateExpiryPolicy(
            SqlExecutionContext executionContext,
            @Nullable CharSequence viewName,
            RecordMetadata viewMetadata,
            CharSequence predicate,
            int position
    ) throws SqlException;

    @TestOnly
    void setEnableJitNullChecks(boolean value);

    @TestOnly
    void setFullFatJoins(boolean fullFatJoins);

    @TestOnly
    ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException;

    @TestOnly
    void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException;
}
