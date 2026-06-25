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

    /**
     * Returns the epoch-micros upper bound T when {@code predicate} is {@code <ts> < T} / {@code <ts> <= T}
     * on the (micros) designated timestamp with a column-free T, else {@link io.questdb.std.Numbers#LONG_NULL}.
     * Lets the row-expiry cleanup classify whole partitions by their bounds without a survivor scan; any
     * non-matching shape or evaluation issue returns LONG_NULL so the caller falls back to the scan.
     */
    long expiryTimestampThresholdMicros(
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            CharSequence predicate,
            CharSequence timestampColumn
    );

    /**
     * Returns true if {@code predicate} (a stored EXPIRE ROWS predicate) references the column at
     * {@code columnIndex} in {@code metadata}. Used to reject DROP / RENAME / ALTER COLUMN TYPE on a column
     * the predicate depends on: such a change would leave the stored predicate referencing a missing,
     * renamed or retyped column and brick every read of the table (the predicate is re-parsed per read).
     * Column names in the predicate are resolved via {@code metadata} so case and quoting match the
     * predicate's original bind. Returns true (conservative — block the change) if the stored predicate no
     * longer parses, so a corrupt predicate can never silently allow a bricking column op.
     */
    boolean expiryPredicateReferencesColumn(
            RecordMetadata metadata,
            CharSequence predicate,
            int columnIndex
    );

    /**
     * Returns true when the EXPIRE ROWS policy {@code predicate} is <b>monotonic</b>, i.e. safe for the
     * background cleanup job to physically reclaim: a row it classifies as expired now can never re-enter the
     * keep-set. Monotonic by construction: the relative modes (KEEP LATEST / KEEP HIGHEST/LOWEST / KEEP N) and
     * any scalar/window {@code WHEN} predicate that is either clock-free (mat-view rows are immutable, so a
     * clock-free predicate's per-row value never changes) or reduces to a designated-timestamp threshold
     * ({@code ts < now()} / {@code ts <= T}). NOT monotonic: a predicate that references a non-deterministic
     * clock in a non-threshold position (e.g. {@code ts > now()}), which un-expires rows as time advances.
     * <p>
     * The check is conservative — any doubt (parse/bind issue, a non-deterministic function it cannot prove
     * monotonic) returns false, so cleanup is SKIPPED and the authoritative read filter alone enforces
     * retention. Disk is not reclaimed for such a policy, but query results stay correct. {@code source} is the
     * FROM source used to validate a window predicate (a quoted table name, or a parenthesised defining SELECT
     * for a not-yet-created view); {@code metadata} is used to bind scalar predicates.
     */
    boolean isExpiryCleanupMonotonic(
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            CharSequence source,
            CharSequence predicate,
            CharSequence timestampColumn
    );

    ExecutionModel generateExecutionModel(CharSequence sqlText, SqlExecutionContext executionContext) throws SqlException;

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
     * Validates an EXPIRE ROWS predicate structurally by parsing and binding it against {@code metadata}
     * (the columns the object will have) and checking the result is a boolean expression, without touching
     * any table. Used by CREATE TABLE / CREATE MATERIALIZED VIEW to reject a bad predicate before the
     * object is created. Any parse/bind error is rewritten as a clear SqlException at {@code position}.
     */
    void validateExpiryPredicateOnMetadata(
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            CharSequence predicate,
            int position
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
