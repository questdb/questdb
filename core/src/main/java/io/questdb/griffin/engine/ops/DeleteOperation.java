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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.tasks.TableWriterTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.tasks.TableWriterTask.CMD_DELETE_TABLE;

public class DeleteOperation extends AbstractOperation {
    public static final String MAT_VIEW_INVALIDATION_REASON = "delete operation";
    // Names of the two NAMED timestamp bind variables that SqlCompilerImpl.generateDelete ANDs onto the
    // apply-time survivor scan (WHERE NOT(pred)) as "<designatedTs> >= :__del_win_lo AND <designatedTs> <
    // :__del_win_hi" (lower bound inclusive, upper bound exclusive). They bound the survivor cursor to a
    // per-window designated-timestamp interval [lo, hi) so OperationExecutor can re-drive the SAME survivor
    // factory window by window (rebinding these two variables and re-running getCursor), each pass reading only
    // the window's partitions via an interval scan instead of re-scanning the whole table. Compiled with
    // (min-non-null, MAX) defaults, so an un-windowed caller gets the whole-range survivor set. Names are WITHOUT
    // the leading ':' (the bind-variable service key form); the survivor model's AST carries the ':'-prefixed
    // literal. Used by the WAL-apply executor (Task 5).
    public static final String WINDOW_HI_BIND = "__del_win_hi";
    public static final String WINDOW_LO_BIND = "__del_win_lo";
    // Time-range fast-path classification (Task 2.1), computed by SqlCompilerImpl.generateDelete from the
    // ORIGINAL (un-negated) predicate. When pureTimeRange is true, the whole DELETE predicate reduces to a
    // SINGLE designated-timestamp interval [timeRangeLo, timeRangeHiExcl) with no residual non-timestamp
    // filter, so OperationExecutor.executeDelete applies it as one empty replaceRange over that interval
    // instead of staging survivors. When false, executeDelete falls back to the whole-range survivor-replace
    // (always correct). Bounds are in the table's designated-timestamp units (micros or nanos); an open lower
    // bound is Long.MIN_VALUE and an open upper bound saturates timeRangeHiExcl at Long.MAX_VALUE.
    private final boolean pureTimeRange;
    private final long timeRangeHiExcl;
    private final long timeRangeLo;
    private RecordCursorFactory survivorFactory;

    public DeleteOperation(
            @NotNull TableToken tableToken,
            int tableId,
            long tableVersion,
            int tableNamePosition,
            @Nullable RecordCursorFactory survivorFactory,
            boolean pureTimeRange,
            long timeRangeLo,
            long timeRangeHiExcl
    ) {
        init(CMD_DELETE_TABLE, TableWriterTask.getCommandName(CMD_DELETE_TABLE), tableToken, tableId, tableVersion, tableNamePosition);
        this.survivorFactory = survivorFactory;
        this.pureTimeRange = pureTimeRange;
        this.timeRangeLo = timeRangeLo;
        this.timeRangeHiExcl = timeRangeHiExcl;
    }

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        // v1 supports WAL tables only; the WAL-apply path uses OperationExecutor.executeDelete,
        // not this method. A direct (non-WAL) apply is rejected at compile time, so reaching here
        // is a programming error.
        throw CairoException.nonCritical()
                .put("DELETE is only supported on WAL tables [table=")
                .put(getTableToken().getTableName())
                .put(']');
    }

    @Override
    public void authorize() {
        final SecurityContext securityContext = this.securityContext;
        if (securityContext == null) {
            throw CairoException.nonCritical()
                    .put("delete security context is empty [table=")
                    .put(getTableToken().getTableName())
                    .put(']');
        }
        securityContext.authorizeTableDelete(getTableToken());
    }

    @Override
    public void close() {
        survivorFactory = Misc.free(survivorFactory);
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        return task.getAsyncWriterCommand();
    }

    public RecordCursorFactory getSurvivorFactory() {
        return survivorFactory;
    }

    /**
     * Exclusive upper bound of the deleted designated-timestamp interval; only meaningful when
     * {@link #isPureTimeRange()} is true. Saturates at {@code Long.MAX_VALUE} for an open upper bound.
     */
    public long getTimeRangeHiExcl() {
        return timeRangeHiExcl;
    }

    /**
     * Inclusive lower bound of the deleted designated-timestamp interval; only meaningful when
     * {@link #isPureTimeRange()} is true. {@code Long.MIN_VALUE} for an open lower bound.
     */
    public long getTimeRangeLo() {
        return timeRangeLo;
    }

    /**
     * True when the whole DELETE predicate reduces to a single designated-timestamp interval
     * {@code [getTimeRangeLo(), getTimeRangeHiExcl())} with no residual non-timestamp filter, so it can be
     * applied as one empty {@code replaceRange} over the deleted interval (Task 2.1).
     */
    public boolean isPureTimeRange() {
        return pureTimeRange;
    }

    @Override
    public boolean isStructural() {
        return false;
    }

    @Override
    public String matViewInvalidationReason() {
        return MAT_VIEW_INVALIDATION_REASON;
    }

    @Override
    public void serialize(TableWriterTask task) {
        super.serialize(task);
        task.setAsyncWriterCommand(this);
    }

    // Sets a DELETE window-bound bind variable (WINDOW_LO_BIND / WINDOW_HI_BIND) in the designated-timestamp
    // column's OWN unit, so the runtime interval bound is interpreted without a micros<->nanos rescale. A
    // micros-typed bind variable (BindVariableService.setTimestamp) evaluated against a TIMESTAMP_NANO
    // designated column is rescaled x1000 by NanosTimestampDriver.from(value, TIMESTAMP_MICRO) -
    // Long.MIN_VALUE+1 / Long.MAX_VALUE (the compiled-in whole-range defaults) then overflow
    // Math.multiplyExact, the survivor factory's getCursor throws ImplicitCastException, and the WAL apply
    // job SUSPENDS the table instead of deleting anything (micros tables are unaffected: the driver call is a
    // no-op rescale). This is the ONE place that sets these bind variables, shared by
    // SqlCompilerImpl.generateDelete (compile-time whole-range defaults) and OperationExecutor (Task 5,
    // per-window rebinds), so both paths stay unit-correct together.
    public static void setWindowBound(BindVariableService bindVariableService, CharSequence name, int timestampColumnType, long value) throws SqlException {
        if (ColumnType.isTimestampNano(timestampColumnType)) {
            bindVariableService.setTimestampNano(name, value);
        } else {
            bindVariableService.setTimestamp(name, value);
        }
    }

    @Override
    public void startAsync() {
        // DeleteOperation is WAL-only; async execution is handled by OperationExecutor.
        // This is a no-op in v1.
    }
}
