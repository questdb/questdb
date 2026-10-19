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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

/**
 * Encoded-key top-K replacement for {@link LimitedSizeSortedLightRecordCursorFactory}.
 * <p>
 * The build phase encodes each scanned row's sort key into a flat native
 * buffer and never calls {@code recordAt} on the base cursor, which is what
 * makes the consumer-side Parquet decode cache work-set independent of K.
 * The base record stays bound to whichever frame {@code hasNext()} advanced
 * into; comparisons happen as native byte compares on the encoded keys.
 * After the scan, {@link io.questdb.std.Vect#sortEncodedEntries} sorts the
 * full buffer and emit yields the requested slice. The partial-sort variant
 * bounds the scan via timestamp-group early stop, keeping the buffer size
 * close to {@code limit}.
 * <p>
 * Used only when {@link SortKeyEncoder#isSupported(RecordMetadata,
 * io.questdb.std.IntList)} accepts the sort key. The legacy tree-chain
 * factory remains the fallback for unsupported keys.
 */
public class EncodedSortLimitedLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final Function hiFunction;
    private final Function loFunction;
    private final ListColumnFilter sortColumnFilter;
    private final int timestampIndex;
    private RecordCursorFactory base;
    private EncodedSortLimitedLightRecordCursor cursor;

    public EncodedSortLimitedLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function loFunction,
            @Nullable Function hiFunction,
            ListColumnFilter sortColumnFilter,
            int timestampIndex
    ) {
        super(metadata);
        // The build phase never calls recordAt, but emit re-fetches the kept rows from the base.
        assert base.recordCursorSupportsRandomAccess();
        this.base = base;
        this.loFunction = loFunction;
        this.hiFunction = hiFunction;
        this.sortColumnFilter = sortColumnFilter;
        this.timestampIndex = timestampIndex;
        this.cursor = new EncodedSortLimitedLightRecordCursor(configuration, metadata, sortColumnFilter, timestampIndex);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            initialize(executionContext, baseCursor);
        } catch (Throwable th) {
            Misc.free(baseCursor);
            throw th;
        }

        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return SortedRecordCursorFactory.getScanDirection(sortColumnFilter);
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Encode sort light");
        sink.meta("lo").val(loFunction);
        if (hiFunction != null) {
            sink.meta("hi").val(hiFunction);
        }
        if (timestampIndex != -1) {
            sink.meta("partiallySorted").val(true);
        }
        SortedLightRecordCursorFactory.addSortKeys(sink, sortColumnFilter);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    /**
     * Re-reads the limit functions, derives the selection window, and binds it to
     * the cursor's selection. Runs on every execution so that bind-variable
     * limits re-bind on cached plans.
     */
    private void initialize(SqlExecutionContext executionContext, RecordCursor baseCursor) throws SqlException {
        loFunction.init(baseCursor, executionContext);
        if (hiFunction != null) {
            hiFunction.init(baseCursor, executionContext);
        }

        boolean isFirstN = false;
        long limit = 0;
        long skipFirst = 0;
        long skipLast = 0;

        final long rawLo = loFunction.getLong(null);
        if (hiFunction == null) {
            if (rawLo < 0) {
                // Last N rows. A NULL lo (Numbers.LONG_NULL) lands here too: -lo stays
                // negative and setSelection() maps it to unbounded, returning the full
                // sorted set like LimitRecordCursor does.
                limit = -rawLo;
            } else {
                // First N rows.
                isFirstN = true;
                limit = rawLo;
            }
        } else {
            final long hi = hiFunction.getLong(null);
            // A NULL lo with a non-NULL hi means "from the start": LIMIT null,3 returns
            // the first 3 rows. Without this, NULL falls into the lo < 0 branch and the
            // query returns the full set instead of the head slice. A NULL lo with a
            // NULL hi must stay raw so the lo == hi check below yields the empty result.
            final long lo = rawLo == Numbers.LONG_NULL && hi != Numbers.LONG_NULL ? 0 : rawLo;
            if (lo < 0) {
                // Negative range, e.g. -10,-5: five rows ending five rows from the
                // tail. lo == hi is an invalid bottom range, e.g. -3,-3: empty result.
                if (lo != hi) {
                    limit = -Math.min(hi, lo);
                    skipLast = Math.max(-Math.max(hi, lo), 0);
                }
            } else if (hi < 0) {
                // lo >= 0, hi < 0: from lo up to -hi rows before the end. The result
                // size cannot be estimated, so the scan is unbounded (setSelection
                // maps the negative limit) and the emit slice applies both skips;
                // isFirstN stays false, so the generic cursor is always selected.
                // A NULL hi produces the empty result, matching the legacy chain
                // and LimitRecordCursor.
                if (hi != Numbers.LONG_NULL) {
                    limit = -1;
                    skipFirst = lo;
                    skipLast = -hi;
                }
            } else {
                // Both non-negative: rows lo..hi.
                isFirstN = true;
                limit = Math.max(hi, lo);
                skipFirst = Math.min(hi, lo);
            }
        }

        cursor.setSelection(isFirstN, limit, skipFirst, skipLast);
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final EncodedSortLimitedLightRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }
}
