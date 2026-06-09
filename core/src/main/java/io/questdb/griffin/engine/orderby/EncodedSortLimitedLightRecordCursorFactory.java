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
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
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
    private final RecordCursorFactory base;
    private final CairoConfiguration configuration;
    private final EncodedSortLimitedLightRecordCursor genericCursor;
    private final Function hiFunction;
    private final Function loFunction;
    private final EncodedSortLimitedPartiallySortedLightRecordCursor partiallySortedCursor;
    private final ListColumnFilter sortColumnFilter;
    private final int timestampIndex;
    private DelegatingRecordCursor activeCursor;
    private EncodedSortLightRecordCursor fallbackCursor;
    private boolean isFirstN;
    private long limit;
    private long skipFirst;
    private long skipLast;

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
        this.base = base;
        this.configuration = configuration;
        this.loFunction = loFunction;
        this.hiFunction = hiFunction;
        this.sortColumnFilter = sortColumnFilter;
        this.timestampIndex = timestampIndex;
        EncodedSortLimitedLightRecordCursor generic = null;
        EncodedSortLimitedPartiallySortedLightRecordCursor partial = null;
        try {
            generic = new EncodedSortLimitedLightRecordCursor(configuration, metadata, sortColumnFilter);
            if (timestampIndex != -1) {
                partial = new EncodedSortLimitedPartiallySortedLightRecordCursor(configuration, metadata, sortColumnFilter, timestampIndex);
            }
        } catch (Throwable th) {
            Misc.free(generic);
            Misc.free(partial);
            close();
            throw th;
        }
        this.genericCursor = generic;
        this.partiallySortedCursor = partial;
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
            baseCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            activeCursor.of(baseCursor, executionContext);
            return activeCursor;
        } catch (Throwable th) {
            Misc.free(activeCursor);
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
        sink.type("Sort light");
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

    private boolean canBeOptimized() {
        return !(loFunction.getLong(null) >= 0 && hiFunction != null && hiFunction.getLong(null) < 0);
    }

    private void computeLimits() {
        this.skipFirst = 0;
        this.skipLast = 0;
        this.limit = 0;
        this.isFirstN = false;

        final long lo = loFunction.getLong(null);
        if (lo < 0 && hiFunction == null) {
            this.limit = -lo;
            return;
        }
        if (lo > -1 && hiFunction == null) {
            this.isFirstN = true;
            this.limit = lo;
            return;
        }
        final long hi = hiFunction.getLong(null);
        if (lo < 0) {
            if (lo == hi) {
                this.limit = 0;
            } else {
                this.limit = -Math.min(hi, lo);
                this.skipLast = Math.max(-Math.max(hi, lo), 0);
            }
            return;
        }
        if (hi < 0) {
            this.limit = -1;
            this.skipFirst = lo;
            this.skipLast = -hi;
            return;
        }
        this.isFirstN = true;
        this.limit = Math.max(hi, lo);
        this.skipFirst = Math.min(hi, lo);
    }

    private void initialize(SqlExecutionContext executionContext, RecordCursor baseCursor) throws SqlException {
        loFunction.init(baseCursor, executionContext);
        if (hiFunction != null) {
            hiFunction.init(baseCursor, executionContext);
        }
        computeLimits();
        if (canBeOptimized()) {
            final EncodedSortLimitedLightRecordCursor next = (timestampIndex != -1 && isFirstN)
                    ? partiallySortedCursor
                    : genericCursor;
            next.setSelection(isFirstN, limit, skipFirst, skipLast);
            activeCursor = next;
            return;
        }
        if (fallbackCursor == null) {
            fallbackCursor = new EncodedSortLightRecordCursor(configuration, getMetadata(), sortColumnFilter);
        }
        activeCursor = fallbackCursor;
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(genericCursor);
        Misc.free(partiallySortedCursor);
        Misc.free(fallbackCursor);
    }
}
