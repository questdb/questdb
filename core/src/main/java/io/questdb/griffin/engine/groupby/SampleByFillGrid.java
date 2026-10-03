/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\__,_|\___||___/\__|____/|____/
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.NotNull;

/**
 * The bucket grid a SAMPLE BY fill steps over: FROM/TO bounds, OFFSET and TIME ZONE
 * resolved per query, and the first bucket anchored the way timestamp_floor_utc floors.
 */
final class SampleByFillGrid {
    // Unwrapped uniform-UTC sampler. timestampSampler may point here or at
    // tzWrap; held separately so the wrap can be rebuilt per of().
    private final TimestampSampler baseSampler;
    private final Function fromFunc;
    private final Function offsetFunc;
    private final int offsetFuncPos;
    // FILL stride unit ('d','w','M','y'), forwarded to the TZ wrap so the
    // local-grid floor uses the right calendar resolution.
    private final char samplingIntervalUnit;
    private final TimestampDriver timestampDriver;
    private final Function toFunc;
    private final int toFuncPos;
    // Runtime-constant TIME ZONE Function (null when no TZ clause). Re-read
    // per of() so a bind variable picks up its current value -- pre-resolving
    // at compile time would silently bake the first-execute value.
    private final Function tzFunc;
    private final int tzFuncPos;
    private long calendarOffset;
    private long fromTs;
    private boolean hasExplicitTo;
    private long maxTimestamp;
    // Active sampler. Points at baseSampler or tzWrap; re-bound per of()
    // so a runtime-constant TIME ZONE picks up its current value.
    private TimestampSampler timestampSampler;
    // Lazily-allocated TZ wrap around baseSampler. Reused across of() calls
    // via setTzRules; held even after a fixed-offset of() for the next bind.
    private TimezoneFloorTimestampSampler tzWrap;

    SampleByFillGrid(
            TimestampSampler timestampSampler,
            int timestampType,
            @NotNull Function fromFunc,
            @NotNull Function toFunc,
            int toFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function tzFunc,
            int tzFuncPos,
            char samplingIntervalUnit
    ) {
        this.baseSampler = timestampSampler;
        this.timestampSampler = timestampSampler;
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        this.fromFunc = fromFunc;
        this.toFunc = toFunc;
        this.toFuncPos = toFuncPos;
        this.offsetFunc = offsetFunc;
        this.offsetFuncPos = offsetFuncPos;
        this.tzFunc = tzFunc;
        this.tzFuncPos = tzFuncPos;
        this.samplingIntervalUnit = samplingIntervalUnit;
    }

    // Data row before the current bucket boundary -- upstream contract
    // violation or bucket-grid drift (DST, FROM/offset misalignment). Fail
    // visibly rather than silently corrupting output.
    static CairoException dataRowBeforeBucket(long dataTs, long bucketTimestamp) {
        return CairoException.critical(0)
                .put("sample by fill: data row timestamp ")
                .put(dataTs)
                .put(" precedes next bucket ")
                .put(bucketTimestamp);
    }

    /**
     * Anchors the grid at the first data row and returns its bucket; without TO the
     * grid is unbounded.
     */
    long firstBucket(long firstTs) {
        final boolean currentBucketIsFirstTs = (fromTs == Numbers.LONG_NULL || firstTs < fromTs);
        long bucket = currentBucketIsFirstTs ? firstTs : fromTs;
        if (calendarOffset != 0 && fromTs == Numbers.LONG_NULL) {
            // No FROM but offset exists: align grid to offset so round()
            // matches timestamp_floor_utc buckets.
            timestampSampler.setOffset(calendarOffset);
            bucket = timestampSampler.round(bucket);
        } else if (calendarOffset != 0 && currentBucketIsFirstTs) {
            // firstTs already sits on the floor grid (anchored at
            // fromTs+calendarOffset). setLocalAnchor forwards untranslated
            // because fromTs+calendarOffset is local-grid space (matches
            // timestamp_floor_utc's raw-modulus treatment).
            timestampSampler.setLocalAnchor(fromTs + calendarOffset);
        } else {
            // firstTs path (calendarOffset == 0) OR fromTs path (any offset).
            // Anchor at effectiveOffset = bucket + calendarOffset to match
            // timestamp_floor_utc's grid.
            //
            // Math.max clamps a positive-offset case where effectiveOffset
            // > seed: GROUP BY's Micros.floor* clamps up; round() doesn't.
            //
            // setStart vs setLocalAnchor tracks the origin of the bucket:
            //  - firstTs path: a GROUP BY bucket label on the local grid;
            //    setStart applies UTC->local conversion. (Here calendarOffset
            //    is 0, so effectiveOffset == firstTs.)
            //  - fromTs path: a raw user FROM in local-grid space;
            //    setLocalAnchor forwards untranslated, and localAnchorAsUtc
            //    lifts back to UTC for the Math.max comparison.
            //  Using setStart on the fromTs path would shift the grid by
            //  tzOffset and trip the grid-drift guard on super-day strides.
            final long effectiveOffset = bucket + calendarOffset;
            final long anchorUtc;
            if (currentBucketIsFirstTs) {
                timestampSampler.setStart(effectiveOffset);
                anchorUtc = effectiveOffset;
            } else {
                timestampSampler.setLocalAnchor(effectiveOffset);
                anchorUtc = timestampSampler.localAnchorAsUtc(effectiveOffset);
            }
            bucket = Math.max(anchorUtc, timestampSampler.round(bucket));
        }
        if (maxTimestamp == Numbers.LONG_NULL) {
            maxTimestamp = Long.MAX_VALUE;
        }
        return bucket;
    }

    /**
     * Returns the first bucket of an input without rows: FROM when both FROM and TO
     * bound the grid, otherwise Long.MAX_VALUE with an empty grid.
     */
    long firstBucketWithoutRows() {
        if (fromTs != Numbers.LONG_NULL && maxTimestamp != Numbers.LONG_NULL) {
            // Same anchor rule as firstBucket(), fromTs path only (no firstTs).
            // effectiveOffset is local-grid space; setLocalAnchor forwards
            // untranslated and localAnchorAsUtc lifts back so Math.max clamps in UTC.
            final long effectiveOffset = fromTs + calendarOffset;
            timestampSampler.setLocalAnchor(effectiveOffset);
            final long anchorUtc = timestampSampler.localAnchorAsUtc(effectiveOffset);
            return Math.max(anchorUtc, timestampSampler.round(fromTs));
        }
        maxTimestamp = Long.MIN_VALUE;
        return Long.MAX_VALUE;
    }

    long getMaxTimestamp() {
        return maxTimestamp;
    }

    boolean hasExplicitTo() {
        return hasExplicitTo;
    }

    long nextBucket(long bucketTimestamp) {
        return timestampSampler.nextTimestamp(bucketTimestamp);
    }

    void of(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        fromFunc.init(symbolTableSource, executionContext);
        toFunc.init(symbolTableSource, executionContext);
        // Reject FROM > TO at the same point as HORIZON JOIN RANGE
        // (SqlCodeGenerator.java) and MAT VIEW REFRESH RANGE
        // (SqlCompilerImpl.java). Both surface a clear SQL-level error
        // pointing at the offending TO expression. Without this guard
        // SAMPLE BY silently returns zero rows, masking what is almost
        // always a query-construction bug. FROM == TO is allowed (a
        // single-point range) -- only strict inversion is rejected.
        // LONG_NULL on either side means the clause is absent / null /
        // unbound and the bound is not in effect; only check when both
        // are concrete timestamps.
        final TimestampDriver driver = timestampDriver;
        if (fromFunc != driver.getTimestampConstantNull() && toFunc != driver.getTimestampConstantNull()) {
            final long from = driver.from(fromFunc.getTimestamp(null), ColumnType.getTimestampType(fromFunc.getType()));
            final long to = driver.from(toFunc.getTimestamp(null), ColumnType.getTimestampType(toFunc.getType()));
            if (to != Numbers.LONG_NULL && from > to) {
                throw SqlException.$(toFuncPos, "TO timestamp must not be earlier than FROM timestamp");
            }
        }
        offsetFunc.init(symbolTableSource, executionContext);
        // Evaluate runtime-constant OFFSET into native units. Mirrors
        // AbstractSampleByCursor.parseParams. Null/absent leaves
        // calendarOffset == 0, which the firstBucket() branches no-op.
        final CharSequence offsetStr = offsetFunc.getStrA(null);
        if (offsetStr != null) {
            final long parsed = Dates.parseOffset(offsetStr);
            if (parsed == Numbers.LONG_NULL) {
                throw SqlException.$(offsetFuncPos, "invalid offset: ").put(offsetStr);
            }
            calendarOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(parsed));
        } else {
            calendarOffset = 0;
        }
        // Re-resolve TIME ZONE per of() so bind variables pick up their
        // current value. The wrap is needed whenever a TZ resolves
        // (named zone or offset literal): only setLocalAnchor /
        // localAnchorAsUtc can fold tzOffset into the anchor for
        // super-day strides. tzFunc != null already implies the wrap
        // is required (binding only sets it for day-or-larger
        // SAMPLE BY + non-trivial FILL). getTimezoneRules unifies
        // offset literals (FixedTimeZoneRule) and DST zones uniformly.
        if (tzFunc != null) {
            tzFunc.init(symbolTableSource, executionContext);
            final CharSequence tz = tzFunc.getStrA(null);
            if (tz != null) {
                final TimeZoneRules tzRules;
                try {
                    tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, tz);
                } catch (CairoException e) {
                    throw SqlException.$(tzFuncPos, "invalid timezone: ").put(tz);
                }
                if (tzWrap == null) {
                    tzWrap = new TimezoneFloorTimestampSampler(baseSampler, tzRules, samplingIntervalUnit);
                } else {
                    tzWrap.setTzRules(tzRules);
                }
                timestampSampler = tzWrap;
            } else {
                timestampSampler = baseSampler;
            }
        }
    }

    /**
     * Resolves FROM and TO for a run of the fill.
     */
    void resolveBounds() {
        final TimestampDriver driver = timestampDriver;
        fromTs = fromFunc == driver.getTimestampConstantNull() ? Numbers.LONG_NULL
                : driver.from(fromFunc.getTimestamp(null), ColumnType.getTimestampType(fromFunc.getType()));
        hasExplicitTo = toFunc != driver.getTimestampConstantNull();
        maxTimestamp = hasExplicitTo
                ? driver.from(toFunc.getTimestamp(null), ColumnType.getTimestampType(toFunc.getType()))
                : Numbers.LONG_NULL;
        // Demote hasExplicitTo when TO evaluates to LONG_NULL at runtime
        // (bind variable, null::timestamp, or function returning null).
        // The toFunc identity check above only catches the constant-null
        // singleton. Long.MIN_VALUE folds into the same path: LONG_NULL ==
        // Long.MIN_VALUE is QuestDB's universal timestamp null sentinel.
        if (maxTimestamp == Numbers.LONG_NULL) {
            hasExplicitTo = false;
        }
    }
}
