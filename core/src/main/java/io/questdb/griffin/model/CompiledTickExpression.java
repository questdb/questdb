/*******************************************************************************
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

package io.questdb.griffin.model;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UntypedFunction;
import io.questdb.std.LongList;
import io.questdb.std.datetime.TimeZoneRules;

/**
 * A Function that represents a pre-parsed tick expression containing date variables
 * ($now, $today, $yesterday, $tomorrow). All string parsing happens at compile time;
 * {@link #evaluate} performs only long arithmetic to produce [lo, hi] interval pairs.
 * <p>
 * Elements are pre-parsed into one of three types:
 * <ul>
 *   <li>SINGLE_VAR — a single {@link DateVariableExpr}</li>
 *   <li>STATIC — a pre-computed [lo, hi] pair</li>
 *   <li>RANGE — a (start, end, isBusinessDay) triple of {@link DateVariableExpr}</li>
 * </ul>
 * <p>
 * The shared suffix (time override, duration, timezone, day filter, exchange schedule)
 * is also pre-parsed so that runtime evaluation uses only long arithmetic.
 */
public class CompiledTickExpression extends UntypedFunction {
    static final byte ELEM_RANGE = 2;
    static final byte ELEM_SINGLE_VAR = 0;
    static final byte ELEM_STATIC = 1;

    private static final int SATURDAY = 6;
    private static final int SUNDAY = 7;

    private final CairoConfiguration configuration;
    private final int dayFilterMask;
    private final int durationPartCount;
    private final char[] durationUnits;
    private final int[] durationValues;
    private final int elemCount;
    private final byte[] elemTypes;
    private final LongList exchangeSchedule;
    private final String expression;
    private final boolean hasDurationWithExchange;
    private final long numericTzOffset;
    private final boolean[] rangeBusinessDay;
    private final DateVariableExpr[] rangeEndExprs;
    private final DateVariableExpr[] rangeStartExprs;
    private final DateVariableExpr[] singleVarExprs;
    private final long[] staticElements;
    private final int timeOverrideCount;
    private final long[] timeOverrides;
    private final TimestampDriver timestampDriver;
    private final TimeZoneRules tzRules;
    private long now;

    CompiledTickExpression(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence expression,
            int elemCount,
            byte[] elemTypes,
            DateVariableExpr[] singleVarExprs,
            long[] staticElements,
            DateVariableExpr[] rangeStartExprs,
            DateVariableExpr[] rangeEndExprs,
            boolean[] rangeBusinessDay,
            long[] timeOverrides,
            int timeOverrideCount,
            char[] durationUnits,
            int[] durationValues,
            int durationPartCount,
            long numericTzOffset,
            TimeZoneRules tzRules,
            int dayFilterMask,
            LongList exchangeSchedule,
            boolean hasDurationWithExchange
    ) {
        this.timestampDriver = timestampDriver;
        this.configuration = configuration;
        this.expression = expression.toString();
        this.elemCount = elemCount;
        this.elemTypes = elemTypes;
        this.singleVarExprs = singleVarExprs;
        this.staticElements = staticElements;
        this.rangeStartExprs = rangeStartExprs;
        this.rangeEndExprs = rangeEndExprs;
        this.rangeBusinessDay = rangeBusinessDay;
        this.timeOverrides = timeOverrides;
        this.timeOverrideCount = timeOverrideCount;
        this.durationUnits = durationUnits;
        this.durationValues = durationValues;
        this.durationPartCount = durationPartCount;
        this.numericTzOffset = numericTzOffset;
        this.tzRules = tzRules;
        this.dayFilterMask = dayFilterMask;
        this.exchangeSchedule = exchangeSchedule;
        this.hasDurationWithExchange = hasDurationWithExchange;
    }

    /**
     * Evaluates the tick expression with the given "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals.
     * This overload is intended for testing without a SqlExecutionContext.
     */
    public void evaluate(LongList outIntervals, long now) throws SqlException {
        this.now = now;
        evaluate(outIntervals);
    }

    /**
     * Evaluates the tick expression using the captured "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals. Pure long arithmetic,
     * no string parsing.
     */
    public void evaluate(LongList outIntervals) throws SqlException {
        int outStart = outIntervals.size();

        // Phase 1: Emit intervals for each element
        int singleVarIdx = 0, staticIdx = 0, rangeIdx = 0;
        for (int i = 0; i < elemCount; i++) {
            switch (elemTypes[i]) {
                case ELEM_SINGLE_VAR -> emitSingleVar(singleVarExprs[singleVarIdx++], outIntervals);
                case ELEM_STATIC -> {
                    outIntervals.add(staticElements[staticIdx * 2], staticElements[staticIdx * 2 + 1]);
                    staticIdx++;
                }
                case ELEM_RANGE -> {
                    emitRange(rangeStartExprs[rangeIdx], rangeEndExprs[rangeIdx],
                            rangeBusinessDay[rangeIdx], outIntervals);
                    rangeIdx++;
                }
            }
        }

        // Phase 2: Apply day filter (before timezone conversion, in local time)
        if (dayFilterMask != 0 && exchangeSchedule == null) {
            applyDayFilter(outIntervals, outStart);
        }

        // Phase 3: Apply timezone conversion
        applyTimezone(outIntervals, outStart);

        // Phase 4: Apply exchange schedule filter + optional duration
        if (exchangeSchedule != null) {
            IntervalUtils.applyTickCalendarFilter(timestampDriver, exchangeSchedule, outIntervals, outStart);
            if (hasDurationWithExchange) {
                applyDurationToAllIntervals(outIntervals, outStart);
            }
        }

        // Phase 5: Sort and merge overlapping intervals
        if (outIntervals.size() > outStart + 2) {
            IntervalUtils.unionBracketExpandedIntervals(outIntervals, outStart);
        }
    }

    @Override
    public int getType() {
        return ColumnType.UNDEFINED;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        now = executionContext.getNow(timestampDriver.getTimestampType());
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("tick('").val(expression).val("')");
    }

    private long applyDuration(long timestamp) {
        long result = timestamp;
        for (int i = 0; i < durationPartCount; i++) {
            result = timestampDriver.add(result, durationUnits[i], durationValues[i]);
        }
        return result;
    }

    private void applyDayFilter(LongList out, int startIndex) {
        int originalSize = out.size();

        // Count matching intervals (single-day check on lo)
        int totalIntervals = 0;
        for (int readIdx = startIndex; readIdx < originalSize; readIdx += 2) {
            long lo = out.getQuick(readIdx);
            int dayOfWeek = timestampDriver.getDayOfWeek(lo) - 1; // 0-6
            if ((dayFilterMask & (1 << dayOfWeek)) != 0) {
                totalIntervals++;
            }
        }

        // Write matching intervals at end, then copy back
        out.setPos(originalSize + totalIntervals * 2);
        int writeIdx = originalSize;
        for (int readIdx = startIndex; readIdx < originalSize; readIdx += 2) {
            long lo = out.getQuick(readIdx);
            long hi = out.getQuick(readIdx + 1);
            int dayOfWeek = timestampDriver.getDayOfWeek(lo) - 1;
            if ((dayFilterMask & (1 << dayOfWeek)) != 0) {
                out.setQuick(writeIdx++, lo);
                out.setQuick(writeIdx++, hi);
            }
        }

        for (int i = 0; i < totalIntervals * 2; i++) {
            out.setQuick(startIndex + i, out.getQuick(originalSize + i));
        }
        out.setPos(startIndex + totalIntervals * 2);
    }

    private void applyDurationToAllIntervals(LongList out, int startIndex) {
        for (int i = startIndex + 1; i < out.size(); i += 2) {
            long hi = out.getQuick(i);
            out.setQuick(i, applyDuration(hi));
        }
    }

    private void applyTimezone(LongList out, int startIndex) {
        int size = out.size();
        if (numericTzOffset != Long.MIN_VALUE) {
            for (int i = startIndex; i < size; i++) {
                out.setQuick(i, out.getQuick(i) - numericTzOffset);
            }
        } else if (tzRules != null) {
            for (int i = startIndex; i < size; i += 2) {
                long lo = out.getQuick(i);
                long hi = out.getQuick(i + 1);

                long adjustedLo = lo;
                long adjustedHi = hi;
                long gapDuration = tzRules.getDstGapOffset(lo);
                if (gapDuration != 0) {
                    adjustedLo = lo + gapDuration;
                    adjustedHi = hi + gapDuration;
                } else {
                    gapDuration = tzRules.getDstGapOffset(hi);
                    if (gapDuration != 0) {
                        adjustedHi = hi + gapDuration;
                    }
                }

                out.setQuick(i, timestampDriver.toUTC(adjustedLo, tzRules));
                out.setQuick(i + 1, timestampDriver.toUTC(adjustedHi, tzRules));
            }
        }
    }

    private void emitDayInterval(long dayStart, LongList out) {
        if (timeOverrideCount > 0) {
            for (int j = 0; j < timeOverrideCount; j++) {
                long lo = dayStart + timeOverrides[j * 2];
                if (durationPartCount > 0 && !hasDurationWithExchange) {
                    out.add(lo, applyDuration(lo) - 1);
                } else {
                    out.add(lo, lo + timeOverrides[j * 2 + 1]);
                }
            }
        } else if (durationPartCount > 0 && !hasDurationWithExchange) {
            out.add(dayStart, applyDuration(dayStart) - 1);
        } else {
            out.add(dayStart, timestampDriver.endOfDay(dayStart));
        }
    }

    private void emitRange(DateVariableExpr startExpr, DateVariableExpr endExpr,
                           boolean isBusinessDay, LongList out) {
        long start = startExpr.evaluate(timestampDriver, now);
        long end = endExpr.evaluate(timestampDriver, now);

        long startDay = timestampDriver.startOfDay(start, 0);
        long endDay = timestampDriver.startOfDay(end, 0);
        boolean hasBothTimeComponents = (start != startDay) && (end != endDay);

        if (hasBothTimeComponents) {
            // Time-based range: single interval
            if (durationPartCount > 0 && timeOverrideCount == 0 && !hasDurationWithExchange) {
                out.add(start, applyDuration(start) - 1);
            } else {
                out.add(start, end);
            }
        } else {
            // Day-based range: iterate day by day
            long currentDay = startDay;
            while (currentDay <= endDay) {
                if (isBusinessDay) {
                    int dow = timestampDriver.getDayOfWeek(currentDay);
                    if (dow == SATURDAY || dow == SUNDAY) {
                        currentDay = timestampDriver.addDays(currentDay, 1);
                        continue;
                    }
                }
                emitDayInterval(currentDay, out);
                currentDay = timestampDriver.addDays(currentDay, 1);
            }
        }
    }

    private void emitSingleVar(DateVariableExpr expr, LongList out) {
        long timestamp = expr.evaluate(timestampDriver, now);
        boolean isDayLevel = (timestamp == timestampDriver.startOfDay(timestamp, 0));

        if (isDayLevel) {
            emitDayInterval(timestamp, out);
        } else if (durationPartCount > 0 && !hasDurationWithExchange) {
            out.add(timestamp, applyDuration(timestamp) - 1);
        } else {
            out.add(timestamp, timestamp);
        }
    }
}
