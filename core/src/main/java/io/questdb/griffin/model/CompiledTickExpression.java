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

package io.questdb.griffin.model;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UntypedFunction;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.StringSink;

/**
 * A Function that represents a pre-parsed tick expression containing date variables
 * ($now, $today, $yesterday, $tomorrow). The entire expression is pre-parsed into
 * a single {@code long[]} IR (intermediate representation) at compile time;
 * {@link #evaluate} performs only long arithmetic to produce [lo, hi] interval pairs.
 * <p>
 * IR layout:
 * <pre>
 *   ir[0]:                         header (flags, see HDR_* constants)
 *   ir[1]:                         numericTzOffset (Long.MIN_VALUE if not numeric)
 *   ir[2]:                         D, the number of duration parts
 *   ir[3]:                         T, the number of time overrides
 *   ir[4]:                         the number of elements
 *   ir[5 .. 5+D):                  duration parts (encoded unit+value)
 *   ir[5+D .. 5+D+3T):             time override triples (offset, width, zoneMatch)
 *   ir[5+D+3T .. end):             elements
 * </pre>
 * Element encodings (tag in bits 63-62, expr in bits 59-0):
 * <ul>
 *   <li>SINGLE_VAR (tag 00): 1 long — encoded DateVariableExpr</li>
 *   <li>STATIC (tag 01): 3 longs — tag, lo, hi</li>
 *   <li>RANGE (tag 10): 2 longs — tag+isBusinessDay+startExpr, endExpr</li>
 * </ul>
 * {@link #evaluate} runs the steps of the parser in the same order, and stops at the interval
 * limit where the parser stops, with the same error.
 * <p>
 * An expression that the IR cannot represent exactly has no IR: {@link #evaluate} parses it with
 * {@link IntervalUtils#parseTickExpr} instead, the way a bind variable is parsed, so that a
 * literal and a bind variable with the same expression always produce the same intervals.
 */
public class CompiledTickExpression extends UntypedFunction {
    // ---- Element tag (bits 63-62) ----
    static final long RANGE_BD_BIT = 1L << 61;
    static final long TAG_RANGE = 0x8000_0000_0000_0000L;
    static final long TAG_SINGLE_VAR = 0x0000_0000_0000_0000L;
    static final long TAG_STATIC = 0x4000_0000_0000_0000L;
    // ---- Section counts (ir[2..4]), a long each, so that no count is ever truncated ----
    static final int IR_DURATION_PART_COUNT_INDEX = 2;
    static final int IR_ELEMENT_COUNT_INDEX = 4;
    static final int IR_TIME_OVERRIDE_COUNT_INDEX = 3;

    // ---- Header (ir[0]) bit layout ----
    private static final long HDR_DAY_MASK = 0x7FL;
    private static final long HDR_DWE_BIT = 1L << 23;
    // ---- Sections: duration parts, then time overrides, then elements ----
    private static final int IR_DURATION_PARTS_INDEX = 5;

    // ---- Expr encoding (bits 59-0) ----
    private static final long IR_BD_BIT = 1L << 57;
    private static final long IR_OFFSET_MASK = 0xFFFFFFFFL;
    private static final int IR_UNIT_SHIFT = 40;
    private static final int IR_VAR_SHIFT = 58;

    private static final int SATURDAY = 6;
    private static final long TAG_MASK = 0xC000_0000_0000_0000L;
    private static final int SUNDAY = 7;

    // the interval limits, and the parser for an expression without IR
    private final CairoConfiguration configuration;
    private final DateLocale dateLocale;
    private final int dayFilterMask;
    private final int durationOff;
    private final int durationPartCount;
    private final int elemCount;
    private final int elemOff;
    private final LongList exchangeSchedule;
    private final String expression;
    private final boolean hasDurationWithExchange;
    // null for an expression without IR, which evaluate() parses
    private final LongList ir;
    // set only for an expression without IR
    private final StringSink parseSink;
    // the position of the expression, for errors
    private final int position;
    private final int timeOverrideCount;
    private final TimestampDriver timestampDriver;
    private final int toOff;
    private final TimeZoneRules tzRules;
    private long now;

    CompiledTickExpression(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            LongList ir,
            CharSequence expression,
            int position,
            TimeZoneRules tzRules,
            DateLocale dateLocale,
            LongList exchangeSchedule
    ) {
        this.timestampDriver = timestampDriver;
        this.configuration = configuration;
        this.ir = ir;
        this.expression = expression.toString();
        this.position = position;
        this.tzRules = tzRules;
        this.dateLocale = dateLocale;
        this.exchangeSchedule = exchangeSchedule;
        this.parseSink = null;

        // Pre-decode header and section offsets
        long header = ir.getQuick(0);
        this.hasDurationWithExchange = (header & HDR_DWE_BIT) != 0;
        this.dayFilterMask = (int) (header & HDR_DAY_MASK);
        this.durationPartCount = (int) ir.getQuick(IR_DURATION_PART_COUNT_INDEX);
        this.timeOverrideCount = (int) ir.getQuick(IR_TIME_OVERRIDE_COUNT_INDEX);
        this.elemCount = (int) ir.getQuick(IR_ELEMENT_COUNT_INDEX);
        this.durationOff = IR_DURATION_PARTS_INDEX;
        this.toOff = durationOff + durationPartCount;
        this.elemOff = toOff + timeOverrideCount * 3;
    }

    /**
     * An expression that the IR cannot represent exactly. {@link #evaluate} parses it at every
     * evaluation with the captured "now".
     */
    CompiledTickExpression(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence expression,
            int position
    ) {
        this.timestampDriver = timestampDriver;
        this.configuration = configuration;
        this.expression = expression.toString();
        this.position = position;
        this.parseSink = new StringSink();
        this.ir = null;
        this.tzRules = null;
        this.dateLocale = null;
        this.exchangeSchedule = null;
        this.hasDurationWithExchange = false;
        this.dayFilterMask = 0;
        this.durationPartCount = 0;
        this.timeOverrideCount = 0;
        this.elemCount = 0;
        this.durationOff = 0;
        this.toOff = 0;
        this.elemOff = 0;
    }

    static long encodeDuration(char unit, int value) {
        return ((long) (unit & 0xFFFF) << 32) | (value & 0xFFFFFFFFL);
    }

    static long encodeHeader(boolean hasDurationWithExchange, int dayFilterMask) {
        return (hasDurationWithExchange ? HDR_DWE_BIT : 0L) | (dayFilterMask & HDR_DAY_MASK);
    }

    /**
     * Evaluates the tick expression with the given "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals.
     * This overload is intended for testing without a SqlExecutionContext.
     */
    public void evaluate(LongList outIntervals, long now) {
        this.now = now;
        evaluate(outIntervals);
    }

    /**
     * Evaluates the tick expression using the captured "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals. With IR this is pure long
     * arithmetic, without IR it parses the expression.
     */
    public void evaluate(LongList outIntervals) {
        if (ir == null) {
            parse(outIntervals);
            return;
        }
        final int outStart = outIntervals.size();

        // Emit the intervals of each element, and run its day filter and timezone steps on them,
        // or on the intervals of each day of a range
        int pos = elemOff;
        for (int i = 0; i < elemCount; i++) {
            long word = ir.getQuick(pos);
            long tag = word & TAG_MASK;
            if (tag == TAG_SINGLE_VAR) {
                final int elementStart = outIntervals.size();
                emitSingleVar(word, outIntervals);
                applyElementSteps(outIntervals, elementStart);
                pos++;
            } else if (tag == TAG_STATIC) {
                final int elementStart = outIntervals.size();
                outIntervals.add(ir.getQuick(pos + 1), ir.getQuick(pos + 2));
                applyElementSteps(outIntervals, elementStart);
                pos += 3;
            } else {
                emitRange(word, ir.getQuick(pos + 1), outIntervals, outStart);
                pos += 2;
            }
        }

        // The tick calendar filter and its duration apply to the whole result, in UTC
        if (exchangeSchedule != null) {
            IntervalUtils.applyTickCalendarFilter(timestampDriver, exchangeSchedule, outIntervals, outStart);
            if (hasDurationWithExchange) {
                applyDurationToAllIntervals(outIntervals, outStart);
            }
        }

        // Sort and merge the intervals, then check the final count, as the parser does
        if (outIntervals.size() > outStart + 2) {
            unionAndCheckLimit(outIntervals, outStart, "Bracket expansion");
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

    private long addBusinessDays(long timestamp, int businessDays) {
        if (businessDays == 0) {
            return timestamp;
        }
        int direction = businessDays > 0 ? 1 : -1;
        int remaining = Math.abs(businessDays);
        long result = timestamp;
        while (remaining > 0) {
            result = timestampDriver.addDays(result, direction);
            int dow = timestampDriver.getDayOfWeek(result);
            if (dow != SATURDAY && dow != SUNDAY) {
                remaining--;
            }
        }
        return result;
    }

    private long applyDuration(long timestamp) {
        long result = timestamp;
        for (int i = 0; i < durationPartCount; i++) {
            long encoded = ir.getQuick(durationOff + i);
            char unit = (char) ((encoded >>> 32) & 0xFFFF);
            int value = (int) encoded;
            result = timestampDriver.add(result, unit, value);
        }
        return result;
    }

    private void applyDayFilter(LongList out, int startIndex) {
        int originalSize = out.size();
        int totalIntervals = 0;
        for (int readIdx = startIndex; readIdx < originalSize; readIdx += 2) {
            long lo = out.getQuick(readIdx);
            int dayOfWeek = timestampDriver.getDayOfWeek(lo) - 1;
            if ((dayFilterMask & (1 << dayOfWeek)) != 0) {
                totalIntervals++;
            }
        }
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

    /**
     * Runs the day filter, in local time, and the timezone steps on the intervals from
     * {@code startIndex} on, which belong to one element, or one day of a range.
     */
    private void applyElementSteps(LongList out, int startIndex) {
        if (dayFilterMask != 0 && exchangeSchedule == null) {
            applyDayFilter(out, startIndex);
        }
        applyTimezone(out, startIndex);
    }

    private void applyTimezone(LongList out, int startIndex) {
        long numericTzOffset = ir.getQuick(1);
        int size = out.size();
        if (numericTzOffset != Long.MIN_VALUE) {
            for (int i = startIndex; i < size; i++) {
                out.setQuick(i, out.getQuick(i) - numericTzOffset);
            }
        } else if (tzRules != null) {
            IntervalUtils.localIntervalsToUtc(tzRules, out, startIndex, 2);
        }
    }

    private void emitDayInterval(long dayStart, LongList out) {
        if (timeOverrideCount > 0) {
            for (int j = 0; j < timeOverrideCount; j++) {
                long offset = ir.getQuick(toOff + j * 3);
                long width = ir.getQuick(toOff + j * 3 + 1);
                long zoneMatch = ir.getQuick(toOff + j * 3 + 2);
                long anchor = dayStart + offset;
                long lo = anchor;
                long hi;
                if (durationPartCount > 0 && !hasDurationWithExchange) {
                    long endExclusive = applyDuration(anchor);
                    lo = IntervalUtils.resolveDurationLo(anchor, endExclusive);
                    hi = IntervalUtils.resolveDurationHi(anchor, endExclusive);
                } else {
                    hi = anchor + width;
                }
                if (zoneMatch != Long.MIN_VALUE) {
                    // Per-element tz: convert to UTC first
                    if (zoneMatch != Long.MAX_VALUE && dateLocale != null) {
                        TimeZoneRules elemRules = dateLocale.getZoneRules(
                                Numbers.decodeLowInt(zoneMatch),
                                timestampDriver.getTZRuleResolution()
                        );
                        lo = timestampDriver.toUTC(lo, elemRules);
                        hi = timestampDriver.toUTC(hi, elemRules);
                    }
                    // lo/hi are now UTC; convert to global-local so Phase 3
                    // (applyTimezone) produces the correct UTC result
                    lo = utcToGlobalLocal(lo);
                    hi = utcToGlobalLocal(hi);
                }
                out.add(lo, hi);
            }
        } else if (durationPartCount > 0 && !hasDurationWithExchange) {
            long endExclusive = applyDuration(dayStart);
            out.add(
                    IntervalUtils.resolveDurationLo(dayStart, endExclusive),
                    IntervalUtils.resolveDurationHi(dayStart, endExclusive)
            );
        } else {
            out.add(dayStart, timestampDriver.endOfDay(dayStart));
        }
    }

    private void emitRange(long startEncoded, long endEncoded, LongList out, int outStart) {
        long start = evaluateExpr(startEncoded);
        long end = evaluateExpr(endEncoded);
        // The expression, not its value, decides whether an endpoint is a whole day or a point
        // in time, so a point in time that falls exactly on midnight stays a point in time.
        boolean isStartDay = DateVariableExpr.isDayLevel(startEncoded);
        boolean isEndDay = DateVariableExpr.isDayLevel(endEncoded);
        // As in the parser, a whole-day endpoint starts the range at the start of its day, and
        // ends it at the end of its day, and a range that starts after it ends fails
        final long rangeLo = isStartDay ? timestampDriver.startOfDay(start, 0) : start;
        final long rangeHi = isEndDay ? timestampDriver.endOfDay(timestampDriver.startOfDay(end, 0)) : end;
        if (rangeLo > rangeHi) {
            throw CairoException.nonCritical().position(position).put("Invalid date range: start is after end");
        }

        if (!isStartDay && !isEndDay) {
            final int rangeStart = out.size();
            out.add(start, end);
            applyElementSteps(out, rangeStart);
            return;
        }

        // Iterate the days of the range, and clip their intervals to the endpoint that is a point
        // in time, if any. A whole-day endpoint covers its entire day. Like the parser, run the
        // steps of each day on its intervals, then merge all intervals so far once there are
        // enough of them, and stop when the merged ones exceed the limit.
        final boolean isBusinessDay = (startEncoded & RANGE_BD_BIT) != 0;
        final boolean isClipped = !isStartDay || !isEndDay;
        final long clipLo = isStartDay ? Long.MIN_VALUE : start;
        final long clipHi = isEndDay ? Long.MAX_VALUE : end;
        final int mergeThreshold = configuration.getSqlIntervalIncrementalMergeThreshold();
        long currentDay = timestampDriver.startOfDay(start, 0);
        long endDay = timestampDriver.startOfDay(end, 0);
        while (currentDay <= endDay) {
            if (isBusinessDay) {
                int dow = timestampDriver.getDayOfWeek(currentDay);
                if (dow == SATURDAY || dow == SUNDAY) {
                    currentDay = timestampDriver.addDays(currentDay, 1);
                    continue;
                }
            }
            final int dayStart = out.size();
            emitDayInterval(currentDay, out);
            if (isClipped) {
                IntervalUtils.clipIntervals(out, dayStart, clipLo, clipHi);
            }
            applyElementSteps(out, dayStart);
            if ((out.size() - outStart) / 2 >= mergeThreshold) {
                unionAndCheckLimit(out, outStart, "Date range expansion");
            }
            currentDay = timestampDriver.addDays(currentDay, 1);
        }
    }

    private void emitSingleVar(long encoded, LongList out) {
        long timestamp = evaluateExpr(encoded);

        if (DateVariableExpr.isDayLevel(encoded)) {
            emitDayInterval(timestamp, out);
        } else if (durationPartCount > 0 && !hasDurationWithExchange) {
            long endExclusive = applyDuration(timestamp);
            out.add(IntervalUtils.resolveDurationLo(timestamp, endExclusive),
                    IntervalUtils.resolveDurationHi(timestamp, endExclusive));
        } else {
            out.add(timestamp, timestamp);
        }
    }

    /**
     * Parses the expression the same way as a bind variable, against the captured "now", and fails
     * with the parser's error.
     */
    private void parse(LongList outIntervals) {
        try {
            IntervalUtils.parseTickExpr(
                    timestampDriver,
                    configuration,
                    expression,
                    0,
                    expression.length(),
                    position,
                    outIntervals,
                    IntervalOperation.INTERSECT,
                    parseSink,
                    true,
                    now
            );
        } catch (SqlException e) {
            throw CairoException.nonCritical().position(e.getPosition()).put(e.getFlyweightMessage());
        }
    }

    /**
     * Converts a UTC timestamp to the global timezone's local time,
     * so that Phase 3 (applyTimezone) can convert it back to UTC correctly.
     * Used for per-element timezone entries that are already in UTC.
     */
    /**
     * Sorts and merges the intervals from {@code outStart} on, and fails when more of them remain
     * than the limit, as the parser does after each day of a date variable range, and at the end.
     */
    private void unionAndCheckLimit(LongList out, int outStart, String expansion) {
        IntervalUtils.unionBracketExpandedIntervals(out, outStart);
        final int maxIntervals = configuration.getSqlIntervalMaxIntervalsAfterMerge();
        if ((out.size() - outStart) / 2 > maxIntervals) {
            out.setPos(outStart);
            throw CairoException.nonCritical().position(position)
                    .put(expansion).put(" produces too many intervals (max ").put(maxIntervals).put(')');
        }
    }

    private long utcToGlobalLocal(long utc) {
        long numericTzOffset = ir.getQuick(1);
        if (numericTzOffset != Long.MIN_VALUE) {
            return utc + numericTzOffset;
        } else if (tzRules != null) {
            return utc + tzRules.getOffset(utc);
        }
        return utc;
    }

    private long evaluateExpr(long encoded) {
        int varType = (int) ((encoded >>> IR_VAR_SHIFT) & 0x3);
        long base = switch (varType) {
            case DateVariableExpr.VAR_NOW -> now;
            case DateVariableExpr.VAR_TODAY -> timestampDriver.startOfDay(now, 0);
            case DateVariableExpr.VAR_TOMORROW -> timestampDriver.startOfDay(now, 1);
            case DateVariableExpr.VAR_YESTERDAY -> timestampDriver.startOfDay(now, -1);
            default -> throw new IllegalStateException("Unknown variable type: " + varType);
        };
        char offsetUnit = (char) ((encoded >>> IR_UNIT_SHIFT) & 0xFFFF);
        boolean isBusinessDays = (encoded & IR_BD_BIT) != 0;
        if (offsetUnit == 0 && !isBusinessDays) {
            return base;
        }
        int offsetValue = (int) (encoded & IR_OFFSET_MASK);
        if (isBusinessDays) {
            return addBusinessDays(base, offsetValue);
        }
        char normalizedUnit = (offsetUnit == 'D') ? 'd' : offsetUnit;
        return timestampDriver.add(base, normalizedUnit, offsetValue);
    }
}
