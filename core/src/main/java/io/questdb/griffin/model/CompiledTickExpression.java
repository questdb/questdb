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
 * ($now, $today, $yesterday, $tomorrow). The entire expression is pre-parsed into
 * a single {@code long[]} IR (intermediate representation) at compile time;
 * {@link #evaluate} performs only long arithmetic to produce [lo, hi] interval pairs.
 * <p>
 * IR layout:
 * <pre>
 *   ir[0]:                         header (counts + flags, see HDR_* constants)
 *   ir[1]:                         numericTzOffset (Long.MIN_VALUE if not numeric)
 *   ir[2 .. 2+D):                  duration parts (encoded unit+value)
 *   ir[2+D .. 2+D+2T):             time override pairs (offset, width)
 *   ir[2+D+2T .. end):             elements
 * </pre>
 * Element encodings (tag in bits 63-62, expr in bits 59-0):
 * <ul>
 *   <li>SINGLE_VAR (tag 00): 1 long — encoded DateVariableExpr</li>
 *   <li>STATIC (tag 01): 3 longs — tag, lo, hi</li>
 *   <li>RANGE (tag 10): 2 longs — tag+isBusinessDay+startExpr, endExpr</li>
 * </ul>
 */
public class CompiledTickExpression extends UntypedFunction {
    // ---- Element tag (bits 63-62) ----
    static final long RANGE_BD_BIT = 1L << 61;
    static final long TAG_RANGE = 0x8000_0000_0000_0000L;
    static final long TAG_SINGLE_VAR = 0x0000_0000_0000_0000L;
    static final long TAG_STATIC = 0x4000_0000_0000_0000L;

    // ---- Header (ir[0]) bit layout ----
    private static final long HDR_DAY_MASK = 0x7FL;
    private static final int HDR_DUR_SHIFT = 40;
    private static final long HDR_DWE_BIT = 1L << 23;
    private static final int HDR_ELEM_SHIFT = 56;
    private static final int HDR_TO_SHIFT = 48;

    // ---- Expr encoding (bits 59-0) ----
    private static final long IR_BD_BIT = 1L << 57;
    private static final long IR_OFFSET_MASK = 0xFFFFFFFFL;
    private static final int IR_UNIT_SHIFT = 40;
    private static final int IR_VAR_SHIFT = 58;

    private static final int SATURDAY = 6;
    private static final long TAG_MASK = 0xC000_0000_0000_0000L;
    private static final int SUNDAY = 7;

    private final int dayFilterMask;
    private final int durationOff;
    private final int durationPartCount;
    private final int elemCount;
    private final int elemOff;
    private final LongList exchangeSchedule;
    private final String expression;
    private final boolean hasDurationWithExchange;
    private final long[] ir;
    private final int timeOverrideCount;
    private final TimestampDriver timestampDriver;
    private final int toOff;
    private final TimeZoneRules tzRules;
    private long now;

    CompiledTickExpression(
            TimestampDriver timestampDriver,
            long[] ir,
            CharSequence expression,
            TimeZoneRules tzRules,
            LongList exchangeSchedule
    ) {
        this.timestampDriver = timestampDriver;
        this.ir = ir;
        this.expression = expression.toString();
        this.tzRules = tzRules;
        this.exchangeSchedule = exchangeSchedule;

        // Pre-decode header and section offsets
        long header = ir[0];
        this.elemCount = (int) ((header >>> HDR_ELEM_SHIFT) & 0xFF);
        this.timeOverrideCount = (int) ((header >>> HDR_TO_SHIFT) & 0xFF);
        this.durationPartCount = (int) ((header >>> HDR_DUR_SHIFT) & 0xFF);
        this.hasDurationWithExchange = (header & HDR_DWE_BIT) != 0;
        this.dayFilterMask = (int) (header & HDR_DAY_MASK);
        this.durationOff = 2;
        this.toOff = 2 + durationPartCount;
        this.elemOff = toOff + timeOverrideCount * 2;
    }

    static long encodeDuration(char unit, int value) {
        return ((long) (unit & 0xFFFF) << 32) | (value & 0xFFFFFFFFL);
    }

    static long encodeHeader(int elemCount, int timeOverrideCount, int durationPartCount,
                             boolean hasDurationWithExchange, int dayFilterMask) {
        return ((long) (elemCount & 0xFF) << HDR_ELEM_SHIFT)
                | ((long) (timeOverrideCount & 0xFF) << HDR_TO_SHIFT)
                | ((long) (durationPartCount & 0xFF) << HDR_DUR_SHIFT)
                | (hasDurationWithExchange ? HDR_DWE_BIT : 0L)
                | (dayFilterMask & HDR_DAY_MASK);
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
        int pos = elemOff;
        for (int i = 0; i < elemCount; i++) {
            long word = ir[pos];
            long tag = word & TAG_MASK;
            if (tag == TAG_SINGLE_VAR) {
                emitSingleVar(word, outIntervals);
                pos++;
            } else if (tag == TAG_STATIC) {
                outIntervals.add(ir[pos + 1], ir[pos + 2]);
                pos += 3;
            } else {
                emitRange(word, ir[pos + 1], outIntervals);
                pos += 2;
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
            long encoded = ir[durationOff + i];
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

    private void applyTimezone(LongList out, int startIndex) {
        long numericTzOffset = ir[1];
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
                long offset = ir[toOff + j * 2];
                long width = ir[toOff + j * 2 + 1];
                long lo = dayStart + offset;
                if (durationPartCount > 0 && !hasDurationWithExchange) {
                    out.add(lo, applyDuration(lo) - 1);
                } else {
                    out.add(lo, lo + width);
                }
            }
        } else if (durationPartCount > 0 && !hasDurationWithExchange) {
            out.add(dayStart, applyDuration(dayStart) - 1);
        } else {
            out.add(dayStart, timestampDriver.endOfDay(dayStart));
        }
    }

    private void emitRange(long startEncoded, long endEncoded, LongList out) {
        boolean isBusinessDay = (startEncoded & RANGE_BD_BIT) != 0;
        long start = evaluateExpr(startEncoded);
        long end = evaluateExpr(endEncoded);

        long startDay = timestampDriver.startOfDay(start, 0);
        long endDay = timestampDriver.startOfDay(end, 0);
        boolean hasBothTimeComponents = (start != startDay) && (end != endDay);

        if (hasBothTimeComponents) {
            if (durationPartCount > 0 && timeOverrideCount == 0 && !hasDurationWithExchange) {
                out.add(start, applyDuration(start) - 1);
            } else {
                out.add(start, end);
            }
        } else {
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

    private void emitSingleVar(long encoded, LongList out) {
        long timestamp = evaluateExpr(encoded);
        boolean isDayLevel = (timestamp == timestampDriver.startOfDay(timestamp, 0));

        if (isDayLevel) {
            emitDayInterval(timestamp, out);
        } else if (durationPartCount > 0 && !hasDurationWithExchange) {
            out.add(timestamp, applyDuration(timestamp) - 1);
        } else {
            out.add(timestamp, timestamp);
        }
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
