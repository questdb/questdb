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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
 *   ir[2]:                         element count
 *   ir[3]:                         time override count (T)
 *   ir[4]:                         duration part count (D)
 *   ir[5 .. 5+D):                  duration parts (encoded unit+value)
 *   ir[5+D .. 5+D+3T):             time override triples (offset, width, zoneMatch)
 *   ir[5+D+3T .. end):             elements
 * </pre>
 * Each count takes a whole IR slot, so no count can overflow its field.
 * Element encodings (tag in bits 63-62, expr in bits 59-0):
 * <ul>
 *   <li>SINGLE_VAR (tag 00): 1 long — encoded DateVariableExpr</li>
 *   <li>STATIC (tag 01): 3 longs — tag, lo, hi of an interval of a fixed date, after the day filter
 *   and in UTC; the compiler writes those of all fixed dates where the first fixed date of the list
 *   stands</li>
 *   <li>RANGE (tag 10): 2 longs — tag+isBusinessDay+startExpr, endExpr</li>
 * </ul>
 * {@link #evaluate} runs the steps of the parser in the same order, and stops at the interval
 * limit where the parser stops, with the same error.
 * <p>
 * In a list with a tick calendar whose only ';' starts its duration, each static date keeps its own
 * extent in UTC, as the parser keeps it for a bind variable, see {@link IntervalUtils#compileTickExpr}.
 * Where the own extents of a static date alone exceed the interval limit, the IR holds windows as long
 * as the duration instead, and {@link #evaluate} parses the list first, see {@link #HDR_PARSE_FIRST_BIT}.
 * <p>
 * An expression that the IR cannot represent exactly has no IR: {@link #evaluate} parses it with
 * {@link IntervalUtils#parseTickExpr} instead, the way a bind variable is parsed, so that a
 * literal and a bind variable with the same expression always produce the same intervals.
 * <p>
 * A query that a user runs fails on a range that starts after it ends, on more intervals than
 * the limit, and on an expression without IR that does not parse. The SQL that the engine runs on
 * its own, see {@link SqlExecutionContext#isBackgroundJob()}, has no caller to report a failure
 * to: WAL apply would suspend the table on an UPDATE that the client was told succeeded. A cursor
 * that the compiler holds to inspect a query, see {@link SqlExecutionContext#isCompileProbe()},
 * hands nobody its results, and its "now" need not be the one the query runs at. In both, see
 * {@link IntervalUtils#isTickEvaluationLenient}, such a range adds no interval, the limit does not
 * apply, as {@link #validate} checked it when the SQL compiled, and an expression that does not
 * parse resolves to no interval, see {@link #evaluate(LongList)}.
 */
public class CompiledTickExpression extends UntypedFunction {
    // ---- Element tag (bits 63-62) ----
    static final long RANGE_BD_BIT = 1L << 61;
    static final long TAG_RANGE = 0x8000_0000_0000_0000L;
    static final long TAG_SINGLE_VAR = 0x0000_0000_0000_0000L;
    static final long TAG_STATIC = 0x4000_0000_0000_0000L;
    // ---- Header (ir[0]) bits that IntervalUtils.compileTickExpr() sets ----
    // evaluate() parses the expression as its bind variable, with the dates of the list keeping their
    // own extent, and evaluates the IR, whose static dates are windows as long as the duration, only
    // where the evaluation is lenient and the parse fails
    static final long HDR_PARSE_FIRST_BIT = 1L << 25;

    // ---- IR prefix slots ----
    static final int IR_DURATION_COUNT_INDEX = 4;
    static final int IR_ELEM_COUNT_INDEX = 2;
    static final int IR_HEADER_INDEX = 0;
    static final int IR_PREFIX_SIZE = 5;
    static final int IR_TIME_OVERRIDE_COUNT_INDEX = 3;
    static final int IR_TZ_OFFSET_INDEX = 1;

    // ---- Header (ir[0]) bit layout ----
    private static final long HDR_DAY_MASK = 0x7FL;
    private static final long HDR_DWE_BIT = 1L << 23;

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
    // see HDR_PARSE_FIRST_BIT
    private final boolean isParseFirst;
    // set only for an expression that evaluate() parses
    private final StringSink parseSink;
    // the position of the expression, for errors
    private final int position;
    private final int timeOverrideCount;
    private final TimestampDriver timestampDriver;
    private final int toOff;
    private final TimeZoneRules tzRules;
    // the checks that evaluate() runs, see IntervalUtils.TICK_CHECK_ALL
    private int checks = IntervalUtils.TICK_CHECK_ALL;
    // set where IntervalUtils.isTickEvaluationLenient() holds: an expression that does not parse
    // resolves to no interval there instead of failing
    private boolean isLenient;
    private long now;
    // the intervals that the merges after the days of date ranges sorted, merged, moved or compared,
    // over all evaluations of this expression
    private long rangeMergeIntervalCount;

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

        // Pre-decode header, counts and section offsets
        long header = ir.getQuick(IR_HEADER_INDEX);
        this.isParseFirst = (header & HDR_PARSE_FIRST_BIT) != 0;
        this.parseSink = isParseFirst ? new StringSink() : null;
        this.elemCount = (int) ir.getQuick(IR_ELEM_COUNT_INDEX);
        this.timeOverrideCount = (int) ir.getQuick(IR_TIME_OVERRIDE_COUNT_INDEX);
        this.durationPartCount = (int) ir.getQuick(IR_DURATION_COUNT_INDEX);
        this.hasDurationWithExchange = (header & HDR_DWE_BIT) != 0;
        this.dayFilterMask = (int) (header & HDR_DAY_MASK);
        // IntervalUtils.compileTickExpr() parses the '#' suffix as a day filter only when it names no
        // tick calendar, so the day filter of evaluate() never meets both.
        assert dayFilterMask == 0 || exchangeSchedule == null;
        this.durationOff = IR_PREFIX_SIZE;
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
        this.isParseFirst = false;
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

    // The timezone step of evaluate(): converts each interval from startIndex on from local time to
    // UTC, a named zone with IntervalUtils.localIntervalsToUtc(). IntervalUtils.compileTickExpr()
    // converts the static intervals of a list with it.
    static void applyTimezone(
            TimestampDriver timestampDriver,
            long numericTzOffset,
            @Nullable TimeZoneRules tzRules,
            LongList out,
            int startIndex
    ) {
        if (numericTzOffset != Long.MIN_VALUE) {
            for (int i = startIndex, size = out.size(); i < size; i++) {
                out.setQuick(i, out.getQuick(i) - numericTzOffset);
            }
        } else if (tzRules != null) {
            IntervalUtils.localIntervalsToUtc(tzRules, out, startIndex);
        }
    }

    static long encodeHeader(boolean hasDurationWithExchange, int dayFilterMask) {
        return (hasDurationWithExchange ? HDR_DWE_BIT : 0L) | (dayFilterMask & HDR_DAY_MASK);
    }

    // Micros.addPeriod() and Nanos.addPeriod() return NULL for a unit they do not know, and
    // DateVariableExpr.parseEncoded() accepts any single character as the unit. evaluateExpr()
    // maps 'D' to 'd', and a zero unit means no offset.
    private static boolean hasKnownOffsetUnit(long encoded) {
        final char unit = (char) ((encoded >>> IR_UNIT_SHIFT) & 0xFFFF);
        return unit == 0 || unit == 'D' || isKnownDurationUnit(unit);
    }

    // The units Micros.addPeriod() and Nanos.addPeriod() know. IntervalUtils.compileTickExpr()
    // compiles an expression whose duration has another unit into one that parses on every
    // evaluation, see isParsedOnEvaluation().
    private static boolean isKnownDurationUnit(char unit) {
        return switch (unit) {
            case 'n', 'u', 'T', 's', 'm', 'h', 'd', 'w', 'M', 'y' -> true;
            default -> false;
        };
    }

    /**
     * Evaluates the tick expression with the given "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals. For callers that
     * read "now" themselves, and for tests without a SqlExecutionContext.
     */
    public void evaluate(LongList outIntervals, long now) {
        this.now = now;
        evaluate(outIntervals);
    }

    /**
     * Evaluates the tick expression using the captured "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals. With IR this is pure long
     * arithmetic, without IR it parses the expression.
     * <p>
     * Where {@link IntervalUtils#isTickEvaluationLenient} holds, an expression without IR that does
     * not parse appends nothing, as an expression that resolves to no interval does, which IN
     * matches no row with, and NOT IN every row, the same as a null value. A query that a user runs
     * fails on it instead.
     * <p>
     * An expression whose IR holds windows for static dates whose own extents alone exceed the limit,
     * see {@link #HDR_PARSE_FIRST_BIT}, parses as its bind variable, with the limit also where the
     * evaluation is lenient, and there the IR evaluates it as it always has where the parse fails.
     */
    public void evaluate(LongList outIntervals) {
        if (ir == null) {
            parse(outIntervals);
            return;
        }
        if (isParseFirst && parseFirst(outIntervals)) {
            return;
        }
        final int outStart = outIntervals.size();
        // the end of the intervals that the last merge after a day of a range left merged
        int mergedEnd = outStart;

        // Emit the intervals of each date variable element, and run its day filter and timezone
        // steps on them, or on the intervals of each day of a range. The compiler applied the day
        // filter and the time zone to the STATIC entries, so they join the intervals as they are.
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
                outIntervals.add(ir.getQuick(pos + 1), ir.getQuick(pos + 2));
                pos += 3;
            } else {
                mergedEnd = emitRange(word, ir.getQuick(pos + 1), outIntervals, outStart, mergedEnd);
                pos += 2;
            }
        }

        // The tick calendar filter and its duration apply to the whole result, in UTC. Own extents
        // can hold one another, as a day holds a minute of a static date, and the filter unions
        // them before it intersects them.
        if (exchangeSchedule != null) {
            IntervalUtils.applyTickCalendarFilter(timestampDriver, exchangeSchedule, outIntervals, outStart);
            if (hasDurationWithExchange) {
                applyDurationToAllIntervals(outIntervals, outStart);
            }
        }

        // Sort and merge the intervals, then check the final count, as the parser does
        if (outIntervals.size() > outStart + 2) {
            unionAndCheckLimit(outIntervals, outStart, outStart, "Bracket expansion");
        }
    }

    /**
     * Returns the number of IR elements: one per SINGLE_VAR, RANGE and STATIC entry.
     */
    @TestOnly
    public int getElementCount() {
        return elemCount;
    }

    public String getExpression() {
        return expression;
    }

    /**
     * Returns an upper bound on the number of intervals {@link #evaluate} yields for any "now", or
     * -1 when this method knows no bound or the bound exceeds {@code cap}. The JIT filter compiles a
     * fixed number of range checks for the expression and fills them on every execution, so the
     * bound has to hold for every clock value, not only for the current one: '$today#workday' yields
     * one interval on a weekday and none at a weekend.
     * <p>
     * An expression that {@link #evaluate} parses, see {@link #isParsedOnEvaluation()}, gets no bound.
     * Otherwise the bound is the sum of the element bounds, where the STATIC entries count together.
     * The final merge joins the intervals that overlap or touch, so it yields no more intervals than
     * the STATIC bound below plus the intervals that the date variable elements emit.
     * <ul>
     *     <li>STATIC: the runs of overlapping or adjacent intervals that the STATIC entries form,
     *     plus one for each empty interval (lo &gt; hi, as a zero duration yields), which the merge
     *     may keep apart from a run. The compiler stores the STATIC entries after the day filter and
     *     the timezone conversion: one per bracket leaf, day or list value, or one per interval of
     *     their union once they exceed the interval cap, so '[$today, 2024-01-[01..20]]' holds twenty
     *     in one run. {@link #evaluate} emits them as stored, so this method counts their runs as
     *     they are.</li>
     *     <li>SINGLE_VAR: the time override count, or 1 without time overrides.
     *     {@link #emitSingleVar} emits one interval per time override for a day-aligned value, and
     *     one interval otherwise. The day filter only drops intervals. A numeric offset maps each
     *     interval to one. A named timezone with clock changes converts an interval that starts or
     *     ends inside a repeated hour to up to three, see {@link IntervalUtils#localIntervalsToUtc},
     *     so such an element counts three times as many, unless it is a whole day: a repeated hour
     *     that starts or ends at midnight, or holds no midnight, leaves the days whole. A zone whose
     *     clocks fall back across midnight would split a day, and
     *     {@link io.questdb.griffin.engine.functions.bind.CompiledFilterTickBounds} fails the query
     *     rather than lose rows when the bound falls short.</li>
     *     <li>RANGE: 1 when the range is contiguous, that is, it has no business-day walk, and the
     *     expression has no time override, duration, day filter or named timezone (a numeric offset
     *     is fine). {@link #emitRange} then emits no interval (bounds reversed by an offset), one
     *     interval, or a run of adjacent whole days, which the final merge joins into one. Any other range yields a count that follows
     *     the length of the range, the weekends in it or the clock changes of the zone, and gets no
     *     bound.</li>
     * </ul>
     * An exchange calendar splits intervals into sessions, so it gets no bound. Nor does an offset or
     * duration unit the timestamp driver does not know: the driver adds it as NULL, and the JIT
     * filter compares a NULL bound unlike the Java filter.
     */
    public int getMaxIntervalCount(int cap) {
        if (isParsedOnEvaluation() || exchangeSchedule != null) {
            return -1;
        }
        for (int i = 0; i < durationPartCount; i++) {
            if (!isKnownDurationUnit((char) ((ir.getQuick(durationOff + i) >>> 32) & 0xFFFF))) {
                return -1;
            }
        }
        final int perDayCount = Math.max(1, timeOverrideCount);
        // a named timezone with clock changes turns a local interval into up to three where clocks
        // fall back, unless the interval is a whole day
        final boolean hasRepeatedHours = tzRules != null && !tzRules.hasFixedOffset();
        final boolean isContiguousRange = timeOverrideCount == 0
                && durationPartCount == 0
                && dayFilterMask == 0
                && tzRules == null;
        long count = 0;
        LongList staticIntervals = null;
        int pos = elemOff;
        for (int i = 0; i < elemCount; i++) {
            final long word = ir.getQuick(pos);
            final long tag = word & TAG_MASK;
            if (tag == TAG_SINGLE_VAR) {
                if (!hasKnownOffsetUnit(word)) {
                    return -1;
                }
                final boolean isWholeDay = timeOverrideCount == 0
                        && durationPartCount == 0
                        && DateVariableExpr.isDayLevel(word);
                count += hasRepeatedHours && !isWholeDay ? 3L * perDayCount : perDayCount;
                pos++;
            } else if (tag == TAG_STATIC) {
                if (staticIntervals == null) {
                    staticIntervals = new LongList();
                }
                staticIntervals.add(ir.getQuick(pos + 1), ir.getQuick(pos + 2));
                pos += 3;
            } else {
                if (!isContiguousRange
                        || (word & RANGE_BD_BIT) != 0
                        || !hasKnownOffsetUnit(word)
                        || !hasKnownOffsetUnit(ir.getQuick(pos + 1))) {
                    return -1;
                }
                count++;
                pos += 2;
            }
            if (count > cap) {
                return -1;
            }
        }
        if (staticIntervals != null) {
            int emptyCount = 0;
            int writeIndex = 0;
            for (int i = 0, n = staticIntervals.size(); i < n; i += 2) {
                final long lo = staticIntervals.getQuick(i);
                final long hi = staticIntervals.getQuick(i + 1);
                if (lo > hi) {
                    emptyCount++;
                } else {
                    staticIntervals.setQuick(writeIndex++, lo);
                    staticIntervals.setQuick(writeIndex++, hi);
                }
            }
            staticIntervals.setPos(writeIndex);
            if (staticIntervals.size() > 2) {
                IntervalUtils.unionBracketExpandedIntervals(staticIntervals, 0);
            }
            count += staticIntervals.size() / 2 + emptyCount;
            if (count > cap) {
                return -1;
            }
        }
        return (int) count;
    }

    /**
     * Returns the number of intervals that the merges after the days of date ranges sorted, merged,
     * moved or compared to find where the new ones land, over all evaluations of this expression.
     */
    @TestOnly
    public long getRangeMergeIntervalCountForTesting() {
        return rangeMergeIntervalCount;
    }

    @Override
    public int getType() {
        return ColumnType.UNDEFINED;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        now = executionContext.getNow(timestampDriver.getTimestampType());
        isLenient = IntervalUtils.isTickEvaluationLenient(executionContext);
        checks = isLenient ? 0 : IntervalUtils.TICK_CHECK_ALL;
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    /**
     * Returns whether {@link #evaluate} parses the expression, as it does for one without IR, or
     * parses it first, see {@link #HDR_PARSE_FIRST_BIT}, rather than evaluating the IR. The parse
     * produces the intervals that an IR would, so only tests tell the forms apart this way.
     */
    public boolean isParsedOnEvaluation() {
        return ir == null || isParseFirst;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("tick('").val(expression).val("')");
    }

    /**
     * Checks the expression as its SQL compiles, against the given "now": an expression that does
     * not parse, and one with more intervals than the limit, fail with the error that evaluating
     * it would raise. A range that starts after it ends does not fail here, because whether it
     * does depends on the "now" the query runs at, which a prepared statement does not know yet.
     * <p>
     * SQL that the engine runs on its own evaluates without the limit, so this is what keeps an
     * expression with too many intervals out of a WAL UPDATE: the client sees the error before
     * the UPDATE is acknowledged.
     */
    public void validate(long now) throws SqlException {
        validate(now, new LongList());
    }

    /**
     * Checks the expression as {@link #validate(long)} does, and appends the intervals that it
     * evaluates to at the given "now" to outIntervals.
     */
    public void validate(long now, LongList outIntervals) throws SqlException {
        this.now = now;
        isLenient = false;
        checks = IntervalUtils.TICK_CHECK_INTERVAL_LIMIT;
        try {
            evaluate(outIntervals);
        } catch (CairoException e) {
            throw SqlException.$(e.getPosition(), e.getFlyweightMessage());
        } finally {
            checks = IntervalUtils.TICK_CHECK_ALL;
        }
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
     * IntervalUtils.compileTickExpr() runs both on the static intervals once and stores the result
     * for every "now", so neither may read "now".
     */
    private void applyElementSteps(LongList out, int startIndex) {
        if (dayFilterMask != 0 && exchangeSchedule == null) {
            applyDayFilter(out, startIndex);
        }
        applyTimezone(timestampDriver, ir.getQuick(IR_TZ_OFFSET_INDEX), tzRules, out, startIndex);
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

    /**
     * Emits the intervals of a date variable range, and returns the end of the intervals that the
     * last merge after one of its days left merged, or {@code mergedEnd}, that of an earlier range,
     * when none of its days merged.
     */
    private int emitRange(long startEncoded, long endEncoded, LongList out, int outStart, int mergedEnd) {
        long start = evaluateExpr(startEncoded);
        long end = evaluateExpr(endEncoded);
        // The expression, not its value, decides whether an endpoint is a whole day or a point
        // in time, so a point in time that falls exactly on midnight stays a point in time.
        boolean isStartDay = DateVariableExpr.isDayLevel(startEncoded);
        boolean isEndDay = DateVariableExpr.isDayLevel(endEncoded);
        // As in the parser, a whole-day endpoint starts the range at the start of its day, and
        // ends it at the end of its day, and a range that starts after it ends fails, or adds no
        // interval without the range order check
        final long rangeLo = isStartDay ? timestampDriver.startOfDay(start, 0) : start;
        final long rangeHi = isEndDay ? timestampDriver.endOfDay(timestampDriver.startOfDay(end, 0)) : end;
        if (rangeLo > rangeHi) {
            if ((checks & IntervalUtils.TICK_CHECK_RANGE_ORDER) != 0) {
                throw CairoException.nonCritical().position(position).put("Invalid date range: start is after end");
            }
            return mergedEnd;
        }

        if (!isStartDay && !isEndDay) {
            final int rangeStart = out.size();
            out.add(start, end);
            applyElementSteps(out, rangeStart);
            return mergedEnd;
        }

        // Iterate the days of the range, and clip their intervals to the endpoint that is a point
        // in time, if any. A whole-day endpoint covers its entire day. Like the parser, run the
        // steps of each day on its intervals, then merge all intervals so far once there are
        // enough of them, and stop when the merged ones exceed the limit. Only the intervals after
        // mergedEnd are new to a merge: the ones before it are the output of the last merge.
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
                rangeMergeIntervalCount += unionAndCheckLimit(out, outStart, mergedEnd, "Date range expansion");
                mergedEnd = out.size();
            }
            currentDay = timestampDriver.addDays(currentDay, 1);
        }
        return mergedEnd;
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
     * with the parser's error, or, where {@link IntervalUtils#isTickEvaluationLenient} holds,
     * appends nothing.
     */
    private void parse(LongList outIntervals) {
        final int outStart = outIntervals.size();
        try {
            IntervalUtils.parseTickExpr(
                    timestampDriver,
                    configuration,
                    expression,
                    0,
                    expression.length(),
                    position,
                    outIntervals,
                    parseSink,
                    now,
                    checks
            );
        } catch (SqlException e) {
            if (isLenient) {
                outIntervals.setPos(outStart);
                return;
            }
            throw CairoException.nonCritical().position(e.getPosition()).put(e.getFlyweightMessage());
        }
    }

    /**
     * Parses the expression as its bind variable, with each date of the list keeping its own extent,
     * see {@link #HDR_PARSE_FIRST_BIT}, and returns true, or fails with the error of the parser at its
     * position. Where {@link IntervalUtils#isTickEvaluationLenient} holds, it parses with the interval
     * limit too, which bounds the parse, and where the parse fails, it drops what the parse appended and
     * returns false, so that the IR evaluates the expression.
     */
    private boolean parseFirst(LongList outIntervals) {
        final int outStart = outIntervals.size();
        try {
            IntervalUtils.parseTickExpr(
                    timestampDriver,
                    configuration,
                    expression,
                    0,
                    expression.length(),
                    position,
                    outIntervals,
                    parseSink,
                    now,
                    isLenient ? checks | IntervalUtils.TICK_CHECK_INTERVAL_LIMIT : checks,
                    false,
                    true
            );
            return true;
        } catch (SqlException e) {
            outIntervals.setPos(outStart);
            if (isLenient) {
                return false;
            }
            throw CairoException.nonCritical().position(e.getPosition()).put(e.getFlyweightMessage());
        }
    }

    /**
     * Sorts and merges the intervals from {@code outStart} on, where those before {@code mergedEnd}
     * are the output of the last merge, see {@link IntervalUtils#unionAppendedIntervals}, and fails
     * when more of them remain than the limit, as the parser does after each day of a date variable
     * range, and at the end. Without the interval limit check, it only merges.
     *
     * @return the number of intervals that the merge sorted, merged, moved or compared
     */
    private int unionAndCheckLimit(LongList out, int outStart, int mergedEnd, String expansion) {
        final int mergeCount = IntervalUtils.unionAppendedIntervals(out, outStart, mergedEnd);
        final int maxIntervals = configuration.getSqlIntervalMaxIntervalsAfterMerge();
        if ((checks & IntervalUtils.TICK_CHECK_INTERVAL_LIMIT) != 0 && (out.size() - outStart) / 2 > maxIntervals) {
            out.setPos(outStart);
            throw CairoException.nonCritical().position(position)
                    .put(expansion).put(" produces too many intervals (max ").put(maxIntervals).put(')');
        }
        return mergeCount;
    }

    /**
     * Converts a UTC timestamp to the global timezone's local time,
     * so that Phase 3 (applyTimezone) can convert it back to UTC correctly.
     * Used for per-element timezone entries that are already in UTC.
     */
    private long utcToGlobalLocal(long utc) {
        long numericTzOffset = ir.getQuick(IR_TZ_OFFSET_INDEX);
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
