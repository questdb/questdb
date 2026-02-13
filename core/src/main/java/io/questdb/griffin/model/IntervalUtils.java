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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TickCalendarService;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Interval;
import io.questdb.std.LongGroupSort;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.FlyweightCharSequence;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public final class IntervalUtils {
    public static final int HI_INDEX = 1;
    public static final int LO_INDEX = 0;
    public static final int OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX = 2;
    public static final int PERIOD_COUNT_INDEX = 3;
    public static final int STATIC_LONGS_PER_DYNAMIC_INTERVAL = 4;

    private static final int FRIDAY = 5;
    // Day of week constants (Monday=1, Sunday=7, matching TimestampDriver.getDayOfWeek())
    private static final int MONDAY = 1;
    private static final int SATURDAY = 6;
    private static final int SUNDAY = 7;
    private static final int DAY_FILTER_WEEKEND = (1 << (SATURDAY - 1)) | (1 << (SUNDAY - 1)); // Sat-Sun
    private static final int THURSDAY = 4;
    private static final int TUESDAY = 2;
    private static final int WEDNESDAY = 3;
    // Day filter bitmask constants (bit 0 = Monday, bit 6 = Sunday)
    private static final int DAY_FILTER_WORKDAY = (1 << (MONDAY - 1)) | (1 << (TUESDAY - 1)) | (1 << (WEDNESDAY - 1))
            | (1 << (THURSDAY - 1)) | (1 << (FRIDAY - 1)); // Mon-Fri
    // compileTickExpr thread-locals (3 sinks + 1 LongList):
    //   tlCompileSink1  — day-filter stripping (effectiveSeq)
    //   tlCompileSink2  — time override parsing (parseSink)
    //   tlCompileSink3  — bracket expansion inside static elements (expansionSink)
    //   tlCompileTmp    — scratch list for intermediate parsing
    private static final ThreadLocal<StringSink> tlCompileSink1 = ThreadLocal.withInitial(StringSink::new);
    private static final ThreadLocal<StringSink> tlCompileSink2 = ThreadLocal.withInitial(StringSink::new);
    private static final ThreadLocal<StringSink> tlCompileSink3 = ThreadLocal.withInitial(StringSink::new);
    private static final ThreadLocal<LongList> tlCompileTmp = ThreadLocal.withInitial(LongList::new);
    private static final ThreadLocal<StringSink> tlDateVarSink = ThreadLocal.withInitial(StringSink::new);
    private static final ThreadLocal<FlyweightCharSequence> tlExchangeCs = ThreadLocal.withInitial(FlyweightCharSequence::new);
    private static final ThreadLocal<LongList> tlExchangeFilterTemp = ThreadLocal.withInitial(LongList::new);
    // Thread-local sinks for bracket expansion, isolated to avoid conflicts with other code.
    // Two sinks are needed for nested usage scenarios:
    //   1. parseTickExpr -> expandBracketsRecursive (uses tlSink1)
    //                           -> expandTimeListBracket
    //                           -> expandBracketsRecursive (needs tlSink2, since tlSink1 is in use)
    //   2. parseTickExpr with day filter (uses tlSink1 for dateSink)
    //                           -> expandDateList with brackets in elements
    //                           -> expandBracketsRecursive (needs tlSink2, since tlSink1 is in use)
    //   3. tlDateVarSink is used for date variable formatting (isolated from other sinks)
    private static final ThreadLocal<StringSink> tlSink1 = ThreadLocal.withInitial(StringSink::new);
    private static final ThreadLocal<StringSink> tlSink2 = ThreadLocal.withInitial(StringSink::new);

    /**
     * Formats a timestamp as "YYYY-MM-DD" into the given sink.
     *
     * @param timestampDriver the timestamp driver for extracting date parts
     * @param timestamp       the timestamp to format
     * @param sink            the sink to write to
     */
    public static void appendDate(TimestampDriver timestampDriver, long timestamp, CharSink<?> sink) {
        int year = timestampDriver.getYear(timestamp);
        int month = timestampDriver.getMonthOfYear(timestamp);
        int day = timestampDriver.getDayOfMonth(timestamp);

        MicrosFormatUtils.appendYear000(sink, year);
        sink.putAscii('-');
        MicrosFormatUtils.append0(sink, month);
        sink.putAscii('-');
        MicrosFormatUtils.append0(sink, day);
    }

    /**
     * Formats a timestamp with full precision (microseconds or nanoseconds depending on driver).
     * Uses the driver's native append method to ensure correct precision is preserved.
     * Used for $now which preserves time component with full sub-second precision.
     */
    public static void appendDateTime(TimestampDriver timestampDriver, long timestamp, CharSink<?> sink) {
        // Use the driver's append method which formats with correct precision
        // (microseconds for TIMESTAMP_MICRO, nanoseconds for TIMESTAMP_NANO)
        timestampDriver.append(sink, timestamp);
    }

    public static void applyLastEncodedInterval(TimestampDriver timestampDriver, LongList intervals) {
        int index = intervals.size() - 4;
        long lo = decodeIntervalLo(intervals, index);
        long hi = decodeIntervalHi(intervals, index);
        int period = decodePeriod(intervals, index);
        char periodType = decodePeriodType(intervals, index);
        int count = decodePeriodCount(intervals, index);

        intervals.setPos(index);
        if (periodType == PeriodType.NONE) {
            intervals.extendAndSet(index + 1, hi);
            intervals.setQuick(index, lo);
            return;
        }
        apply(timestampDriver, intervals, lo, hi, period, periodType, count);
    }

    /**
     * Pre-parses a tick expression containing date variables and returns a
     * {@link CompiledTickExpression} with all suffix data pre-parsed so that
     * runtime evaluation uses only long arithmetic (no string parsing).
     * <p>
     * The expression is also validated at compile time by running
     * {@link #parseTickExpr} with the current timestamp. This ensures
     * structural errors are caught early.
     *
     * @param timestampDriver the timestamp driver determining precision
     * @param configuration   the Cairo configuration
     * @param seq             the tick expression string
     * @param lo              start index (inclusive) within seq
     * @param lim             end index (exclusive) within seq
     * @param position        source position for error reporting
     * @return a compiled expression that can be re-evaluated with different "now" values
     * @throws SqlException if the expression is structurally invalid
     */
    public static CompiledTickExpression compileTickExpr(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence seq,
            int lo,
            int lim,
            int position
    ) throws SqlException {
        StringSink sink = tlCompileSink1.get();
        sink.clear();
        LongList tmp = tlCompileTmp.get();
        tmp.clear();

        // Phase 1: Strip whitespace and day filter
        int firstNonSpace = lo;
        while (firstNonSpace < lim && Chars.isAsciiWhitespace(seq.charAt(firstNonSpace))) {
            firstNonSpace++;
        }

        int dayFilterMarkerPos = findDayFilterMarker(seq, firstNonSpace, lim);
        int dayFilterMask = 0;
        int dayFilterHi = -1;
        LongList exchangeSchedule = null;

        if (dayFilterMarkerPos >= 0) {
            dayFilterHi = lim;
            for (int i = dayFilterMarkerPos + 1; i < lim; i++) {
                if (seq.charAt(i) == ';') {
                    dayFilterHi = i;
                    break;
                }
            }
            exchangeSchedule = getExchangeSchedule(configuration, seq, dayFilterMarkerPos + 1, dayFilterHi);
            if (exchangeSchedule == null) {
                dayFilterMask = parseDayFilter(seq, dayFilterMarkerPos + 1, dayFilterHi, position);
            }
        }

        CharSequence effectiveSeq;
        int effectiveSeqLo;
        int effectiveSeqLim;

        if (dayFilterMarkerPos >= 0) {
            sink.put(seq, firstNonSpace, dayFilterMarkerPos);
            sink.put(seq, dayFilterHi, lim);
            effectiveSeq = sink;
            effectiveSeqLo = 0;
            effectiveSeqLim = sink.length();
        } else {
            effectiveSeq = seq;
            effectiveSeqLo = firstNonSpace;
            effectiveSeqLim = lim;
        }

        if (effectiveSeqLo >= effectiveSeqLim) {
            throw SqlException.$(position, "Empty tick expression");
        }

        // Phase 2: Detect expression structure — find element list bounds and suffix start
        int elemListLo;
        int elemListHi;
        int suffixLo;
        boolean isBareVar;

        if (effectiveSeq.charAt(effectiveSeqLo) == '[') {
            int listEnd = compileFindClosingBracket(effectiveSeq, effectiveSeqLo + 1, effectiveSeqLim, position);
            elemListLo = effectiveSeqLo + 1;
            elemListHi = listEnd;
            suffixLo = listEnd + 1;
            isBareVar = false;
        } else {
            isBareVar = true;
            int exprEnd = compileFindBareVarEnd(effectiveSeq, effectiveSeqLo, effectiveSeqLim);
            elemListLo = effectiveSeqLo;
            elemListHi = exprEnd;
            suffixLo = exprEnd;
        }

        // Phase 3: Parse suffix — timezone, duration, and time overrides
        int durationHi = effectiveSeqLim;

        // 3a: Timezone
        int tzMarkerPos = findTimezoneMarker(effectiveSeq, suffixLo, durationHi);
        long numericTzOffset = Long.MIN_VALUE;
        TimeZoneRules tzRules = null;
        int tzContentHi = -1;

        if (tzMarkerPos >= 0) {
            int tzContentLo = tzMarkerPos + 1;
            tzContentHi = compileFindSemicolonOrEnd(effectiveSeq, tzContentLo, durationHi);
            try {
                long l = Dates.parseOffset(effectiveSeq, tzContentLo, tzContentHi);
                if (l != Long.MIN_VALUE) {
                    numericTzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                } else {
                    DateLocale dateLocale = configuration.getDefaultDateLocale();
                    tzRules = dateLocale.getZoneRules(
                            Numbers.decodeLowInt(dateLocale.matchZone(effectiveSeq, tzContentLo, tzContentHi)),
                            timestampDriver.getTZRuleResolution()
                    );
                }
            } catch (NumericException e) {
                throw SqlException.$(position, "invalid timezone: ").put(effectiveSeq, tzContentLo, tzContentHi);
            }
        }

        // 3b: Find duration semicolon
        int durationSemicolon = compileFindCharOrNeg1(effectiveSeq, suffixLo, durationHi, ';');

        // Build IR using LongList — no manual capacity management, no shift hack.
        // Layout: [header, numericTz, durationParts..., timeOverrides..., elements...]
        LongList irList = new LongList(64);
        irList.add(0L);               // [0] header placeholder
        irList.add(numericTzOffset);   // [1] numeric timezone offset

        // 3c: Duration parts
        int durationPartCount = 0;
        if (durationSemicolon >= 0) {
            durationPartCount = compileDurationParts(effectiveSeq, durationSemicolon + 1, durationHi, position, irList);
        }
        boolean hasDurationWithExchange = exchangeSchedule != null && durationPartCount > 0;

        // 3d: Time overrides
        int timeLo = suffixLo;
        int timeHi = compileSuffixTimeHi(tzMarkerPos, durationSemicolon, durationHi);

        // Second sink for parsing (sink may be effectiveSeq)
        StringSink parseSink = (effectiveSeq == sink) ? tlCompileSink2.get() : sink;
        parseSink.clear();

        int timeOverrideCount = 0;
        if (timeLo < timeHi) {
            timeOverrideCount = compileTimeOverrides(
                    timestampDriver, configuration, effectiveSeq,
                    timeLo, timeHi, position, irList, parseSink, tmp
            );
        }

        // Phase 4: Append elements to IR
        int elemCount = compileElements(
                timestampDriver, configuration, effectiveSeq,
                elemListLo, elemListHi, isBareVar,
                timeLo, timeHi, tzMarkerPos, durationSemicolon, durationHi, tzContentHi,
                dayFilterMask, exchangeSchedule,
                position, irList, parseSink, tmp
        );

        // Phase 5: Finalize header and extract IR array
        irList.setQuick(0,
                CompiledTickExpression.encodeHeader(
                        elemCount,
                        timeOverrideCount,
                        durationPartCount,
                        hasDurationWithExchange,
                        dayFilterMask
                )
        );

        return new CompiledTickExpression(
                timestampDriver,
                irList,
                seq.subSequence(lo, lim),
                tzRules,
                configuration.getDefaultDateLocale(),
                exchangeSchedule
        );
    }

    public static int decodeDayFilterMask(LongList intervals, int index) {
        // Day filter mask is stored in the high byte of periodCount
        int encodedPeriodCount = Numbers.decodeHighInt(intervals.getQuick(index + PERIOD_COUNT_INDEX));
        return (encodedPeriodCount >>> 24) & 0xFF;
    }

    public static long decodeIntervalHi(LongList out, int index) {
        return out.getQuick(index + HI_INDEX);
    }

    public static long decodeIntervalLo(LongList out, int index) {
        return out.getQuick(index + LO_INDEX);
    }

    public static int decodePeriod(LongList intervals, int index) {
        return Numbers.decodeLowInt(intervals.getQuick(index + PERIOD_COUNT_INDEX));
    }

    public static int decodePeriodCount(LongList intervals, int index) {
        // Mask out the high byte which contains dayFilterMask
        return Numbers.decodeHighInt(intervals.getQuick(index + PERIOD_COUNT_INDEX)) & 0x00FFFFFF;
    }

    public static char decodePeriodType(LongList intervals, int index) {
        int pts = Numbers.decodeHighShort(
                Numbers.decodeLowInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
        return (char) (pts - (int) Short.MIN_VALUE);
    }

    public static void encodeInterval(
            long lo,
            long hi,
            int period,
            char periodType,
            int periodCount,
            short operation,
            LongList out
    ) {
        encodeInterval(
                lo,
                hi,
                period,
                periodType,
                periodCount,
                IntervalDynamicIndicator.NONE,
                IntervalOperation.NONE,
                operation,
                0,
                out
        );
    }

    public static void encodeInterval(Interval interval, short operation, LongList out) {
        encodeInterval(interval.getLo(), interval.getHi(), operation, out);
    }

    public static void encodeInterval(
            long lo,
            long hi,
            int period,
            char periodType,
            int periodCount,
            short adjustment,
            short dynamicIndicator,
            short operation,
            int dayFilterMask,
            LongList out
    ) {
        // Encode dayFilterMask in high byte of periodCount (only uses 7 bits)
        int encodedPeriodCount = (periodCount & 0x00FFFFFF) | ((dayFilterMask & 0xFF) << 24);
        out.add(
                lo,
                hi,
                Numbers.encodeLowHighInts(
                        Numbers.encodeLowHighShorts(operation, (short) ((int) periodType + Short.MIN_VALUE)),
                        Numbers.encodeLowHighShorts(adjustment, dynamicIndicator)
                ),
                Numbers.encodeLowHighInts(period, encodedPeriodCount)
        );
    }

    public static void encodeInterval(
            long lo,
            long hi,
            int period,
            char periodType,
            int periodCount,
            short adjustment,
            short dynamicIndicator,
            short operation,
            LongList out
    ) {
        encodeInterval(lo, hi, period, periodType, periodCount, adjustment, dynamicIndicator, operation, 0, out);
    }

    public static void encodeInterval(
            long lo,
            long hi,
            short adjustment,
            short dynamicIndicator,
            short operation,
            LongList out
    ) {
        encodeInterval(lo, hi, 0, PeriodType.NONE, 1, adjustment, dynamicIndicator, operation, out);
    }

    public static void encodeInterval(long lo, long hi, short operation, LongList out) {
        encodeInterval(lo, hi, 0, PeriodType.NONE, 1, operation, out);
    }

    public static int findInterval(LongList intervals, long timestamp) {
        assert intervals.size() % 2 == 0 : "interval list has an odd size";
        int left = 0;
        int right = (intervals.size() >>> 1) - 1;
        while (left <= right) {
            int mid = (left + right) >>> 1;
            long lo = decodeIntervalLo(intervals, mid << 1);
            long hi = decodeIntervalHi(intervals, mid << 1);
            if (lo > timestamp) {
                right = mid - 1;
            } else if (hi < timestamp) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    public static short getEncodedAdjustment(LongList intervals, int index) {
        return Numbers.decodeLowShort(
                Numbers.decodeHighInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
    }

    public static short getEncodedDynamicIndicator(LongList intervals, int index) {
        return Numbers.decodeHighShort(
                Numbers.decodeHighInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
    }

    public static short getEncodedOperation(LongList intervals, int index) {
        return Numbers.decodeLowShort(
                Numbers.decodeLowInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
    }

    public static int getIntervalType(int timestampType) {
        assert ColumnType.isTimestamp(timestampType);
        return switch (timestampType) {
            case ColumnType.TIMESTAMP_MICRO -> ColumnType.INTERVAL_TIMESTAMP_MICRO;
            case ColumnType.TIMESTAMP_NANO -> ColumnType.INTERVAL_TIMESTAMP_NANO;
            default -> ColumnType.UNDEFINED;
        };
    }

    public static TimestampDriver getTimestampDriverByIntervalType(int intervalType) {
        assert ColumnType.isInterval(intervalType);
        if (intervalType == ColumnType.INTERVAL_TIMESTAMP_NANO) {
            return NanosTimestampDriver.INSTANCE;
        }
        return MicrosTimestampDriver.INSTANCE;
    }

    public static int getTimestampTypeByIntervalType(int intervalType) {
        assert ColumnType.isInterval(intervalType);
        return switch (intervalType) {
            case ColumnType.INTERVAL_RAW, ColumnType.INTERVAL_TIMESTAMP_MICRO -> ColumnType.TIMESTAMP_MICRO;
            case ColumnType.INTERVAL_TIMESTAMP_NANO -> ColumnType.TIMESTAMP_NANO;
            default -> ColumnType.UNDEFINED;
        };
    }

    /**
     * Intersects two lists of intervals compacted in one list in place.
     * Intervals to be chronologically ordered and result list will be ordered as well.
     * <p>
     * Treat a as 2 lists,
     * a: first from 0 to divider
     * b: from divider to the end of list
     *
     * @param concatenatedIntervals 2 lists of intervals concatenated in 1
     */
    public static void intersectInPlace(LongList concatenatedIntervals, int dividerIndex) {
        final int sizeA = dividerIndex / 2;
        final int sizeB = sizeA + (concatenatedIntervals.size() - dividerIndex) / 2;
        int aLower = 0;

        int intervalB = sizeA;
        int writePoint = 0;

        int aUpperSize = sizeB;
        int aUpper = sizeB;

        while ((aLower < sizeA || aUpper < aUpperSize) && intervalB < sizeB) {
            int intervalA = aUpper < aUpperSize ? aUpper : aLower;
            long aLo = concatenatedIntervals.getQuick(intervalA * 2);
            long aHi = concatenatedIntervals.getQuick(intervalA * 2 + 1);

            long bLo = concatenatedIntervals.getQuick(intervalB * 2);
            long bHi = concatenatedIntervals.getQuick(intervalB * 2 + 1);

            if (aHi < bLo) {
                // a fully above b
                // a loses
                if (aUpper < aUpperSize) {
                    aUpper++;
                } else {
                    aLower++;
                }
            } else if (aLo > bHi) {
                // a fully below b
                // b loses
                intervalB++;
            } else {
                if (aHi < bHi) {
                    // b hanging lower than a
                    // a loses
                    if (aUpper < aUpperSize) {
                        aUpper++;
                    } else {
                        aLower++;
                    }
                } else {
                    // otherwise a lower than b
                    // a loses
                    intervalB++;
                }

                assert writePoint <= aLower || writePoint >= sizeA;
                if (writePoint == aLower && aLower < sizeA) {
                    // We cannot keep A position, it will be overwritten, hence intervalB++; is not possible
                    // Copy a point to A area instead
                    concatenatedIntervals.add(
                            concatenatedIntervals.getQuick(writePoint * 2),
                            concatenatedIntervals.getQuick(writePoint * 2 + 1)
                    );
                    aUpperSize = concatenatedIntervals.size() / 2;
                    aLower++;
                }

                writePoint = append(concatenatedIntervals, writePoint, Math.max(aLo, bLo), Math.min(aHi, bHi));
            }
        }

        concatenatedIntervals.setPos(2 * writePoint);
    }

    /**
     * Inverts intervals. This method also produces inclusive edges that differ from source ones by 1.
     *
     * @param intervals collection of intervals
     * @see #invert(LongList, int)
     */
    public static void invert(LongList intervals) {
        invert(intervals, 0);
    }

    /**
     * Calculates the complement for a list of sorted, non-overlapping intervals. This operation
     * finds all the "gaps" between the given intervals, covering the full range from
     * {@code Long.MIN_VALUE} to {@code Long.MAX_VALUE}.
     * <p>
     * The calculation is performed <strong>in-place</strong>, overwriting the original list
     * content starting from {@code startIndex} with the resulting inverted intervals. The list's
     * logical size is adjusted to reflect the result.
     * <p>
     * <strong>Precondition:</strong> For the result to be correct, the source intervals in the list must be
     * sorted in ascending order and must not overlap.
     * <p>
     * <b>Example:</b>
     * <pre>
     * // Source intervals: [100, 200], [500, 600]
     * LongList list = new LongList(new long[]{100, 200, 500, 600});
     * invert(list, 0);
     * // After inversion, list contains: [Long.MIN_VALUE, 99, 201, 499, 601, Long.MAX_VALUE]
     * </pre>
     *
     * @param intervals  A {@code LongList} containing a flat sequence of sorted, non-overlapping,
     *                   inclusive intervals (e.g., {@code [start1, end1, start2, end2, ...]}).
     *                   This list will be modified directly.
     * @param startIndex The index in the list from which to start processing. Elements
     *                   before this index are ignored and preserved.
     */
    public static void invert(LongList intervals, int startIndex) {
        long last = Long.MIN_VALUE;
        int n = intervals.size();
        int writeIndex = startIndex;
        for (int i = startIndex; i < n; i += 2) {
            final long lo = intervals.getQuick(i);
            final long hi = intervals.getQuick(i + 1);
            if (lo > last) {
                intervals.setQuick(writeIndex, last);
                intervals.setQuick(writeIndex + 1, lo - 1);
                writeIndex += 2;
            }
            last = hi + 1;
        }

        // If last hi was Long.MAX_VALUE then last will be Long.MIN_VALUE after +1 overflow
        if (last != Long.MIN_VALUE) {
            intervals.extendAndSet(writeIndex + 1, Long.MAX_VALUE);
            intervals.setQuick(writeIndex, last);
            writeIndex += 2;
        }

        intervals.setPos(writeIndex);
    }

    // Checks if the timestamp is in the intervals, both sides inclusive.
    public static boolean isInIntervals(LongList intervals, long timestamp) {
        return findInterval(intervals, timestamp) != -1;
    }

    /**
     * Parses a TICK (Temporal Interval Calendar Kit) interval string with bracket expansion,
     * timezone support, and duration suffixes. This method is the main entry point for parsing
     * complex interval expressions that can expand to multiple disjoint time intervals.
     *
     * <h4>Supported Features</h4>
     * <ul>
     *   <li><b>Bracket Expansion:</b> {@code [a,b,c]} for comma-separated values,
     *       {@code [a..b]} for inclusive ranges</li>
     *   <li><b>Date Lists:</b> {@code [date1,date2,...]} for non-contiguous dates</li>
     *   <li><b>Time Lists:</b> {@code T[09:00,14:30]} for multiple complete times</li>
     *   <li><b>Timezone Support:</b> {@code @timezone} for DST-aware conversion</li>
     *   <li><b>Duration Suffix:</b> {@code ;6h}, {@code ;30m}, {@code ;1h30m} for interval duration</li>
     *   <li><b>Cartesian Product:</b> Multiple bracket groups combine all possibilities</li>
     * </ul>
     *
     * <h4>Bracket Expansion Examples</h4>
     * <pre>{@code
     * // Comma-separated values - expands to days 10, 15, 20 of January 2024
     * "2024-01-[10,15,20]"
     *
     * // Inclusive range - expands to days 10, 11, 12
     * "2024-01-[10..12]"
     *
     * // Mixed values and ranges - expands to days 5, 10, 11, 12, 20
     * "2024-01-[5,10..12,20]"
     *
     * // Multiple bracket groups (Cartesian product) - 4 intervals
     * "2024-[01,06]-[10,15]"  // Jan 10, Jan 15, Jun 10, Jun 15
     *
     * // With duration suffix - two 1-hour intervals
     * "2024-01-[10,15]T10:30;1h"
     * }</pre>
     *
     * <h4>Date List Examples</h4>
     * <pre>{@code
     * // Non-contiguous dates
     * "[2024-01-15,2024-03-20,2024-06-01]"
     *
     * // Date list with nested field expansion
     * "[2024-12-31,2025-01-[03..05]]"  // Dec 31, Jan 3, Jan 4, Jan 5
     *
     * // Date list with time suffix
     * "[2024-01-15,2024-01-20]T09:30;6h30m"
     * }</pre>
     *
     * <h4>Time List Examples</h4>
     * <pre>{@code
     * // Multiple times on same day (note: colon inside bracket = time list)
     * "2024-01-15T[09:00,14:30,18:00];1h"  // Three 1-hour intervals
     *
     * // Contrast with numeric expansion (no colon inside bracket)
     * "2024-01-15T[09,14]:30"  // Expands hour field only -> 09:30 and 14:30
     *
     * // Per-element timezone in time list
     * "2024-01-15T[09:00@UTC,14:30@Europe/London];1h"
     * }</pre>
     *
     * <h4>Timezone Examples</h4>
     * <pre>{@code
     * // Numeric offset: 08:00 in UTC+3 = 05:00 UTC
     * "2024-01-15T08:00@+03:00"
     *
     * // Named timezone with DST awareness
     * "2024-07-15T08:00@Europe/London"  // Summer: 08:00 BST = 07:00 UTC
     * "2024-01-15T08:00@Europe/London"  // Winter: 08:00 GMT = 08:00 UTC
     *
     * // Timezone with bracket expansion and duration
     * "2024-01-[15..19]T09:30@America/New_York;6h30m"
     * }</pre>
     *
     * <h4>Multi-Unit Duration Examples</h4>
     * <pre>{@code
     * // Single unit (traditional)
     * "2024-01-15T09:00;1h"
     *
     * // Multi-unit duration
     * "2024-01-15T09:00;1h30m"      // 1 hour 30 minutes
     * "2024-01-15T09:00;2h15m30s"   // 2 hours 15 minutes 30 seconds
     * "2024-01-15T09:00;500T250u"   // 500 milliseconds + 250 microseconds
     *
     * // Supported units: y(years), M(months), w(weeks), d(days), h(hours),
     * //                  m(minutes), s(seconds), T(milliseconds), u(microseconds), n(nanoseconds)
     * }</pre>
     *
     * <h4>Output Format</h4>
     * <p>Intervals are appended to the {@code out} list. In static mode ({@code applyEncoded=true}),
     * each interval is stored as 2 longs: {@code [lo, hi]}. In dynamic mode ({@code applyEncoded=false}),
     * each interval is stored as 4 longs: {@code [lo, hi, operation|periodType|adjustment, period|count]}.
     * When multiple intervals are generated, they are automatically unioned (merged if overlapping).</p>
     *
     * @param timestampDriver the timestamp driver determining precision (microseconds or nanoseconds)
     * @param seq             the interval string to parse (e.g., {@code "2024-01-[10,15]T09:00;1h"})
     * @param lo              start index (inclusive) within {@code seq} to parse from
     * @param lim             end index (exclusive) within {@code seq} to parse to
     * @param position        source position for error reporting (typically the token position in SQL)
     * @param out             output list where parsed intervals will be appended as lo/hi pairs
     * @param operation       the interval operation type (e.g., {@link IntervalOperation#INTERSECT})
     * @param sink            a reusable string sink for internal string manipulation; contents may be modified
     * @param applyEncoded    if {@code true}, intervals are fully resolved (static mode);
     *                        if {@code false}, interval metadata is preserved for runtime evaluation (dynamic mode)
     * @throws SqlException if the interval string is malformed (e.g., unclosed bracket, invalid range,
     *                      empty bracket, invalid timezone, invalid duration format)
     * @see IntervalOperation
     * @see TimestampDriver
     */
    public static void parseTickExpr(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation,
            StringSink sink,
            boolean applyEncoded
    ) throws SqlException {
        parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, out, operation, sink, applyEncoded, timestampDriver.getTicks());
    }

    /**
     * Parses a TICK interval expression with support for date variables ($today, $now, etc.).
     * This overload allows specifying a custom "now" timestamp for deterministic testing.
     *
     * @param timestampDriver the timestamp driver determining precision (microseconds or nanoseconds)
     * @param configuration   the Cairo configuration
     * @param seq             the interval string to parse
     * @param lo              start index (inclusive) within seq
     * @param lim             end index (exclusive) within seq
     * @param position        source position for error reporting
     * @param out             output list where parsed intervals will be appended
     * @param operation       the interval operation type
     * @param sink            a reusable string sink for internal string manipulation
     * @param applyEncoded    if true, intervals are fully resolved (static mode)
     * @param nowTimestamp    the timestamp to use for resolving date variables ($today, $now, etc.)
     */
    public static void parseTickExpr(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation,
            StringSink sink,
            boolean applyEncoded,
            long nowTimestamp
    ) throws SqlException {
        assert configuration.getSqlIntervalMaxIntervalsAfterMerge() > configuration.getSqlIntervalIncrementalMergeThreshold()
                : "sqlIntervalMaxIntervalsAfterMerge must be greater than sqlIntervalIncrementalMergeThreshold";
        // Skip leading whitespace
        int firstNonSpace = lo;
        while (firstNonSpace < lim && Chars.isAsciiWhitespace(seq.charAt(firstNonSpace))) {
            firstNonSpace++;
        }

        // Find day filter marker (#) - must be outside brackets and before ; (duration)
        // Timezone is optional: "2024-01-01#Mon" and "2024-01-01@+05:00#Mon" are both valid
        // Day filter or tick calendar is applied based on LOCAL time (before timezone conversion)
        int dayFilterMarkerPos = findDayFilterMarker(seq, firstNonSpace, lim);
        int dayFilterMask = 0;
        int dayFilterHi = -1;
        LongList exchangeSchedule = null;

        if (dayFilterMarkerPos >= 0) {
            // Find end of filter (at ; or end of string)
            dayFilterHi = lim;
            for (int i = dayFilterMarkerPos + 1; i < lim; i++) {
                if (seq.charAt(i) == ';') {
                    dayFilterHi = i;
                    break;
                }
            }
            // First try to look up as tick calendar
            exchangeSchedule = getExchangeSchedule(configuration, seq, dayFilterMarkerPos + 1, dayFilterHi);
            if (exchangeSchedule == null) {
                // Not an tick calendar, parse as day filter
                dayFilterMask = parseDayFilter(seq, dayFilterMarkerPos + 1, dayFilterHi, position);
            }
        }

        // Determine effective limits after removing day filter
        int effectiveSeqLo = firstNonSpace;
        int effectiveSeqLim = lim;
        CharSequence effectiveSeq = seq;

        if (dayFilterMarkerPos >= 0) {
            // Reconstruct the string without the day filter part
            sink.clear();
            sink.put(seq, firstNonSpace, dayFilterMarkerPos);
            sink.put(seq, dayFilterHi, lim);
            effectiveSeq = sink;
            effectiveSeqLo = 0;
            effectiveSeqLim = sink.length();
        }

        // Check if this is a date list (starts with '[')
        // A date list looks like: [date1,date2,...] or [date1,date2,...]T09:30;1h
        // Each date element can contain field expansion brackets like: [2025-12-31,2026-01-[03..05]]
        //
        // To distinguish from field expansion like [1]-[1]-[1]T..., we check:
        // - Find the first ']' that matches the opening '[' (depth 0)
        // - If it's followed by end-of-string, 'T', ';', ',', or whitespace -> date list
        // - If it's followed by '-', ':', '.', or digit -> field expansion (part of date format)
        if (effectiveSeqLo < effectiveSeqLim && effectiveSeq.charAt(effectiveSeqLo) == '[' && isDateList(effectiveSeq, effectiveSeqLo, effectiveSeqLim)) {
            int outSize = out.size();
            StringSink dateSink = dayFilterMarkerPos >= 0 ? tlSink1.get() : sink;
            dateSink.clear();
            try {
                // When global exchange schedule + duration, exclude duration from date list expansion
                // (duration will be applied after the exchange filter by applyExchangeFilterAndDuration)
                int expandDateListLim = effectiveSeqLim;
                int globalSemicolon = -1;
                if (exchangeSchedule != null) {
                    globalSemicolon = findDurationSemicolon(effectiveSeq, effectiveSeqLo, effectiveSeqLim);
                    if (globalSemicolon >= 0) {
                        expandDateListLim = globalSemicolon;
                    }
                }
                expandDateList(
                        timestampDriver,
                        configuration,
                        effectiveSeq,
                        effectiveSeqLo,
                        expandDateListLim,
                        position,
                        out,
                        operation,
                        dateSink,
                        applyEncoded,
                        outSize,
                        dayFilterMask,
                        nowTimestamp
                );
                // Apply tick calendar filter if specified
                if (exchangeSchedule != null) {
                    applyExchangeFilterAndDuration(timestampDriver, exchangeSchedule, out, outSize, effectiveSeq,
                            globalSemicolon >= 0 ? globalSemicolon + 1 : -1, effectiveSeqLim, position);
                }
                // In static mode, union all bracket-expanded intervals and validate count
                if (applyEncoded) {
                    mergeAndValidateIntervals(configuration, out, outSize, position);
                }
            } catch (SqlException e) {
                out.setPos(outSize);
                throw e;
            }
            return;
        }

        // Check for bare date variable expression (starts with '$' without brackets)
        // e.g., '$now - 2h..$now' or '$today + 5d'
        // We wrap it in brackets and process as a date list
        if (effectiveSeqLo < effectiveSeqLim && effectiveSeq.charAt(effectiveSeqLo) == '$') {
            int outSize = out.size();
            StringSink dateSink = dayFilterMarkerPos >= 0 ? tlSink1.get() : sink;
            dateSink.clear();

            // Find where the date variable expression ends (before suffix like T, @, ;)
            // Note: '#' (day filter) is already stripped from effectiveSeq at this point
            // Note: 'T' is only a time suffix if followed by a digit (e.g., T09:30)
            // This allows variable names containing 'T' like $TOMORROW
            // Note: ',' is a stop character - comma lists require brackets
            int exprEnd = effectiveSeqLo;
            while (exprEnd < effectiveSeqLim) {
                char c = effectiveSeq.charAt(exprEnd);
                // Stop at suffix markers (but allow '..' for ranges, and '+'/'-' for arithmetic)
                if (c == '@' || c == ';' || c == ',') {
                    break;
                }
                // 'T' is only a time suffix if followed by a digit
                if (c == 'T' && exprEnd + 1 < effectiveSeqLim && Chars.isAsciiDigit(effectiveSeq.charAt(exprEnd + 1))) {
                    break;
                }
                exprEnd++;
            }

            // Reject bare comma lists - they require brackets
            if (exprEnd < effectiveSeqLim && effectiveSeq.charAt(exprEnd) == ',') {
                throw SqlException.$(position, "comma-separated date lists require brackets, e.g., [$now,$tomorrow]");
            }

            // Wrap in brackets: "$now - 2h" -> "[$now - 2h]"
            StringSink wrappedSink = tlSink2.get();
            wrappedSink.clear();
            wrappedSink.putAscii('[');
            wrappedSink.put(effectiveSeq, effectiveSeqLo, exprEnd);
            wrappedSink.putAscii(']');
            // Append any suffix (T09:30, @timezone, ;duration, #dayfilter)
            if (exprEnd < effectiveSeqLim) {
                wrappedSink.put(effectiveSeq, exprEnd, effectiveSeqLim);
            }

            try {
                expandDateList(
                        timestampDriver,
                        configuration,
                        wrappedSink,
                        0,
                        wrappedSink.length(),
                        position,
                        out,
                        operation,
                        dateSink,
                        applyEncoded,
                        outSize,
                        dayFilterMask,
                        nowTimestamp
                );
                // Apply tick calendar filter if specified
                if (exchangeSchedule != null) {
                    int semicolon = findDurationSemicolon(wrappedSink, 0, wrappedSink.length());
                    applyExchangeFilterAndDuration(timestampDriver, exchangeSchedule, out, outSize, wrappedSink,
                            semicolon >= 0 ? semicolon + 1 : -1, wrappedSink.length(), position);
                }
                if (applyEncoded) {
                    mergeAndValidateIntervals(configuration, out, outSize, position);
                }
            } catch (SqlException e) {
                out.setPos(outSize);
                throw e;
            }
            return;
        }

        // Single scan: detect brackets, find semicolon and timezone marker positions
        int dateLim = effectiveSeqLim;
        int semicolonPos = -1;
        int depth = 0;
        boolean hasBrackets = false;
        for (int i = effectiveSeqLo; i < effectiveSeqLim; i++) {
            char c = effectiveSeq.charAt(i);
            if (c == '[') {
                hasBrackets = true;
                depth++;
            } else if (c == ']') {
                depth--;
            } else if (c == ';' && depth == 0) {
                semicolonPos = i;
                dateLim = i;
                break;
            }
        }

        // Find timezone marker (@ before semicolon, outside brackets)
        int tzMarkerPos = findTimezoneMarker(effectiveSeq, effectiveSeqLo, semicolonPos >= 0 ? semicolonPos : effectiveSeqLim);
        int tzLo = -1;
        int tzHi = -1;
        int effectiveDateLim = dateLim;
        int effectiveLim = effectiveSeqLim;

        // Use a separate sink for timezone reconstruction to avoid conflicts
        StringSink tzSink = dayFilterMarkerPos >= 0 ? tlSink1.get() : sink;

        if (tzMarkerPos >= 0) {
            // Extract timezone bounds
            tzLo = tzMarkerPos + 1;
            tzHi = semicolonPos >= 0 ? semicolonPos : effectiveSeqLim;
            // Adjust date limit to exclude timezone
            effectiveDateLim = tzMarkerPos;
            // Build the interval string without timezone but with duration suffix
            // e.g., "2024-01-01T08:00@Europe/London;1h" -> "2024-01-01T08:00;1h"
            if (semicolonPos >= 0) {
                // Has duration suffix - we need to reconstruct the string
                tzSink.clear();
                tzSink.put(effectiveSeq, effectiveSeqLo, tzMarkerPos);
                tzSink.put(effectiveSeq, semicolonPos, effectiveSeqLim);
                effectiveLim = tzSink.length();
                effectiveDateLim = tzMarkerPos - effectiveSeqLo; // Position in tzSink
            } else {
                effectiveLim = tzMarkerPos;
            }
        }

        int outSize = out.size();

        if (!hasBrackets) {
            // When tick calendar filter AND duration are both present, parse without duration first,
            // apply the filter, then extend intervals with duration
            boolean hasDurationWithExchange = exchangeSchedule != null && semicolonPos >= 0;
            if (hasDurationWithExchange) {
                // Parse without duration - use dateLim which excludes the duration suffix
                if (tzMarkerPos >= 0) {
                    // Reconstruct without timezone and without duration
                    parseIntervalSuffix(timestampDriver, effectiveSeq, effectiveSeqLo, tzMarkerPos, position, out, operation);
                } else {
                    parseIntervalSuffix(timestampDriver, effectiveSeq, effectiveSeqLo, dateLim, position, out, operation);
                }
            } else if (tzMarkerPos >= 0 && semicolonPos >= 0) {
                // Parse from reconstructed tzSink
                parseIntervalSuffix(timestampDriver, tzSink, 0, effectiveLim, position, out, operation);
            } else if (tzMarkerPos >= 0) {
                // No semicolon, just truncate at timezone marker
                parseIntervalSuffix(timestampDriver, effectiveSeq, effectiveSeqLo, effectiveLim, position, out, operation);
            } else {
                parseIntervalSuffix(timestampDriver, effectiveSeq, effectiveSeqLo, effectiveSeqLim, position, out, operation);
            }
            if (applyEncoded) {
                applyLastEncodedInterval(timestampDriver, out);
                // Day filter applies BEFORE timezone conversion (based on local time)
                if (dayFilterMask != 0 && exchangeSchedule == null) {
                    applyDayFilter(
                            timestampDriver,
                            out,
                            outSize,
                            dayFilterMask,
                            hasDatePrecision(effectiveSeq, effectiveSeqLo, effectiveDateLim)
                    );
                }
            } else if (dayFilterMask != 0 && exchangeSchedule == null) {
                // Dynamic mode: store day filter mask for runtime evaluation
                setDayFilterMaskOnEncodedIntervals(out, outSize, dayFilterMask);
            }
            // Apply timezone conversion if present (before tick calendar filter)
            if (tzMarkerPos >= 0) {
                applyTimezoneToIntervals(timestampDriver, configuration, out, outSize, effectiveSeq, tzLo, tzHi, position, applyEncoded);
            }
            // Tick calendar filter applies AFTER timezone conversion (intersects with UTC trading hours)
            if (exchangeSchedule != null) {
                applyExchangeFilterAndDuration(timestampDriver, exchangeSchedule, out, outSize, effectiveSeq,
                        semicolonPos >= 0 ? semicolonPos + 1 : -1, effectiveSeqLim, position);
            }
            return;
        }

        StringSink reconstructSink = dayFilterMarkerPos >= 0 ? tlSink2.get() : sink;
        reconstructSink.clear();
        CharSequence parseSeq = effectiveSeq;
        int parseLo = effectiveSeqLo;
        int parseDateLim = effectiveDateLim;
        int parseLim = effectiveSeqLim;

        // When tick calendar filter AND duration are both present, exclude duration from parsing
        // Duration will be applied after the tick calendar filter
        boolean excludeDurationFromParsing = exchangeSchedule != null && semicolonPos >= 0;

        if (tzMarkerPos >= 0) {
            // Reconstruct the string without timezone
            reconstructSink.put(effectiveSeq, effectiveSeqLo, tzMarkerPos);
            if (semicolonPos >= 0 && !excludeDurationFromParsing) {
                reconstructSink.put(effectiveSeq, semicolonPos, effectiveSeqLim);
            }
            parseSeq = reconstructSink;
            parseLo = 0;
            parseDateLim = tzMarkerPos - effectiveSeqLo;
            parseLim = reconstructSink.length();
        } else if (excludeDurationFromParsing) {
            // No timezone but need to exclude duration - use dateLim
            parseLim = dateLim;
        }

        StringSink expansionSink = tlSink1.get();
        expansionSink.clear();
        try {
            boolean hadTimeListBracket = expandBracketsRecursive(
                    timestampDriver,
                    configuration,
                    parseSeq,
                    parseLo,
                    parseDateLim,
                    parseLim,
                    position,
                    out,
                    operation,
                    expansionSink,
                    0,
                    applyEncoded,
                    outSize,
                    effectiveSeq, // globalTzSeq - sequence with timezone
                    tzLo,         // globalTzLo
                    tzHi          // globalTzHi
            );
            // Day filter applies BEFORE timezone conversion (based on local time)
            if (dayFilterMask != 0 && exchangeSchedule == null) {
                if (applyEncoded) {
                    applyDayFilter(
                            timestampDriver,
                            out,
                            outSize,
                            dayFilterMask,
                            hasDatePrecision(parseSeq, parseLo, parseDateLim)
                    );
                } else {
                    // Dynamic mode: store day filter mask for runtime evaluation
                    setDayFilterMaskOnEncodedIntervals(out, outSize, dayFilterMask);
                }
            }
            // Apply timezone conversion if present AND no time list brackets were processed
            // (time list brackets handle their own timezone internally)
            if (tzMarkerPos >= 0 && !hadTimeListBracket) {
                applyTimezoneToIntervals(timestampDriver, configuration, out, outSize, effectiveSeq, tzLo, tzHi, position, applyEncoded);
            }
            // Tick calendar filter applies AFTER timezone conversion (intersects with UTC trading hours)
            if (exchangeSchedule != null) {
                applyExchangeFilterAndDuration(timestampDriver, exchangeSchedule, out, outSize, effectiveSeq,
                        semicolonPos >= 0 ? semicolonPos + 1 : -1, effectiveSeqLim, position);
            }
            // In static mode, union all bracket-expanded intervals with each other
            // (they were added without union during expansion)
            if (applyEncoded && out.size() > outSize + 2) {
                unionBracketExpandedIntervals(out, outSize);
                // Check final interval count against limit
                int intervalCount = (out.size() - outSize) / 2;
                int maxIntervalsAfterMerge = configuration.getSqlIntervalMaxIntervalsAfterMerge();
                if (intervalCount > maxIntervalsAfterMerge) {
                    throw SqlException.$(position, "Bracket expansion produces too many intervals (max ")
                            .put(maxIntervalsAfterMerge).put(')');
                }
            }
        } catch (SqlException e) {
            // Unwind output on error
            out.setPos(outSize);
            throw e;
        }
    }

    public static void parseTickExprAndIntersect(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            @Nullable CharSequence seq,
            LongList out,
            int position,
            StringSink sink,
            boolean applyEncoded
    ) throws SqlException {
        if (seq != null) {
            parseTickExpr(timestampDriver, configuration, seq, 0, seq.length(), position, out, IntervalOperation.INTERSECT, sink, applyEncoded);
        } else {
            // null expression
            encodeInterval(Numbers.LONG_NULL, Numbers.LONG_NULL, IntervalOperation.INTERSECT, out);
            if (applyEncoded) {
                applyLastEncodedInterval(timestampDriver, out);
            }
        }
    }

    public static void replaceHiLoInterval(long lo, long hi, int period, char periodType, int periodCount, short operation, LongList out) {
        int lastIndex = out.size() - 4;
        out.setQuick(lastIndex, lo);
        out.setQuick(lastIndex + HI_INDEX, hi);
        out.setQuick(
                lastIndex + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX,
                Numbers.encodeLowHighInts(
                        Numbers.encodeLowHighShorts(operation, (short) ((int) periodType + Short.MIN_VALUE)),
                        0
                )
        );
        out.setQuick(lastIndex + PERIOD_COUNT_INDEX, Numbers.encodeLowHighInts(period, periodCount));
    }

    /**
     * Sets the day filter mask on intervals in dynamic (4-long) format.
     * This modifies encoded intervals in-place to add day filtering for runtime evaluation.
     *
     * @param intervals     the interval list containing encoded intervals
     * @param startIndex    start index of intervals to modify
     * @param dayFilterMask bitmask for day-of-week filter (bits 0-6 for Mon-Sun)
     */
    public static void setDayFilterMaskOnEncodedIntervals(LongList intervals, int startIndex, int dayFilterMask) {
        if (dayFilterMask == 0) {
            return;
        }
        for (int i = startIndex; i < intervals.size(); i += STATIC_LONGS_PER_DYNAMIC_INTERVAL) {
            long periodCountLong = intervals.getQuick(i + PERIOD_COUNT_INDEX);
            int period = Numbers.decodeLowInt(periodCountLong);
            int periodCount = Numbers.decodeHighInt(periodCountLong);
            // Clear existing dayFilterMask bits and set new ones
            int newPeriodCount = (periodCount & 0x00FFFFFF) | ((dayFilterMask & 0xFF) << 24);
            intervals.setQuick(i + PERIOD_COUNT_INDEX, Numbers.encodeLowHighInts(period, newPeriodCount));
        }
    }

    public static void subtract(LongList intervals, int divider) {
        IntervalUtils.invert(intervals, divider);
        IntervalUtils.intersectInPlace(intervals, divider);
    }

    /**
     * Unions two lists of intervals compacted in one list in place.
     * Intervals to be chronologically ordered and result list will be ordered as well.
     * <p>
     * Treat a as 2 lists,
     * a: first from 0 to divider
     * b: from divider to the end of list
     *
     * @param intervals 2 lists of intervals concatenated in 1
     */
    public static void unionInPlace(LongList intervals, int dividerIndex) {
        final int sizeB = dividerIndex + (intervals.size() - dividerIndex);
        int aLower = 0;

        int intervalB = dividerIndex;
        int writePoint = 0;

        int aUpperSize = sizeB;
        int aUpper = sizeB;
        long aLo = 0, aHi = 0, bLo = 0, bHi = 0;

        while (aLower < dividerIndex || aUpper < aUpperSize || intervalB < sizeB) {
            // This tries to get either interval from A or from B
            // where it's available
            // and union with last interval in writePoint position
            boolean hasA = aLower < dividerIndex || aUpper < aUpperSize;
            if (hasA) {
                int intervalA = aUpper < aUpperSize ? aUpper : aLower;
                aLo = intervals.getQuick(intervalA);
                aHi = intervals.getQuick(intervalA + 1);
            }

            boolean hasB = intervalB < sizeB;
            if (hasB) {
                bLo = intervals.getQuick(intervalB);
                bHi = intervals.getQuick(intervalB + 1);
            }

            long nextLo, nextHi;

            if (hasA) {
                if (hasB && bLo < aLo) {
                    nextLo = bLo;
                    nextHi = bHi;
                    intervalB += 2;
                } else {
                    nextLo = aLo;
                    nextHi = aHi;
                    if (aUpper < aUpperSize) {
                        aUpper += 2;
                    } else {
                        aLower += 2;
                    }
                }
            } else {
                nextLo = bLo;
                nextHi = bHi;
                intervalB += 2;
            }

            if (writePoint > 0) {
                long prevHi = intervals.getQuick(writePoint - 1);
                if (nextLo <= prevHi) {
                    // Intersection with a previously saved interval
                    intervals.setQuick(writePoint - 1, Math.max(nextHi, prevHi));
                    continue;
                }
            }

            // new interval to save
            assert writePoint <= aLower || writePoint >= dividerIndex;
            if (writePoint == aLower && aLower < dividerIndex) {
                // We cannot keep A position, it will be overwritten
                // Copy a point to A area instead
                intervals.add(
                        intervals.getQuick(writePoint),
                        intervals.getQuick(writePoint + 1)
                );
                aUpperSize = intervals.size();
                aLower += 2;
            }
            intervals.setQuick(writePoint++, nextLo);
            intervals.setQuick(writePoint++, nextHi);
        }

        intervals.setPos(writePoint);
    }

    /**
     * Adds a duration string to a timestamp. Supports multi-unit format like "5h3m31s".
     * Supported units: y (years), M (months), w (weeks), d (days), h (hours),
     * m (minutes), s (seconds), T (millis), u (micros), n (nanos).
     *
     * @param timestampDriver the timestamp driver
     * @param timestamp       the base timestamp
     * @param seq             the duration string
     * @param lo              start position in seq
     * @param lim             end position in seq
     * @param position        position for error reporting
     * @return the new timestamp after adding the duration
     */
    private static long addDuration(
            TimestampDriver timestampDriver,
            long timestamp,
            CharSequence seq,
            int lo,
            int lim,
            int position
    ) throws SqlException {
        int numStart = lo;
        for (int i = lo; i < lim; i++) {
            char c = seq.charAt(i);
            if ((c >= '0' && c <= '9') || c == '_') {
                continue;
            }
            // Found a unit character
            if (i == numStart) {
                throw SqlException.$(position, "Expected number before unit '").put(c).put('\'');
            }
            int period;
            try {
                period = Numbers.parseInt(seq, numStart, i);
            } catch (NumericException e) {
                throw SqlException.$(position, "Duration not a number: ").put(seq, numStart, i);
            }
            timestamp = timestampDriver.add(timestamp, c, period);
            if (timestamp == Numbers.LONG_NULL) {
                throw SqlException.$(position, "Invalid duration unit: ").put(c);
            }
            numStart = i + 1;
        }
        if (numStart < lim) {
            throw SqlException.$(position, "Missing unit at end of duration");
        }
        return timestamp;
    }

    private static void addLinearInterval(long period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);
        int n = count - 1;
        if (period < 0) {
            lo += period * n;
            hi += period * n;
            out.setQuick(k - 2, lo);
            out.setQuick(k - 1, hi);
            period = -period;
        }
        int writePoint = k / 2;

        for (int i = 0; i < n; i++) {
            lo += period;
            hi += period;
            writePoint = append(out, writePoint, lo, hi);
        }
    }

    private static void addMonthInterval(TimestampDriver timestampDriver, int period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);
        int writePoint = k / 2;
        int n = count - 1;
        if (period < 0) {
            lo = timestampDriver.addMonths(lo, period * n);
            hi = timestampDriver.addMonths(hi, period * n);
            out.setQuick(k - 2, lo);
            out.setQuick(k - 1, hi);
            period = -period;
        }

        for (int i = 0; i < n; i++) {
            lo = timestampDriver.addMonths(lo, period);
            hi = timestampDriver.addMonths(hi, period);
            writePoint = append(out, writePoint, lo, hi);
        }
    }

    private static void addYearIntervals(TimestampDriver timestampDriver, int period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);
        int writePoint = k / 2;
        int n = count - 1;
        if (period < 0) {
            lo = timestampDriver.addYears(lo, period * n);
            hi = timestampDriver.addYears(hi, period * n);
            out.setQuick(k - 2, lo);
            out.setQuick(k - 1, hi);
            period = -period;
        }

        for (int i = 0; i < n; i++) {
            lo = timestampDriver.addYears(lo, period);
            hi = timestampDriver.addYears(hi, period);
            writePoint = append(out, writePoint, lo, hi);
        }
    }

    private static void appendPaddedInt(StringSink sink, int value, int padWidth) {
        if (padWidth <= 0) {
            Numbers.append(sink, value);
            return;
        }
        // Calculate number of digits
        int digits = 1;
        int temp = value;
        while (temp >= 10) {
            temp /= 10;
            digits++;
        }
        // Add leading zeros
        for (int i = digits; i < padWidth; i++) {
            sink.put('0');
        }
        Numbers.append(sink, value);
    }

    private static void apply(TimestampDriver timestampDriver, LongList temp, long lo, long hi, int period, char periodType, int count) {
        temp.add(lo, hi);
        if (count > 1) {
            switch (periodType) {
                case PeriodType.YEAR:
                    addYearIntervals(timestampDriver, period, count, temp);
                    break;
                case PeriodType.MONTH:
                    addMonthInterval(timestampDriver, period, count, temp);
                    break;
                case PeriodType.HOUR:
                    addLinearInterval(timestampDriver.fromHours(period), count, temp);
                    break;
                case PeriodType.MINUTE:
                    addLinearInterval(timestampDriver.fromMinutes(period), count, temp);
                    break;
                case PeriodType.SECOND:
                    addLinearInterval(timestampDriver.fromSeconds(period), count, temp);
                    break;
                case PeriodType.DAY:
                    addLinearInterval(timestampDriver.fromDays(period), count, temp);
                    break;
            }
        }
    }

    /**
     * Applies a day-of-week filter to intervals in the output list.
     * For multi-day intervals, optionally expands them into individual days and filters each day.
     * For single-day intervals, checks if the day matches the filter.
     * Uses a variable stride algorithm to jump directly to matching days, avoiding
     * iteration over non-matching days.
     *
     * @param timestampDriver the timestamp driver for day-of-week calculation
     * @param out             the interval list to filter
     * @param startIndex      index to start filtering from
     * @param dayFilterMask   bitmask of allowed days (bit 0 = Monday, bit 6 = Sunday)
     * @param ignoreMultiDay  if true, expand multi-day intervals into individual matching days;
     *                        if false, just filter based on start day (for precise dates with duration)
     */
    private static void applyDayFilter(
            TimestampDriver timestampDriver,
            LongList out,
            int startIndex,
            int dayFilterMask,
            boolean ignoreMultiDay
    ) {
        assert dayFilterMask != 0; // Callers must check dayFilterMask != 0 before calling

        int originalSize = out.size();

        // Pass 1: Count total output intervals using strides (no day-by-day iteration)
        int totalIntervals = 0;
        for (int readIdx = startIndex; readIdx < originalSize; readIdx += 2) {
            long lo = out.getQuick(readIdx);
            long hi = out.getQuick(readIdx + 1);

            long loDay = timestampDriver.startOfDay(lo, 0);
            long hiDay = timestampDriver.startOfDay(hi, 0);

            if (loDay == hiDay || ignoreMultiDay) {
                // Single day OR precise date with duration - just check if start day matches
                int dayOfWeek = timestampDriver.getDayOfWeek(lo) - 1; // 0-6
                if ((dayFilterMask & (1 << dayOfWeek)) != 0) {
                    totalIntervals++;
                }
            } else {
                // Multi-day imprecise interval (year/month) - count matching days using strides
                totalIntervals += countMatchingDays(timestampDriver, loDay, hiDay, dayFilterMask);
            }
        }

        // Write expanded intervals at end of list, then copy back
        // This avoids overwriting data we haven't read yet
        out.setPos(originalSize + totalIntervals * 2);

        int writeIdx = originalSize;
        for (int readIdx = startIndex; readIdx < originalSize; readIdx += 2) {
            long lo = out.getQuick(readIdx);
            long hi = out.getQuick(readIdx + 1);

            long loDay = timestampDriver.startOfDay(lo, 0);
            long hiDay = timestampDriver.startOfDay(hi, 0);

            if (loDay == hiDay || ignoreMultiDay) {
                // Single day OR precise date with duration - keep entire interval if start day matches
                int dayOfWeek = timestampDriver.getDayOfWeek(lo) - 1; // 0-6
                if ((dayFilterMask & (1 << dayOfWeek)) != 0) {
                    out.setQuick(writeIdx++, lo);
                    out.setQuick(writeIdx++, hi);
                }
            } else {
                // Multi-day imprecise interval (year/month) - expand using strides
                // For imprecise dates: lo is at midnight, hi is at end of last day
                // No clamping needed since currentDay is always within [loDay, hiDay]
                int startDow = timestampDriver.getDayOfWeek(loDay) - 1; // 0-6
                long currentDay = timestampDriver.addDays(loDay, daysToFirstMatch(startDow, dayFilterMask));

                while (currentDay <= hiDay) {
                    out.setQuick(writeIdx++, currentDay);
                    out.setQuick(writeIdx++, timestampDriver.endOfDay(currentDay));

                    int currentDow = timestampDriver.getDayOfWeek(currentDay) - 1;
                    currentDay = timestampDriver.addDays(currentDay, nextMatchingDayStride(currentDow, dayFilterMask));
                }
            }
        }

        // Copy from temp area back to start
        for (int i = 0; i < totalIntervals * 2; i++) {
            out.setQuick(startIndex + i, out.getQuick(originalSize + i));
        }
        out.setPos(startIndex + totalIntervals * 2);
    }

    /**
     * Applies duration suffix to intervals by extending the hi bound of each interval.
     * This is used when both an tick calendar filter and duration are present.
     * The duration is applied after the tick calendar filter to extend trading hours.
     *
     * @param timestampDriver the timestamp driver
     * @param out             the interval list
     * @param startIndex      index to start from
     * @param seq             the duration string (e.g., "1h", "30m")
     * @param lo              start position in seq
     * @param lim             end position in seq
     * @param position        position for error reporting
     */
    private static void applyDurationToIntervals(
            TimestampDriver timestampDriver,
            LongList out,
            int startIndex,
            CharSequence seq,
            int lo,
            int lim,
            int position
    ) throws SqlException {
        for (int i = startIndex + 1; i < out.size(); i += 2) {
            long hi = out.getQuick(i);
            long extendedHi = addDuration(timestampDriver, hi, seq, lo, lim, position);
            out.setQuick(i, extendedHi);
        }
    }

    /**
     * Applies tick calendar filter and optional duration extension.
     * The filter intersects query intervals with the exchange's trading schedule,
     * then extends each interval's hi bound by the duration if present.
     */
    private static void applyExchangeFilterAndDuration(
            TimestampDriver timestampDriver,
            LongList exchangeSchedule,
            LongList out,
            int startIndex,
            CharSequence durationSeq,
            int durationLo,
            int durationLim,
            int position
    ) throws SqlException {
        applyTickCalendarFilter(timestampDriver, exchangeSchedule, out, startIndex);
        if (durationLo >= 0) {
            applyDurationToIntervals(timestampDriver, out, startIndex, durationSeq, durationLo, durationLim, position);
        }
    }

    /**
     * Applies timezone conversion to all intervals in the output list from outSizeBeforeConversion to end.
     * This converts local timestamps to UTC. Handles both numeric offsets (+03:00) and named
     * timezones (Europe/London).
     * <p>
     * <b>DST gap handling:</b> When the lo timestamp falls within a DST gap (a period that
     * doesn't exist in local time, e.g., 2:30 AM on spring-forward day), both lo and hi are
     * adjusted forward by the same offset to preserve the interval width. For example, if
     * clocks jump from 2:00 AM to 3:00 AM, a query for the minute 2:30 AM will be adjusted
     * to the minute 3:00 AM (both lo and hi shift by 30 minutes).
     * This behavior is consistent with {@code TimestampFloorFromOffsetFunctionFactory}.
     * <p>
     * <b>DST overlap handling:</b> When a local timestamp falls within a DST overlap (a period
     * that occurs twice, e.g., during fall-back), the daylight saving timezone offset is used.
     *
     * @param timestampDriver         the timestamp driver
     * @param out                     the interval list
     * @param outSizeBeforeConversion size of output before intervals that need conversion
     * @param tz                      timezone string
     * @param tzLo                    start index in tz
     * @param tzHi                    end index in tz
     * @param position                position for error reporting
     * @param isStaticMode            true if intervals are in 2-long format, false if 4-long format
     */
    private static void applyTimezoneToIntervals(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            LongList out,
            int outSizeBeforeConversion,
            CharSequence tz,
            int tzLo,
            int tzHi,
            int position,
            boolean isStaticMode
    ) throws SqlException {
        int stride = isStaticMode ? 2 : 4;
        int currentSize = out.size();

        // Pre-parse timezone once outside the loop to avoid repeated parsing
        long numericOffset;
        TimeZoneRules tzRules;
        DateLocale dateLocale = configuration.getDefaultDateLocale();

        try {
            long l = Dates.parseOffset(tz, tzLo, tzHi);
            if (l != Long.MIN_VALUE) {
                // Numeric offset - convert minutes to timestamp units
                numericOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                // Numeric offset - simple subtraction loop
                for (int i = outSizeBeforeConversion; i < currentSize; i += stride) {
                    out.setQuick(i, out.getQuick(i) - numericOffset);
                    out.setQuick(i + 1, out.getQuick(i + 1) - numericOffset);
                }
            } else {
                // Named timezone - get timezone rules
                tzRules = dateLocale.getZoneRules(
                        Numbers.decodeLowInt(dateLocale.matchZone(tz, tzLo, tzHi)),
                        timestampDriver.getTZRuleResolution()
                );
                // Named timezone - handle DST gaps
                for (int i = outSizeBeforeConversion; i < currentSize; i += stride) {
                    long lo = out.getQuick(i);
                    long hi = out.getQuick(i + 1);

                    long adjustedLo = lo;
                    long adjustedHi = hi;
                    long gapDuration = tzRules.getDstGapOffset(lo);
                    if (gapDuration != 0) {
                        // lo is in a DST gap - adjust both lo and hi forward by the same amount
                        adjustedLo = lo + gapDuration;
                        adjustedHi = hi + gapDuration;
                    } else {
                        // lo is not in a gap, but hi might be (edge case: interval spans gap boundary)
                        gapDuration = tzRules.getDstGapOffset(hi);
                        if (gapDuration != 0) {
                            adjustedHi = hi + gapDuration;
                        }
                    }

                    out.setQuick(i, timestampDriver.toUTC(adjustedLo, tzRules));
                    out.setQuick(i + 1, timestampDriver.toUTC(adjustedHi, tzRules));
                }
            }
        } catch (NumericException e) {
            throw SqlException.$(position, "invalid timezone: ").put(tz, tzLo, tzHi);
        }
    }

    /**
     * Compiles a bare variable (possibly a range like "$today..$today+5bd") into irList.
     */
    private static void compileBareVarElement(
            CharSequence effectiveSeq,
            int elemListLo,
            int elemListHi,
            int position,
            LongList irList
    ) throws SqlException {
        int rangeOpPos = findRangeOperator(effectiveSeq, elemListLo, elemListHi);
        if (rangeOpPos >= 0) {
            int startExprHi = rangeOpPos;
            int endExprLo = rangeOpPos + 2;
            int endExprHi = elemListHi;
            while (startExprHi > elemListLo && Chars.isAsciiWhitespace(effectiveSeq.charAt(startExprHi - 1))) {
                startExprHi--;
            }
            while (endExprLo < endExprHi && Chars.isAsciiWhitespace(effectiveSeq.charAt(endExprLo))) {
                endExprLo++;
            }
            while (endExprHi > endExprLo && Chars.isAsciiWhitespace(effectiveSeq.charAt(endExprHi - 1))) {
                endExprHi--;
            }
            boolean isBusinessDay = isBusinessDayExpression(effectiveSeq, endExprHi);
            irList.add(CompiledTickExpression.TAG_RANGE
                    | (isBusinessDay ? CompiledTickExpression.RANGE_BD_BIT : 0L)
                    | DateVariableExpr.parseEncoded(effectiveSeq, elemListLo, startExprHi, position));
            irList.add(DateVariableExpr.parseEncoded(effectiveSeq, endExprLo, endExprHi, position));
        } else {
            irList.add(CompiledTickExpression.TAG_SINGLE_VAR
                    | DateVariableExpr.parseEncoded(effectiveSeq, elemListLo, elemListHi, position));
        }
    }

    /**
     * Iterates comma-separated elements within a bracket list and appends each to irList.
     *
     * @return number of elements appended
     */
    private static int compileBracketListElements(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence effectiveSeq,
            int elemListLo,
            int elemListHi,
            int timeLo,
            int timeHi,
            int tzMarkerPos,
            int durationSemicolon,
            int durationHi,
            int tzContentHi,
            int dayFilterMask,
            LongList exchangeSchedule,
            int position,
            LongList irList,
            StringSink parseSink,
            LongList tmp
    ) throws SqlException {
        int elemCount = 0;
        int depth = 0;
        int elementStart = elemListLo;
        while (elementStart < elemListHi && Chars.isAsciiWhitespace(effectiveSeq.charAt(elementStart))) {
            elementStart++;
        }

        for (int elementEnd = elementStart; elementEnd <= elemListHi; elementEnd++) {
            char c = elementEnd < elemListHi ? effectiveSeq.charAt(elementEnd) : ',';

            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                int es = elementStart;
                int ee = elementEnd;
                while (es < ee && Chars.isAsciiWhitespace(effectiveSeq.charAt(es))) {
                    es++;
                }
                while (ee > es && Chars.isAsciiWhitespace(effectiveSeq.charAt(ee - 1))) {
                    ee--;
                }
                if (es >= ee) {
                    throw SqlException.$(position, "Empty element in date list");
                }

                if (effectiveSeq.charAt(es) == '$') {
                    compileVarElement(effectiveSeq, es, ee, position, irList);
                    elemCount++;
                } else {
                    elemCount += compileStaticElement(
                            timestampDriver, configuration, effectiveSeq,
                            es, ee, timeLo, timeHi, tzMarkerPos,
                            durationSemicolon, durationHi, tzContentHi,
                            dayFilterMask, exchangeSchedule,
                            position, irList, parseSink, tmp
                    );
                }
                elementStart = elementEnd + 1;
            }
        }
        return elemCount;
    }

    /**
     * Parses duration parts (e.g. "6h30m") and appends each (unit, value) encoded
     * long to irList.
     *
     * @return number of duration parts parsed
     */
    private static int compileDurationParts(
            CharSequence seq,
            int durationLo,
            int durationHi,
            int position,
            LongList irList
    ) throws SqlException {
        int durationPartCount = 0;
        int numStart = durationLo;
        for (int i = durationLo; i < durationHi; i++) {
            char c = seq.charAt(i);
            if (Chars.isAsciiDigit(c) || c == '_') {
                continue;
            }
            if (i == numStart) {
                throw SqlException.$(position, "Expected number before unit '").put(c).put('\'');
            }
            int value;
            try {
                value = Numbers.parseInt(seq, numStart, i);
            } catch (NumericException e) {
                throw SqlException.$(position, "Duration not a number: ").put(seq, numStart, i);
            }
            irList.add(CompiledTickExpression.encodeDuration(c, value));
            durationPartCount++;
            numStart = i + 1;
        }
        if (numStart < durationHi) {
            throw SqlException.$(position, "Missing unit at end of duration");
        }
        return durationPartCount;
    }

    /**
     * Appends element entries (SINGLE_VAR, STATIC, RANGE) to irList.
     *
     * @return number of elements appended
     */
    private static int compileElements(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence effectiveSeq,
            int elemListLo,
            int elemListHi,
            boolean isBareVar,
            int timeLo,
            int timeHi,
            int tzMarkerPos,
            int durationSemicolon,
            int durationHi,
            int tzContentHi,
            int dayFilterMask,
            LongList exchangeSchedule,
            int position,
            LongList irList,
            StringSink parseSink,
            LongList tmp
    ) throws SqlException {
        if (isBareVar) {
            compileBareVarElement(effectiveSeq, elemListLo, elemListHi, position, irList);
            return 1;
        }
        return compileBracketListElements(
                timestampDriver, configuration, effectiveSeq,
                elemListLo, elemListHi,
                timeLo, timeHi, tzMarkerPos, durationSemicolon, durationHi, tzContentHi,
                dayFilterMask, exchangeSchedule,
                position, irList, parseSink, tmp
        );
    }

    /**
     * Finds the end of a bare variable expression (stops at '@', ';', or 'T' followed by digit).
     */
    private static int compileFindBareVarEnd(CharSequence seq, int from, int lim) {
        int pos = from;
        while (pos < lim) {
            char c = seq.charAt(pos);
            if (c == '@' || c == ';') {
                break;
            }
            if (c == 'T' && pos + 1 < lim && Chars.isAsciiDigit(seq.charAt(pos + 1))) {
                break;
            }
            pos++;
        }
        return pos;
    }

    /**
     * Finds the first occurrence of {@code target} in [from, lim), or returns -1 if not found.
     */
    private static int compileFindCharOrNeg1(CharSequence seq, int from, int lim, char target) {
        for (int i = from; i < lim; i++) {
            if (seq.charAt(i) == target) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Finds the matching ']' for a '[' that was already consumed.
     */
    private static int compileFindClosingBracket(CharSequence seq, int from, int lim, int position) throws SqlException {
        int bracketDepth = 1;
        for (int i = from; i < lim; i++) {
            char c = seq.charAt(i);
            if (c == '[') {
                bracketDepth++;
            } else if (c == ']' && --bracketDepth == 0) {
                return i;
            }
        }
        throw SqlException.$(position, "Unclosed '[' in date list");
    }

    /**
     * Finds the first occurrence of {@code target} in [from, lim), or returns lim if not found.
     */
    private static int compileFindSemicolonOrEnd(CharSequence seq, int from, int lim) {
        for (int i = from; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                return i;
            }
        }
        return lim;
    }

    /**
     * Parses a single time element (e.g. "09:00" or "09:00@+05:00") and appends
     * one (offset, width, zoneMatch) triple to irList.
     */
    private static void compileOneTimeOverride(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence effectiveSeq,
            int elemStart,
            int elemEnd,
            int position,
            LongList irList,
            StringSink parseSink,
            LongList tmp
    ) throws SqlException {
        int es = elemStart;
        int etzHi = elemEnd;
        while (es < etzHi && Chars.isAsciiWhitespace(effectiveSeq.charAt(es))) {
            es++;
        }
        while (etzHi > es && Chars.isAsciiWhitespace(effectiveSeq.charAt(etzHi - 1))) {
            etzHi--;
        }
        if (es >= etzHi) {
            throw SqlException.$(position, "Empty element in time list");
        }

        // Find per-element timezone marker
        int elemTzMarker = compileFindCharOrNeg1(effectiveSeq, es, etzHi, '@');
        int timeEnd = elemTzMarker >= 0 ? elemTzMarker : etzHi;

        parseSink.clear();
        parseSink.put("1970-01-01T");
        parseSink.put(effectiveSeq, es, timeEnd);
        tmp.clear();
        try {
            timestampDriver.parseInterval(parseSink, 0, parseSink.length(), IntervalOperation.INTERSECT, tmp);
        } catch (NumericException e) {
            throw SqlException.$(position, "Invalid time in time list: ").put(effectiveSeq, es, timeEnd);
        }
        long tLo = decodeIntervalLo(tmp, 0);
        long tHi = decodeIntervalHi(tmp, 0);

        // Per-element timezone
        long zoneMatch = Long.MIN_VALUE;
        if (elemTzMarker >= 0) {
            int etzLo = elemTzMarker + 1;
            try {
                long l = Dates.parseOffset(effectiveSeq, etzLo, etzHi);
                if (l != Long.MIN_VALUE) {
                    long elemTzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                    tLo -= elemTzOffset;
                    tHi -= elemTzOffset;
                    zoneMatch = Long.MAX_VALUE;
                } else {
                    DateLocale dateLocale = configuration.getDefaultDateLocale();
                    zoneMatch = dateLocale.matchZone(effectiveSeq, etzLo, etzHi);
                }
            } catch (NumericException e) {
                throw SqlException.$(position, "invalid timezone in time list: ").put(effectiveSeq, etzLo, etzHi);
            }
        }

        irList.add(tLo);
        irList.add(tHi - tLo);
        irList.add(zoneMatch);
    }

    /**
     * Compiles a static (non-variable) element by pre-computing its [lo, hi] intervals
     * and appending TAG_STATIC entries to irList.
     *
     * @return number of static elements appended (may be &gt;1 for bracket expansion)
     */
    private static int compileStaticElement(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence effectiveSeq,
            int es,
            int ee,
            int timeLo,
            int timeHi,
            int tzMarkerPos,
            int durationSemicolon,
            int durationHi,
            int tzContentHi,
            int dayFilterMask,
            LongList exchangeSchedule,
            int position,
            LongList irList,
            StringSink parseSink,
            LongList tmp
    ) throws SqlException {
        // Build the full element string: date + time override + duration suffix
        parseSink.clear();
        parseSink.put(effectiveSeq, es, ee);
        int dateLimInSink = parseSink.length();
        if (timeLo < timeHi) {
            if (tzMarkerPos >= 0) {
                parseSink.put(effectiveSeq, timeLo, tzMarkerPos);
            } else if (durationSemicolon >= 0) {
                parseSink.put(effectiveSeq, timeLo, durationHi);
            } else {
                parseSink.put(effectiveSeq, timeLo, timeHi);
            }
        }
        if (durationSemicolon >= 0 && tzMarkerPos >= 0) {
            parseSink.put(effectiveSeq, tzContentHi, durationHi);
        } else if (durationSemicolon >= 0 && timeLo >= timeHi) {
            parseSink.put(effectiveSeq, durationSemicolon, durationHi);
        }

        tmp.clear();

        // Check if element contains brackets (e.g. 2025-01-[13..15])
        boolean elemHasBrackets = false;
        for (int j = es; j < ee; j++) {
            if (effectiveSeq.charAt(j) == '[') {
                elemHasBrackets = true;
                break;
            }
        }

        if (elemHasBrackets) {
            StringSink expansionSink = tlCompileSink3.get();
            expansionSink.clear();
            expandBracketsRecursive(
                    timestampDriver, configuration, parseSink,
                    0, dateLimInSink, parseSink.length(),
                    position, tmp, IntervalOperation.INTERSECT,
                    expansionSink, 0, true, 0,
                    null, -1, -1
            );
        } else {
            parseIntervalSuffix(timestampDriver, parseSink, 0, parseSink.length(), position, tmp, IntervalOperation.INTERSECT);
            applyLastEncodedInterval(timestampDriver, tmp);
        }

        // Day filter in local time (before tz conversion)
        if (dayFilterMask != 0 && exchangeSchedule == null && tmp.size() >= 2) {
            applyDayFilter(timestampDriver, tmp, 0, dayFilterMask, hasDatePrecision(effectiveSeq, es, ee));
        }

        // Append TAG_STATIC entries to IR
        int addedElems = 0;
        for (int k = 0; k < tmp.size(); k += 2) {
            irList.add(CompiledTickExpression.TAG_STATIC);
            irList.add(tmp.getQuick(k));
            irList.add(tmp.getQuick(k + 1));
            addedElems++;
        }
        return addedElems;
    }

    /**
     * Determines the end of the time override region within the suffix.
     */
    private static int compileSuffixTimeHi(int tzMarkerPos, int durationSemicolon, int durationHi) {
        if (tzMarkerPos >= 0) {
            return tzMarkerPos;
        }
        if (durationSemicolon >= 0) {
            return durationSemicolon;
        }
        return durationHi;
    }

    /**
     * Parses a bracket time list like T[09:00,14:00,16:30@+05:00] and appends
     * (offset, width, zoneMatch) triples to irList.
     *
     * @return number of time overrides parsed
     */
    private static int compileTimeOverrideBracketList(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence effectiveSeq,
            int tContentLo,
            int timeHi,
            int position,
            LongList irList,
            StringSink parseSink,
            LongList tmp
    ) throws SqlException {
        int bracketEnd = -1;
        for (int i = tContentLo + 1; i < timeHi; i++) {
            if (effectiveSeq.charAt(i) == ']') {
                bracketEnd = i;
                break;
            }
        }
        if (bracketEnd < 0) {
            throw SqlException.$(position, "Unclosed '[' in time list");
        }

        int timeOverrideCount = 0;
        int elemStart = tContentLo + 1;
        for (int i = tContentLo + 1; i <= bracketEnd; i++) {
            char c = i < bracketEnd ? effectiveSeq.charAt(i) : ',';
            if (c == ',') {
                compileOneTimeOverride(
                        timestampDriver, configuration, effectiveSeq,
                        elemStart, i, position, irList, parseSink, tmp
                );
                timeOverrideCount++;
                elemStart = i + 1;
            }
        }
        return timeOverrideCount;
    }

    /**
     * Parses time override(s) from the suffix (e.g. "T09:30" or "T[09:00,14:00]")
     * and appends (offset, width, zoneMatch) triples to irList.
     *
     * @return number of time overrides parsed
     */
    private static int compileTimeOverrides(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence effectiveSeq,
            int timeLo,
            int timeHi,
            int position,
            LongList irList,
            StringSink parseSink,
            LongList tmp
    ) throws SqlException {
        if (effectiveSeq.charAt(timeLo) != 'T') {
            throw SqlException.$(position, "Expected 'T' time override, got: ").put(effectiveSeq.charAt(timeLo));
        }
        int tContentLo = timeLo + 1;
        if (tContentLo >= timeHi) {
            throw SqlException.$(position, "Invalid time override: T with no value");
        }

        if (effectiveSeq.charAt(tContentLo) == '[') {
            return compileTimeOverrideBracketList(
                    timestampDriver, configuration, effectiveSeq,
                    tContentLo, timeHi, position, irList, parseSink, tmp
            );
        }

        // Single time value: T09:30
        parseSink.clear();
        parseSink.put("1970-01-01T");
        parseSink.put(effectiveSeq, tContentLo, timeHi);
        tmp.clear();
        try {
            timestampDriver.parseInterval(parseSink, 0, parseSink.length(), IntervalOperation.INTERSECT, tmp);
        } catch (NumericException e) {
            throw SqlException.$(position, "Invalid time override: ").put(effectiveSeq, tContentLo, timeHi);
        }
        long tLo = decodeIntervalLo(tmp, 0);
        long tHi = decodeIntervalHi(tmp, 0);
        irList.add(tLo);
        irList.add(tHi - tLo);
        irList.add(Long.MIN_VALUE);
        return 1;
    }

    /**
     * Compiles a single variable element (possibly a range) and appends to irList.
     */
    private static void compileVarElement(
            CharSequence effectiveSeq,
            int es,
            int ee,
            int position,
            LongList irList
    ) throws SqlException {
        int rangeOpPos = findRangeOperator(effectiveSeq, es, ee);
        if (rangeOpPos >= 0) {
            int startHi = rangeOpPos;
            int endLo = rangeOpPos + 2;
            while (startHi > es && Chars.isAsciiWhitespace(effectiveSeq.charAt(startHi - 1))) {
                startHi--;
            }
            while (endLo < ee && Chars.isAsciiWhitespace(effectiveSeq.charAt(endLo))) {
                endLo++;
            }
            boolean isBd = isBusinessDayExpression(effectiveSeq, ee);
            irList.add(CompiledTickExpression.TAG_RANGE
                    | (isBd ? CompiledTickExpression.RANGE_BD_BIT : 0L)
                    | DateVariableExpr.parseEncoded(effectiveSeq, es, startHi, position));
            irList.add(DateVariableExpr.parseEncoded(effectiveSeq, endLo, ee, position));
        } else {
            irList.add(CompiledTickExpression.TAG_SINGLE_VAR
                    | DateVariableExpr.parseEncoded(effectiveSeq, es, ee, position));
        }
    }

    /**
     * Counts matching days in a date range using O(1) mathematical computation.
     * Both loDay and hiDay must be at start of day (midnight).
     */
    private static int countMatchingDays(
            TimestampDriver timestampDriver,
            long loDay,
            long hiDay,
            int dayFilterMask
    ) {
        // Total days in range (inclusive)
        int totalDays = (int) ((hiDay - loDay) / timestampDriver.fromDays(1)) + 1;

        // Full weeks contribute Integer.bitCount(mask) matching days each
        int fullWeeks = totalDays / 7;
        int matchingDaysPerWeek = Integer.bitCount(dayFilterMask);
        int count = fullWeeks * matchingDaysPerWeek;

        // Handle remainder days (partial week starting from same dow as loDay)
        int remainderDays = totalDays % 7;
        if (remainderDays > 0) {
            int startDow = timestampDriver.getDayOfWeek(loDay) - 1; // 0-6
            int remainderMask = createDayRangeMask(startDow, remainderDays);
            count += Integer.bitCount(dayFilterMask & remainderMask);
        }

        return count;
    }

    /**
     * Creates a bitmask for a contiguous range of days-of-week.
     * Handles wrap-around from Sunday (6) back to Monday (0).
     *
     * @param startDow starting day of week (0=Monday, 6=Sunday)
     * @param length   number of consecutive days
     * @return bitmask with bits set for each day in the range
     */
    private static int createDayRangeMask(int startDow, int length) {
        if (startDow + length <= 7) {
            // No wrap: bits from startDow to startDow + length - 1
            return ((1 << length) - 1) << startDow;
        } else {
            // Wrap around: bits from startDow to 6, plus bits from 0 to wrap point
            int highBits = ((1 << (7 - startDow)) - 1) << startDow;
            int lowBits = (1 << (startDow + length - 7)) - 1;
            return highBits | lowBits;
        }
    }

    /**
     * Computes days from startDow to the first matching day in the mask.
     * Returns 0 if startDow itself matches.
     * Uses bit manipulation to avoid array allocation.
     */
    private static int daysToFirstMatch(int startDow, int dayFilterMask) {
        // If current day matches, return 0
        if ((dayFilterMask & (1 << startDow)) != 0) {
            return 0;
        }
        // Otherwise find next matching day
        return nextMatchingDayStride(startDow, dayFilterMask);
    }

    private static int determinePadWidth(CharSequence sink) {
        // Determine field width based on position in timestamp
        // For accurate analysis after prior bracket expansions, check the sink content
        // which contains the expanded prefix up to this bracket
        //
        // Format: YYYY-MM-DDTHH:MM:SS.ffffff or YYYY-Www-DTHH:MM:SS (ISO week)

        // First, analyze the sink (expanded content so far)
        int sinkLen = sink.length();
        if (sinkLen > 0) {
            boolean afterDot = false;
            boolean afterT = false;
            int wPos = -1;

            for (int i = 0; i < sinkLen; i++) {
                char c = sink.charAt(i);
                if (c == '.') {
                    afterDot = true;
                } else if (c == 'T' || c == ' ') {
                    afterT = true;
                } else if (c == 'W') {
                    wPos = i;
                }
            }

            if (afterDot) {
                // Microseconds/nanoseconds - no padding (variable width)
                return 0;
            }
            if (afterT) {
                // Time field: hour, minute, or second - 2 digits
                return 2;
            }

            // Check for ISO week format in sink: YYYY-Www-D
            if (wPos >= 0) {
                // Count chars after W to determine which field
                int charsAfterW = sinkLen - wPos - 1;
                if (charsAfterW == 0) {
                    // Sink ends with "W" - week number position (2 digits)
                    return 2;
                }
                // Sink ends with "Wxx-" - day of week position (1 digit)
                return 1;
            }

            // Standard date format - month or day (2 digits)
            return 2;
        }

        // First bracket with empty sink - no padding needed
        return 0;
    }

    /**
     * Recursive bracket expansion. Finds the first bracket, iterates through its values,
     * and recurses to handle remaining brackets. When no brackets remain, parses the
     * fully-expanded string.
     * <p>
     * The sink accumulates the expanded string as recursion proceeds. Each call restores
     * the sink to its entry state before returning, allowing the parent to continue
     * iterating through other values.
     *
     * @return true if any time list brackets were processed, false otherwise
     */
    private static boolean expandBracketsRecursive(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence seq,
            // start of interval in seq (for pad width calculation)
            int pos,        // current position in seq (within date part)
            int dateLim,    // end of date part (before semicolon)
            int fullLim,    // end of entire string (including duration suffix)
            int errorPos,
            LongList out,
            short operation,
            StringSink sink,
            int depth,
            boolean applyEncoded,
            int outSizeBeforeExpansion,
            CharSequence globalTzSeq, // original sequence containing timezone (for applying global TZ)
            int globalTzLo,           // start of global timezone in globalTzSeq (-1 if none)
            int globalTzHi            // end of global timezone in globalTzSeq (-1 if none)
    ) throws SqlException {
        int maxBracketDepth = configuration.getSqlIntervalMaxBracketDepth();
        if (depth > maxBracketDepth) {
            throw SqlException.$(errorPos, "Too many bracket groups (max ").put(maxBracketDepth).put(')');
        }

        // Find first bracket starting from pos
        int bracketStart = -1;
        for (int i = pos; i < dateLim; i++) {
            if (seq.charAt(i) == '[') {
                bracketStart = i;
                break;
            }
        }

        // Save sink state before modifications
        int startLen = sink.length();

        if (bracketStart < 0) {
            // No more brackets - parse the accumulated expansion
            // Note: sink always has content here (at minimum the expanded bracket value)
            sink.put(seq, pos, fullLim);
            parseExpandedInterval(timestampDriver, sink, errorPos, out, operation, applyEncoded, outSizeBeforeExpansion);
            sink.clear(startLen);
            return false; // No time list bracket processed
        }

        // Copy text before bracket
        sink.put(seq, pos, bracketStart);
        int afterPrefixLen = sink.length();

        // Find closing bracket
        int bracketEnd = findMatchingBracket(seq, bracketStart, dateLim, errorPos);

        // Check if bracket contains time list (has ':' inside, e.g., [09:00,14:30])
        // vs numeric expansion (no ':', e.g., [09,14])
        if (isTimeListBracket(seq, bracketStart, bracketEnd)) {
            expandTimeListBracket(
                    timestampDriver,
                    configuration,
                    seq,
                    bracketStart,
                    bracketEnd,
                    fullLim,
                    errorPos,
                    out,
                    operation,
                    sink,
                    afterPrefixLen,
                    applyEncoded,
                    outSizeBeforeExpansion,
                    globalTzSeq,
                    globalTzLo,
                    globalTzHi
            );
            sink.clear(startLen);
            return true; // Time list bracket was processed
        }

        // Determine zero-padding width based on position in timestamp
        // Use sink content for accurate analysis after prior bracket expansions
        int padWidth = determinePadWidth(sink);

        // Track if any time list bracket was found in recursive calls
        boolean foundTimeListBracket = false;

        // Iterate through values in bracket without allocating collections
        int i = bracketStart + 1;
        int valueCount = 0;

        while (i < bracketEnd) {
            // Skip whitespace
            while (i < bracketEnd && Chars.isAsciiWhitespace(seq.charAt(i))) {
                i++;
            }
            if (i >= bracketEnd) {
                break;
            }

            // Parse number
            int numStart = i;
            while (i < bracketEnd && Chars.isAsciiDigit(seq.charAt(i))) {
                i++;
            }
            if (numStart == i) {
                throw SqlException.$(errorPos, "Expected number in bracket expansion");
            }

            int value;
            try {
                value = Numbers.parseInt(seq, numStart, i);
            } catch (NumericException e) {
                throw SqlException.$(errorPos, "Expected number in bracket expansion");
            }

            // Check for military time format (e.g., [0900,1430] instead of [09:00,14:30])
            // If prefix ends with 'T' and value >= 100, user likely meant military time
            if (afterPrefixLen > 0 && sink.charAt(afterPrefixLen - 1) == 'T' && value >= 100) {
                throw SqlException.$(errorPos, "Military time format not supported in bracket expansion. Use colons: [09:00,14:30] instead of [0900,1430]");
            }

            // Skip whitespace
            while (i < bracketEnd && Chars.isAsciiWhitespace(seq.charAt(i))) {
                i++;
            }

            // Check for range (..)
            int rangeEnd = value;
            if (i + 1 < bracketEnd && seq.charAt(i) == '.' && seq.charAt(i + 1) == '.') {
                i += 2;
                // Skip whitespace
                while (i < bracketEnd && Chars.isAsciiWhitespace(seq.charAt(i))) {
                    i++;
                }
                // Parse range end
                int endStart = i;
                while (i < bracketEnd && Chars.isAsciiDigit(seq.charAt(i))) {
                    i++;
                }
                if (endStart == i) {
                    throw SqlException.$(errorPos, "Expected number after '..'");
                }
                try {
                    rangeEnd = Numbers.parseInt(seq, endStart, i);
                } catch (NumericException e) {
                    throw SqlException.$(errorPos, "Expected number after '..'");
                }
                if (rangeEnd < value) {
                    throw SqlException.$(errorPos, "Range must be ascending: ").put(value).put("..").put(rangeEnd);
                }
            }

            // Expand range (or single value if value == rangeEnd)
            for (int v = value; v <= rangeEnd; v++) {
                appendPaddedInt(sink, v, padWidth);
                if (expandBracketsRecursive(
                        timestampDriver, configuration, seq, bracketEnd + 1, dateLim, fullLim,
                        errorPos, out, operation, sink, depth + 1, applyEncoded, outSizeBeforeExpansion,
                        globalTzSeq, globalTzLo, globalTzHi
                )) {
                    foundTimeListBracket = true;
                }
                sink.clear(afterPrefixLen);

                // Incremental merge: when interval count exceeds threshold, merge to bound memory
                // This prevents unbounded growth from large ranges like [1..1000000]
                if (applyEncoded) {
                    int intervalCount = (out.size() - outSizeBeforeExpansion) / 2;
                    if (intervalCount >= configuration.getSqlIntervalIncrementalMergeThreshold()) {
                        unionBracketExpandedIntervals(out, outSizeBeforeExpansion);
                        // Check if we still exceed max limit after merging
                        intervalCount = (out.size() - outSizeBeforeExpansion) / 2;
                        int maxIntervalsAfterMerge = configuration.getSqlIntervalMaxIntervalsAfterMerge();
                        if (intervalCount > maxIntervalsAfterMerge) {
                            throw SqlException.$(errorPos, "Bracket expansion produces too many intervals (max ")
                                    .put(maxIntervalsAfterMerge).put(')');
                        }
                    }
                }
            }

            valueCount++;

            // Skip whitespace
            while (i < bracketEnd && Chars.isAsciiWhitespace(seq.charAt(i))) {
                i++;
            }

            // Expect comma or end
            if (i < bracketEnd) {
                if (seq.charAt(i) == ',') {
                    i++;
                } else {
                    throw SqlException.$(errorPos, "Expected ',' or end of bracket");
                }
            }
        }

        if (valueCount == 0) {
            throw SqlException.$(errorPos, "Empty bracket expansion");
        }

        // Restore sink to entry state
        sink.clear(startLen);
        return foundTimeListBracket;
    }

    /**
     * Expands a date list in the format: [date1,date2,...]suffix
     * Each date element can contain field expansion brackets.
     * The suffix (like T09:30;1h) is appended to each date.
     *
     * @param timestampDriver        timestamp driver for parsing
     * @param seq                    input string starting with '['
     * @param lo                     position of the opening '['
     * @param lim                    end of string
     * @param errorPos               position for error reporting
     * @param out                    output list for parsed intervals
     * @param operation              interval operation
     * @param sink                   reusable StringSink for building expanded strings.
     * @param applyEncoded           if true, converts to simple format
     * @param outSizeBeforeExpansion size of out before expansion started
     * @param dayFilterMask          bitmask for day-of-week filter (0 if not specified)
     * @param nowTimestamp           timestamp to use for resolving date variables ($today, $now, etc.)
     */
    private static void expandDateList(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence seq,
            int lo,
            int lim,
            int errorPos,
            LongList out,
            short operation,
            StringSink sink,
            boolean applyEncoded,
            int outSizeBeforeExpansion,
            int dayFilterMask,
            long nowTimestamp
    ) throws SqlException {
        // lo points to '[' - find the matching ']' for the date list
        int depth = 1;
        int listEnd = -1;
        for (int i = lo + 1; i < lim; i++) {
            char c = seq.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    listEnd = i;
                    break;
                }
            }
        }

        if (listEnd < 0) {
            throw SqlException.$(errorPos, "Unclosed '[' in date list");
        }

        // Check for empty brackets
        int contentStart = lo + 1;
        int contentEnd = listEnd;

        // Skip leading whitespace in content
        while (contentStart < contentEnd && Chars.isAsciiWhitespace(seq.charAt(contentStart))) {
            contentStart++;
        }

        if (contentStart >= contentEnd) {
            throw SqlException.$(errorPos, "Empty date list");
        }

        // The suffix is everything after the closing bracket (like T09:30;1h or T09:30@Europe/London;1h)
        int suffixStart = listEnd + 1;

        // Check for global timezone in suffix (applies to elements without per-element timezone)
        int globalTzMarker = findTimezoneMarker(seq, suffixStart, lim);
        int globalTzLo = -1;
        int globalTzHi = -1;

        // Find duration semicolon in suffix (for applying duration after tick calendar filter)
        int globalDurationSemicolon = findDurationSemicolon(seq, suffixStart, lim);

        if (globalTzMarker >= 0) {
            globalTzLo = globalTzMarker + 1;
            // Find where timezone ends (at ';' or end of string)
            int semicolonPos = -1;
            for (int j = globalTzMarker + 1; j < lim; j++) {
                if (seq.charAt(j) == ';') {
                    semicolonPos = j;
                    break;
                }
            }
            globalTzHi = semicolonPos >= 0 ? semicolonPos : lim;
        }

        // Parse comma-separated date elements, respecting nested brackets
        int elementStart = contentStart;

        for (int i = contentStart; i <= contentEnd; i++) {
            char c = i < contentEnd ? seq.charAt(i) : ','; // Treat end as virtual comma

            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                // Found element boundary
                int elementEnd = i;

                // Trim whitespace from element
                while (elementStart < elementEnd && Chars.isAsciiWhitespace(seq.charAt(elementStart))) {
                    elementStart++;
                }
                while (elementEnd > elementStart && Chars.isAsciiWhitespace(seq.charAt(elementEnd - 1))) {
                    elementEnd--;
                }

                if (elementStart >= elementEnd) {
                    throw SqlException.$(errorPos, "Empty element in date list");
                }

                // Check if element starts with '$' (date variable expression)
                // Date variables like $today, $now, $yesterday, $tomorrow, $today+3bd
                // The expression ends at '@' (timezone), '#' (day filter), or element end
                CharSequence elementSeq = seq;
                int resolvedElementStart = elementStart;
                int resolvedElementEnd = elementEnd;

                if (seq.charAt(elementStart) == '$') {
                    // Check for range operator (..) in element - e.g., "$today..$today+5d"
                    int rangeOpPos = findRangeOperator(seq, elementStart, elementEnd);

                    if (rangeOpPos >= 0) {
                        // Handle date variable range - expand into multiple dates
                        expandDateVariableRange(
                                timestampDriver,
                                configuration,
                                seq,
                                elementStart,
                                elementEnd,
                                rangeOpPos,
                                lim,
                                suffixStart,
                                globalTzMarker,
                                globalTzLo,
                                globalTzHi,
                                dayFilterMask,
                                nowTimestamp,
                                errorPos,
                                out,
                                operation,
                                sink,
                                applyEncoded,
                                outSizeBeforeExpansion
                        );
                        elementStart = i + 1;
                        continue;
                    }

                    // Find where the date expression ends
                    int exprEnd = elementEnd;
                    for (int j = elementStart; j < elementEnd; j++) {
                        char ec = seq.charAt(j);
                        if (ec == '@' || ec == '#') {
                            exprEnd = j;
                            break;
                        }
                    }

                    // Evaluate the date expression
                    long resolvedTimestamp = DateExpressionEvaluator.evaluate(
                            timestampDriver, seq, elementStart, exprEnd, nowTimestamp, errorPos
                    );

                    // Check if timestamp has time component (e.g., $now)
                    // If so, format with full datetime; otherwise just date
                    StringSink dateVarSink = tlDateVarSink.get();
                    dateVarSink.clear();
                    long startOfDay = timestampDriver.startOfDay(resolvedTimestamp, 0);
                    boolean hasTimeComponent = resolvedTimestamp != startOfDay;

                    if (hasTimeComponent) {
                        // $now - format with full datetime, timezone suffix still applies
                        appendDateTime(timestampDriver, resolvedTimestamp, dateVarSink);
                    } else {
                        // $today, $yesterday, $tomorrow - format as date only
                        appendDate(timestampDriver, resolvedTimestamp, dateVarSink);
                    }

                    // Append any suffix (timezone/day filter) from the original element
                    if (exprEnd < elementEnd) {
                        dateVarSink.put(seq, exprEnd, elementEnd);
                    }

                    elementSeq = dateVarSink;
                    resolvedElementStart = 0;
                    resolvedElementEnd = dateVarSink.length();
                }

                // Check for per-element day filter (e.g., "2024-01-01#Mon")
                // Order is: date@timezone#dayFilter, so find # first, then @ before it
                int elemDayFilterMarker = -1;
                for (int j = resolvedElementStart; j < resolvedElementEnd; j++) {
                    if (elementSeq.charAt(j) == '#') {
                        elemDayFilterMarker = j;
                        break;
                    }
                }

                int elemDayFilterMask = 0;
                LongList elemExchangeSchedule = null;
                int effectiveElementEndForTz = resolvedElementEnd;
                if (elemDayFilterMarker >= 0) {
                    // First try to look up as tick calendar
                    elemExchangeSchedule = getExchangeSchedule(configuration, elementSeq, elemDayFilterMarker + 1, resolvedElementEnd);
                    if (elemExchangeSchedule == null) {
                        // Not an tick calendar, parse as day filter
                        elemDayFilterMask = parseDayFilter(elementSeq, elemDayFilterMarker + 1, resolvedElementEnd, errorPos);
                    }
                    effectiveElementEndForTz = elemDayFilterMarker;
                }

                // Check if element has per-date timezone (e.g., "2024-01-01@Europe/London")
                // Look for @ only before the day filter marker (if present)
                int elemTzMarker = findTimezoneMarker(elementSeq, resolvedElementStart, effectiveElementEndForTz);
                int elemTzLo;
                int elemTzHi;
                int effectiveElementEnd = effectiveElementEndForTz;

                // Determine which timezone to use: per-element takes precedence over global
                // Track both the timezone bounds and the sequence they reference
                int activeTzLo = -1;
                int activeTzHi = -1;
                CharSequence activeTzSeq = seq;

                if (elemTzMarker >= 0) {
                    // Per-element timezone
                    elemTzLo = elemTzMarker + 1;
                    elemTzHi = effectiveElementEndForTz;
                    effectiveElementEnd = elemTzMarker;
                    activeTzLo = elemTzLo;
                    activeTzHi = elemTzHi;
                    activeTzSeq = elementSeq;
                } else if (globalTzMarker >= 0) {
                    // Use global timezone from suffix
                    activeTzLo = globalTzLo;
                    activeTzHi = globalTzHi;
                }

                // Determine which day filter to use: per-element takes precedence over global
                int activeDayFilterMask = elemDayFilterMask != 0 ? elemDayFilterMask : dayFilterMask;
                // Per-element exchange schedule (no global fallback here - global is applied after expandDateList)
                LongList activeExchangeSchedule = elemExchangeSchedule;

                // Remember output size before parsing this element
                int outSizeBeforeElement = out.size();

                // Check if element already has a time component (contains 'T' followed by digit)
                boolean elementHasTime = false;
                for (int j = resolvedElementStart; j < effectiveElementEnd - 1; j++) {
                    char ec = elementSeq.charAt(j);
                    if (ec == 'T' && Chars.isAsciiDigit(elementSeq.charAt(j + 1))) {
                        elementHasTime = true;
                        break;
                    }
                }

                // Find where the time part of suffix ends (at ';' or '@' or end)
                // Suffix format: T10:00, T10:00;1h, T10:00@TZ, T10:00@TZ;1h
                int suffixTimeEnd = suffixStart;
                if (suffixStart < lim && seq.charAt(suffixStart) == 'T') {
                    // Find where time ends (at ';', '@', or end)
                    while (suffixTimeEnd < lim) {
                        char sc = seq.charAt(suffixTimeEnd);
                        if (sc == ';' || sc == '@') {
                            break;
                        }
                        suffixTimeEnd++;
                    }
                }

                // Build the full interval string: element (without tz) + suffix (without tz)
                // When tick calendar filter is present, exclude duration from parsing
                // (duration will be applied after the tick calendar filter)
                sink.clear();
                sink.put(elementSeq, resolvedElementStart, effectiveElementEnd);

                // If element already has time, skip the time part of suffix but include duration
                int effectiveSuffixStart = elementHasTime ? suffixTimeEnd : suffixStart;

                // Determine suffix end - exclude duration if tick calendar is active
                int effectiveSuffixEnd = lim;
                if (activeExchangeSchedule != null && globalDurationSemicolon >= 0) {
                    effectiveSuffixEnd = globalDurationSemicolon;
                }

                if (globalTzMarker >= 0) {
                    // Add suffix without timezone: part before @
                    if (effectiveSuffixStart < globalTzMarker) {
                        sink.put(seq, effectiveSuffixStart, globalTzMarker);
                    }
                    // Add part after timezone but before duration end
                    if (globalTzHi < effectiveSuffixEnd) {
                        sink.put(seq, globalTzHi, effectiveSuffixEnd);
                    }
                } else {
                    sink.put(seq, effectiveSuffixStart, effectiveSuffixEnd);
                }

                // Check if the combined string contains brackets that need expansion
                // This includes brackets in the element part AND in the suffix part
                boolean hasBrackets = false;
                int expandedLen = sink.length();
                for (int j = 0; j < expandedLen; j++) {
                    if (sink.charAt(j) == '[') {
                        hasBrackets = true;
                        break;
                    }
                }

                if (hasBrackets) {
                    // Element has field expansion brackets - use existing recursive expansion
                    // Find where semicolon would be in the expanded string
                    int expandedLim = sink.length();
                    int dateLim = expandedLim;
                    for (int j = 0; j < expandedLim; j++) {
                        char ch = sink.charAt(j);
                        if (ch == ';') {
                            // Semicolon always appears after brackets close (in suffix like T[09,14]:30;1h)
                            dateLim = j;
                            break;
                        }
                    }

                    // Use tlSink2 since tlSink1 may be in use by the caller (dateSink when day filter exists)
                    StringSink expansionSink = tlSink2.get();
                    expansionSink.clear();
                    boolean hadTimeListBracket = expandBracketsRecursive(
                            timestampDriver,
                            configuration,
                            sink,
                            0,
                            dateLim,
                            sink.length(),
                            errorPos,
                            out,
                            operation,
                            expansionSink,
                            0,
                            applyEncoded,
                            outSizeBeforeExpansion,
                            activeTzSeq,   // tzSeq - for time list TZ fallback
                            activeTzLo,    // tzLo
                            activeTzHi     // tzHi
                    );
                    // If time list brackets were processed, they handled TZ internally
                    if (hadTimeListBracket) {
                        activeTzLo = -1; // Clear to skip TZ application below
                    }
                } else {
                    // No brackets - parse directly
                    parseExpandedInterval(timestampDriver, sink, errorPos, out, operation, applyEncoded, outSizeBeforeExpansion);
                }

                // Day filter applies BEFORE timezone conversion (based on local time)
                if (activeDayFilterMask != 0 && activeExchangeSchedule == null) {
                    if (applyEncoded) {
                        // Only expand to individual days if the element is an imprecise date (year/month)
                        // Precise dates (with day component) should just be filtered, not expanded
                        applyDayFilter(
                                timestampDriver,
                                out,
                                outSizeBeforeElement,
                                activeDayFilterMask,
                                hasDatePrecision(elementSeq, resolvedElementStart, effectiveElementEnd)
                        );
                    } else {
                        // Dynamic mode: store day filter mask for runtime evaluation
                        setDayFilterMaskOnEncodedIntervals(out, outSizeBeforeElement, activeDayFilterMask);
                    }
                }

                // Apply timezone conversion if present (per-element or global) and NOT already handled by time list
                if (activeTzLo >= 0) {
                    applyTimezoneToIntervals(timestampDriver, configuration, out, outSizeBeforeElement, activeTzSeq, activeTzLo, activeTzHi, errorPos, applyEncoded);
                }

                // Tick calendar filter applies AFTER timezone conversion (intersects with UTC trading hours)
                if (activeExchangeSchedule != null) {
                    applyExchangeFilterAndDuration(timestampDriver, activeExchangeSchedule, out, outSizeBeforeElement, seq,
                            globalDurationSemicolon >= 0 ? globalDurationSemicolon + 1 : -1, lim, errorPos);
                }

                elementStart = i + 1;
            }
        }
    }

    /**
     * Expands a date variable range like "$today..$today+5d" or "$today..$today+5bd" into multiple individual dates.
     * <p>
     * Range type is determined by the END expression:
     * - If end expression ends with "bd", iterate business days only (skip Sat/Sun)
     * - Otherwise, iterate all calendar days
     * <p>
     * Each generated date is processed like a single date element with the shared suffix.
     * Adjacent full-day intervals will be merged by the caller's unionBracketExpandedIntervals.
     * <p>
     * Consider extracting a shared helper method if this becomes a maintenance burden.
     */
    private static void expandDateVariableRange(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence seq,
            int elementStart,
            int elementEnd,
            int rangeOpPos,
            int lim,
            int suffixStart,
            int globalTzMarker,
            int globalTzLo,
            int globalTzHi,
            int dayFilterMask,
            long nowTimestamp,
            int errorPos,
            LongList out,
            short operation,
            StringSink sink,
            boolean applyEncoded,
            int outSizeBeforeExpansion
    ) throws SqlException {
        // Find duration semicolon in suffix (for applying duration after tick calendar filter)
        int durationSemicolon = findDurationSemicolon(seq, suffixStart, lim);

        // Find where the range expression ends (at '@' timezone or '#' exchange/day filter marker, or element end)
        // The global '#' is stripped from seq at the top level, but per-element '#' inside brackets is not.
        int rangeEnd = elementEnd;
        for (int j = rangeOpPos + 2; j < elementEnd; j++) {
            char ec = seq.charAt(j);
            if (ec == '@' || ec == '#') {
                rangeEnd = j;
                break;
            }
        }

        // Extract start and end expressions
        int startExprHi = rangeOpPos;
        int endExprLo = rangeOpPos + 2; // skip ".."
        int endExprHi = rangeEnd;

        // Trim whitespace from start expression (trailing)
        while (startExprHi > elementStart && Chars.isAsciiWhitespace(seq.charAt(startExprHi - 1))) {
            startExprHi--;
        }
        // Trim whitespace from end expression (leading and trailing)
        while (endExprLo < endExprHi && Chars.isAsciiWhitespace(seq.charAt(endExprLo))) {
            endExprLo++;
        }
        while (endExprHi > endExprLo && Chars.isAsciiWhitespace(seq.charAt(endExprHi - 1))) {
            endExprHi--;
        }

        // Start expression is never empty: we only enter this function if element starts with '$',
        // and '$' is not whitespace, so whitespace trimming always preserves at least '$'.
        assert elementStart < startExprHi : "Empty start expression in date range";
        if (endExprLo >= endExprHi) {
            throw SqlException.$(errorPos + rangeOpPos - elementStart, "Empty end expression in date range");
        }

        // Evaluate start and end timestamps
        long startTimestamp = DateExpressionEvaluator.evaluate(
                timestampDriver, seq, elementStart, startExprHi, nowTimestamp, errorPos
        );
        long endTimestamp = DateExpressionEvaluator.evaluate(
                timestampDriver, seq, endExprLo, endExprHi, nowTimestamp, errorPos
        );

        // Validate start <= end
        if (startTimestamp > endTimestamp) {
            throw SqlException.$(errorPos + rangeOpPos - elementStart, "Invalid date range: start is after end");
        }

        // Check if BOTH endpoints have time precision (not at start of day)
        // If so, produce a single interval from start to end instead of iterating by days
        // Note: we require BOTH to have time precision, so that expressions like
        // "$now..$tomorrow" still iterate by days (since $tomorrow is at midnight)
        long startDay = timestampDriver.startOfDay(startTimestamp, 0);
        long endDay = timestampDriver.startOfDay(endTimestamp, 0);
        boolean hasTimePrecision = (startTimestamp != startDay) && (endTimestamp != endDay);

        if (hasTimePrecision) {
            // Time-based range: produce single interval from start to end
            int outSizeBeforeInterval = out.size();
            encodeInterval(startTimestamp, endTimestamp, operation, out);
            if (applyEncoded) {
                // Static mode: convert from 4-long format to 2-long format
                applyLastEncodedInterval(timestampDriver, out);
            }

            // Apply day filter BEFORE timezone conversion (local-time semantics)
            // Day filter should check the day in local time, not UTC
            if (dayFilterMask != 0) {
                if (applyEncoded) {
                    // Static mode: filter intervals directly
                    applyDayFilter(timestampDriver, out, outSizeBeforeInterval, dayFilterMask, true);
                } else {
                    // Dynamic mode: store mask for runtime evaluation
                    setDayFilterMaskOnEncodedIntervals(out, outSizeBeforeInterval, dayFilterMask);
                }
            }

            // Resolve active timezone: element-level takes precedence over global
            // This mirrors the timezone resolution in the day-based path below
            int activeTzLo = -1;
            int activeTzHi = -1;

            int elemTzMarker = findTimezoneMarker(seq, rangeEnd, elementEnd);
            if (elemTzMarker >= 0) {
                // Element-level timezone (inside the range element)
                activeTzLo = elemTzMarker + 1;
                activeTzHi = elementEnd;
            } else if (globalTzMarker >= 0) {
                // Fall back to global timezone (outside brackets)
                activeTzLo = globalTzLo;
                activeTzHi = globalTzHi;
            }

            // Apply timezone AFTER day filter (converts local to UTC)
            if (activeTzLo >= 0) {
                applyTimezoneToIntervals(timestampDriver, configuration, out, outSizeBeforeInterval, seq, activeTzLo, activeTzHi, errorPos, applyEncoded);
            }
            return;
        }

        // Day-based range: iterate from start to end by days
        startTimestamp = startDay;
        endTimestamp = endDay;

        // Determine if business day range (end expression ends with "bd")
        boolean isBusinessDayRange = isBusinessDayExpression(seq, endExprHi);

        // Extract suffix after range expression (timezone/day filter)
        int suffixAfterRangeLo = -1;
        int suffixAfterRangeHi = -1;
        if (rangeEnd < elementEnd) {
            suffixAfterRangeLo = rangeEnd;
            suffixAfterRangeHi = elementEnd;
        }

        // Iterate from start to end, generating dates
        StringSink dateVarSink = tlDateVarSink.get();
        long currentTimestamp = startTimestamp;
        long previousTimestamp = currentTimestamp - 1; // For infinite loop protection

        while (currentTimestamp <= endTimestamp) {
            // Safety check: addDays always advances the timestamp, so this should never trigger
            assert currentTimestamp > previousTimestamp : "Date range iteration not advancing";
            previousTimestamp = currentTimestamp;

            int dow = timestampDriver.getDayOfWeek(currentTimestamp);

            // For business day ranges, skip weekends
            if (isBusinessDayRange && (dow == SATURDAY || dow == SUNDAY)) {
                currentTimestamp = timestampDriver.addDays(currentTimestamp, 1);
                continue;
            }

            // Format the date
            dateVarSink.clear();
            appendDate(timestampDriver, currentTimestamp, dateVarSink);

            // Append suffix from original element (timezone/day filter after range)
            if (suffixAfterRangeLo >= 0) {
                dateVarSink.put(seq, suffixAfterRangeLo, suffixAfterRangeHi);
            }

            // Now process this date like a single element
            // This is similar to the single date variable processing in expandDateList
            int resolvedElementStart = 0;
            int resolvedElementEnd = dateVarSink.length();

            // Check for per-element day filter
            int elemDayFilterMarker = -1;
            for (int j = resolvedElementStart; j < resolvedElementEnd; j++) {
                if (dateVarSink.charAt(j) == '#') {
                    elemDayFilterMarker = j;
                    break;
                }
            }

            int elemDayFilterMask = 0;
            LongList elemExchangeSchedule = null;
            int effectiveElementEndForTz = resolvedElementEnd;
            if (elemDayFilterMarker >= 0) {
                // First try to look up as tick calendar
                elemExchangeSchedule = getExchangeSchedule(configuration, dateVarSink, elemDayFilterMarker + 1, resolvedElementEnd);
                if (elemExchangeSchedule == null) {
                    // Not an tick calendar, parse as day filter
                    elemDayFilterMask = parseDayFilter(dateVarSink, elemDayFilterMarker + 1, resolvedElementEnd, errorPos);
                }
                effectiveElementEndForTz = elemDayFilterMarker;
            }
            // Check for per-element timezone (search only up to '#' marker, matching expandDateList)
            int elemTzMarker = findTimezoneMarker(dateVarSink, resolvedElementStart, effectiveElementEndForTz);
            int effectiveElementEnd = effectiveElementEndForTz;

            int activeTzLo = -1;
            int activeTzHi = -1;
            CharSequence activeTzSeq = seq;

            if (elemTzMarker >= 0) {
                activeTzLo = elemTzMarker + 1;
                activeTzHi = resolvedElementEnd;
                effectiveElementEnd = elemTzMarker;
                activeTzSeq = dateVarSink;
            } else if (globalTzMarker >= 0) {
                activeTzLo = globalTzLo;
                activeTzHi = globalTzHi;
            }

            int activeDayFilterMask = elemDayFilterMask != 0 ? elemDayFilterMask : dayFilterMask;
            LongList activeExchangeSchedule = elemExchangeSchedule;
            // Note: per-element day filter would always be 0 since '#' is stripped at top level
            int outSizeBeforeElement = out.size();

            // Build the full interval string
            // When tick calendar filter is present, exclude duration from parsing
            // (duration will be applied after the tick calendar filter)
            sink.clear();
            sink.put(dateVarSink, resolvedElementStart, effectiveElementEnd);

            // Determine suffix end - exclude duration if tick calendar is active
            int effectiveSuffixEnd = lim;
            if (activeExchangeSchedule != null && durationSemicolon >= 0) {
                effectiveSuffixEnd = durationSemicolon;
            }

            // Add suffix (time, duration) from global suffix
            if (globalTzMarker >= 0) {
                if (suffixStart < globalTzMarker) {
                    sink.put(seq, suffixStart, globalTzMarker);
                }
                if (globalTzHi < effectiveSuffixEnd) {
                    sink.put(seq, globalTzHi, effectiveSuffixEnd);
                }
            } else {
                sink.put(seq, suffixStart, effectiveSuffixEnd);
            }

            // Check for brackets in combined string
            boolean hasBrackets = false;
            int expandedLen = sink.length();
            for (int j = 0; j < expandedLen; j++) {
                if (sink.charAt(j) == '[') {
                    hasBrackets = true;
                    break;
                }
            }

            if (hasBrackets) {
                int expandedLim = sink.length();
                int dateLim = expandedLim;
                for (int j = 0; j < expandedLim; j++) {
                    char ch = sink.charAt(j);
                    if (ch == ';') {
                        dateLim = j;
                        break;
                    }
                }

                StringSink expansionSink = tlSink2.get();
                expansionSink.clear();
                boolean hadTimeListBracket = expandBracketsRecursive(
                        timestampDriver,
                        configuration,
                        sink,
                        0,
                        dateLim,
                        sink.length(),
                        errorPos,
                        out,
                        operation,
                        expansionSink,
                        0,
                        applyEncoded,
                        outSizeBeforeExpansion,
                        activeTzSeq,
                        activeTzLo,
                        activeTzHi
                );
                if (hadTimeListBracket) {
                    activeTzLo = -1;
                }
            } else {
                parseExpandedInterval(timestampDriver, sink, errorPos, out, operation, applyEncoded, outSizeBeforeExpansion);
            }

            // Day filter applies BEFORE timezone conversion (based on local time)
            if (activeDayFilterMask != 0 && activeExchangeSchedule == null) {
                if (applyEncoded) {
                    // expandMultiDay=false: appendDate always produces full "YYYY-MM-DD" dates
                    applyDayFilter(timestampDriver, out, outSizeBeforeElement, dayFilterMask, true);
                } else {
                    setDayFilterMaskOnEncodedIntervals(out, outSizeBeforeElement, dayFilterMask);
                }
            }

            // Apply timezone conversion
            if (activeTzLo >= 0) {
                applyTimezoneToIntervals(timestampDriver, configuration, out, outSizeBeforeElement, activeTzSeq, activeTzLo, activeTzHi, errorPos, applyEncoded);
            }

            // Tick calendar filter applies AFTER timezone conversion (intersects with UTC trading hours)
            if (activeExchangeSchedule != null) {
                applyExchangeFilterAndDuration(timestampDriver, activeExchangeSchedule, out, outSizeBeforeElement, seq,
                        durationSemicolon >= 0 ? durationSemicolon + 1 : -1, lim, errorPos);
            }

            // Incremental merge: when interval count exceeds threshold, merge to bound memory
            // This prevents unbounded growth from large ranges like [$today..$today+10000d]
            if (applyEncoded) {
                int intervalCount = (out.size() - outSizeBeforeExpansion) / 2;
                if (intervalCount >= configuration.getSqlIntervalIncrementalMergeThreshold()) {
                    unionBracketExpandedIntervals(out, outSizeBeforeExpansion);
                    // Check if we still exceed max limit after merging
                    intervalCount = (out.size() - outSizeBeforeExpansion) / 2;
                    int maxIntervalsAfterMerge = configuration.getSqlIntervalMaxIntervalsAfterMerge();
                    if (intervalCount > maxIntervalsAfterMerge) {
                        throw SqlException.$(errorPos, "Date range expansion produces too many intervals (max ")
                                .put(maxIntervalsAfterMerge).put(')');
                    }
                }
            }

            // Move to next day
            currentTimestamp = timestampDriver.addDays(currentTimestamp, 1);
        }
    }

    /**
     * Expands a time list bracket where each element is a full time value.
     * Example: [09:00,14:30,16:00] expands to 3 intervals.
     * Supports per-element timezones: [09:00@Asia/Tokyo,08:00@Europe/London]
     * If no per-element timezone, falls back to global timezone if provided.
     *
     * @param timestampDriver        timestamp driver for parsing
     * @param seq                    the full input string
     * @param bracketStart           position of the opening '['
     * @param bracketEnd             position of the closing ']'
     * @param fullLim                end of entire string (including duration suffix)
     * @param errorPos               position for error reporting
     * @param out                    output list for parsed intervals
     * @param operation              interval operation
     * @param sink                   reusable StringSink containing the prefix (e.g., "2024-01-15T")
     * @param sinkPrefixLen          length of prefix to preserve in sink
     * @param applyEncoded           if true, converts to simple format
     * @param outSizeBeforeExpansion size of out before expansion started
     * @param globalTzSeq            original sequence containing timezone (for applying global TZ)
     * @param globalTzLo             start of global timezone in globalTzSeq (-1 if none)
     * @param globalTzHi             end of global timezone in globalTzSeq (-1 if none)
     */
    private static void expandTimeListBracket(
            TimestampDriver timestampDriver,
            CairoConfiguration configuration,
            CharSequence seq,
            int bracketStart,
            int bracketEnd,
            int fullLim,
            int errorPos,
            LongList out,
            short operation,
            StringSink sink,
            int sinkPrefixLen,
            boolean applyEncoded,
            int outSizeBeforeExpansion,
            CharSequence globalTzSeq,
            int globalTzLo,
            int globalTzHi
    ) throws SqlException {
        int i = bracketStart + 1;

        // Check once if suffix contains brackets that need expansion (same for all elements)
        boolean suffixHasBrackets = false;
        for (int j = bracketEnd + 1; j < fullLim; j++) {
            if (seq.charAt(j) == '[') {
                suffixHasBrackets = true;
                break;
            }
        }

        // Allocate recursion sink once outside the loop (only if needed)
        StringSink recursionSink = null;
        if (suffixHasBrackets) {
            recursionSink = tlSink2.get();
            recursionSink.clear();
        }

        while (i < bracketEnd) {
            // Skip whitespace
            while (i < bracketEnd && Chars.isAsciiWhitespace(seq.charAt(i))) {
                i++;
            }
            if (i >= bracketEnd) {
                break;
            }

            // Find element end (comma or bracket end)
            int elemStart = i;
            while (i < bracketEnd && seq.charAt(i) != ',') {
                i++;
            }
            int elemEnd = i;

            // Trim trailing whitespace from element
            while (elemEnd > elemStart && Chars.isAsciiWhitespace(seq.charAt(elemEnd - 1))) {
                elemEnd--;
            }

            if (elemStart >= elemEnd) {
                throw SqlException.$(errorPos, "Empty element in time list");
            }

            // Check for nested brackets in element (not supported)
            for (int j = elemStart; j < elemEnd; j++) {
                if (seq.charAt(j) == '[') {
                    throw SqlException.$(errorPos, "Nested brackets not supported in time list. Use separate expansions: T[09:00,09:30] instead of T[09:[00,30]]");
                }
            }

            // Check for per-element timezone (@)
            int tzMarker = -1;
            for (int j = elemStart; j < elemEnd; j++) {
                if (seq.charAt(j) == '@') {
                    tzMarker = j;
                    break;
                }
            }

            int timeEnd = tzMarker >= 0 ? tzMarker : elemEnd;
            int tzLo = tzMarker >= 0 ? tzMarker + 1 : -1;
            int tzHi = tzMarker >= 0 ? elemEnd : -1;

            // Remember output size before parsing this element
            int outSizeBeforeElement = out.size();

            // Build full timestamp: prefix + time element (without @tz) + suffix after bracket
            sink.put(seq, elemStart, timeEnd);          // time value (e.g., "09:00")
            sink.put(seq, bracketEnd + 1, fullLim);     // suffix (e.g., ";6h")

            if (suffixHasBrackets) {
                // Suffix has brackets - need recursive expansion
                // Find where the date part ends (before semicolon) in the expanded string
                int expandedLen = sink.length();
                int expandedDateLim = expandedLen;
                for (int j = 0; j < expandedLen; j++) {
                    if (sink.charAt(j) == ';') {
                        expandedDateLim = j;
                        break;
                    }
                }

                // Recursively expand brackets in the suffix
                expandBracketsRecursive(
                        timestampDriver,
                        configuration,
                        sink,
                        // intervalStart
                        0,              // pos
                        expandedDateLim,
                        sink.length(),
                        errorPos,
                        out,
                        operation,
                        recursionSink,
                        0,              // depth
                        applyEncoded,
                        outSizeBeforeExpansion,
                        globalTzSeq,
                        tzLo >= 0 ? -1 : globalTzLo,  // Skip global TZ if per-element TZ
                        tzLo >= 0 ? -1 : globalTzHi
                );
            } else {
                // No brackets in suffix - parse directly
                parseExpandedInterval(timestampDriver, sink, errorPos, out, operation, applyEncoded, outSizeBeforeExpansion);
            }

            // Apply timezone: per-element takes precedence, then global fallback
            if (tzLo >= 0) {
                // Per-element timezone
                applyTimezoneToIntervals(timestampDriver, configuration, out, outSizeBeforeElement, seq, tzLo, tzHi, errorPos, applyEncoded);
            } else if (globalTzLo >= 0) {
                // Global timezone as fallback
                applyTimezoneToIntervals(timestampDriver, configuration, out, outSizeBeforeElement, globalTzSeq, globalTzLo, globalTzHi, errorPos, applyEncoded);
            }

            // Reset sink to prefix for next element
            sink.clear(sinkPrefixLen);

            // Skip comma (if i < bracketEnd, we exited the element loop at a comma)
            if (i < bracketEnd) {
                i++;
            }
        }
    }

    /**
     * Finds the position of '#' day filter marker in the string, respecting brackets.
     * The '#' must be outside brackets and before ';' (duration suffix).
     * Timezone is optional: both "2024-01-01#Mon" and "2024-01-01@+05:00#Mon" are valid.
     *
     * @param seq the input string
     * @param lo  start index
     * @param lim end index
     * @return position of '#' or -1 if not found
     */
    private static int findDayFilterMarker(CharSequence seq, int lo, int lim) {
        int depth = 0;
        for (int i = lo; i < lim; i++) {
            char c = seq.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
            } else if (depth == 0) {
                if (c == '#') {
                    return i;
                }
                // Stop at duration marker
                if (c == ';') {
                    return -1;
                }
            }
        }
        return -1;
    }

    /**
     * Returns the index of the first ';' in seq[lo, lim), or -1 if none.
     */
    private static int findDurationSemicolon(CharSequence seq, int lo, int lim) {
        for (int i = lo; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                return i;
            }
        }
        return -1;
    }

    private static int findMatchingBracket(CharSequence seq, int start, int lim, int position) throws SqlException {
        // start points to '['
        int depth = 1;
        for (int i = start + 1; i < lim; i++) {
            char c = seq.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw SqlException.$(position, "Unclosed '[' in interval");
    }

    /**
     * Finds the position of ".." range operator in a date variable expression.
     * Stops searching at '@' (timezone) or '#' (day filter) markers.
     *
     * @param seq the input string
     * @param lo  start index
     * @param hi  end index
     * @return position of the first '.' in ".." or -1 if not found
     */
    private static int findRangeOperator(CharSequence seq, int lo, int hi) {
        for (int i = lo; i < hi - 1; i++) {
            char c = seq.charAt(i);
            // Stop at '@' or '#' (timezone/day filter markers that appear after the range)
            if (c == '@' || c == '#') {
                break;
            }
            // Valid date variable expressions only use '.' in ".." range operator
            if (c == '.') {
                return i;
            }
        }
        return -1;
    }

    /**
     * Finds the position of '@' timezone marker in the string, respecting brackets.
     * The '@' must be outside brackets and before any ';' (duration suffix).
     * Searches backwards to find the LAST '@' before ';' to handle edge cases.
     *
     * @param seq the input string
     * @param lo  start index
     * @param lim end index (stop at ';' if found)
     * @return position of '@' or -1 if not found
     */
    private static int findTimezoneMarker(CharSequence seq, int lo, int lim) {
        // First find where ';' is (if any) to set the effective limit
        int effectiveLim = lim;
        int depth = 0;
        for (int i = lo; i < lim; i++) {
            char c = seq.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
            } else if (c == ';' && depth == 0) {
                effectiveLim = i;
                break;
            }
        }

        // Now search backwards for '@' outside brackets
        depth = 0;
        for (int i = effectiveLim - 1; i >= lo; i--) {
            char c = seq.charAt(i);
            if (c == ']') {
                depth++;
            } else if (c == '[') {
                depth--;
            } else if (c == '@' && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static short getEffectiveOp(short operation, boolean isFirstInterval) {
        short effectiveOp;
        if (operation == IntervalOperation.SUBTRACT) {
            // For SUBTRACT: each interval is processed individually (inverted and intersected)
            // This achieves: NOT A AND NOT B AND NOT C...
            effectiveOp = operation;
        } else {
            // For INTERSECT: first uses INTERSECT, subsequent use UNION to combine
            // This achieves: (A OR B OR C...) AND previous
            effectiveOp = isFirstInterval ? operation : IntervalOperation.UNION;
        }
        return effectiveOp;
    }

    /**
     * Looks up an tick calendar schedule for the token between {@code lo} (inclusive) and
     * {@code hi} (exclusive) in {@code seq}. Returns the schedule {@link LongList} if the
     * token identifies a known exchange, or {@code null} otherwise.
     */
    @Nullable
    private static LongList getExchangeSchedule(CairoConfiguration configuration, CharSequence seq, int lo, int hi) {
        return configuration.getFactoryProvider()
                .getTickCalendarServiceFactory()
                .getInstance()
                .getSchedule(tlExchangeCs.get().of(seq, lo, hi - lo));
    }

    /**
     * Checks if a date string has day precision (contains at least 2 hyphens).
     * Callers must ensure [lo, hi) contains only the date part (no 'T', '@', '#').
     * - "2024" has 0 hyphens -> false (year only)
     * - "2024-01" has 1 hyphen -> false (year-month)
     * - "2024-W03" has 1 hyphen -> false (ISO week)
     * - "2024-01-15" has 2 hyphens -> true (full date)
     * - "2024-W03-1" has 2 hyphens -> true (ISO week with day)
     */
    private static boolean hasDatePrecision(CharSequence seq, int lo, int hi) {
        int hyphenCount = 0;
        for (int i = lo; i < hi; i++) {
            if (seq.charAt(i) == '-') {
                hyphenCount++;
                if (hyphenCount >= 2) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if a date expression ends with "bd" (business days).
     * Used to determine if a range should iterate business days or calendar days.
     *
     * @param seq the input string
     * @param hi  end index
     * @return true if expression ends with "bd" (case-insensitive)
     */
    private static boolean isBusinessDayExpression(CharSequence seq, int hi) {
        // Caller guarantees hi - lo >= 4 (shortest valid expression is "$now")
        // via the empty expression check and DateExpressionEvaluator validation
        char c1 = seq.charAt(hi - 2);
        char c2 = seq.charAt(hi - 1);
        return (c1 | 32) == 'b' && (c2 | 32) == 'd';
    }

    /**
     * Determines if a string starting with '[' is a date list or field expansion.
     * <p>
     * A date list: [2025-01-01,2025-01-05] or [2025-01-01]T09:30
     * Field expansion: [1]-[1]-[1]T... or [2018,2019]-01-10
     * <p>
     * The key difference: after the closing ']' that matches the opening '[':
     * - Date list: followed by end-of-string, 'T', ';', '@' (timezone), or whitespace
     * - Field expansion: followed by '-', ':', '.', or digit (part of date format)
     *
     * @param seq string that starts with '['
     * @param lo  position of the opening '['
     * @param lim end of string
     * @return true if this is a date list, false if it's field expansion
     */
    private static boolean isDateList(CharSequence seq, int lo, int lim) {
        // Find the matching ']' for the opening '['
        int depth = 1;
        for (int i = lo + 1; i < lim; i++) {
            char c = seq.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    // Found matching ']', check what follows
                    if (i + 1 >= lim) {
                        // End of string - it's a date list
                        return true;
                    }
                    char next = seq.charAt(i + 1);
                    // Date list: followed by T, ;, @ (timezone marker), or whitespace
                    // Field expansion: followed by -, :, ., or digit (part of date format)
                    return next == 'T' || next == ';' || next == '@' || Chars.isAsciiWhitespace(next);
                }
            }
        }
        // No matching ']' found - will error later, treat as date list for now
        return true;
    }

    /**
     * Checks if a bracket contains a time list (has ':' in content).
     * Time list brackets contain full time values like [09:00,14:30] vs numeric expansion like [09,14].
     *
     * @param seq          the input string
     * @param bracketStart position of the opening '['
     * @param bracketEnd   position of the closing ']'
     * @return true if this is a time list bracket, false if it's numeric expansion
     */
    private static boolean isTimeListBracket(CharSequence seq, int bracketStart, int bracketEnd) {
        for (int i = bracketStart + 1; i < bracketEnd; i++) {
            if (seq.charAt(i) == ':') {
                return true;
            }
        }
        return false;
    }

    /**
     * Unions intervals in the range [startIndex, end) with each other.
     * Pre-existing intervals at [0, startIndex) are preserved unchanged.
     * Bracket values may be in any order (e.g., [20,10,15]), so we sort first.
     * This operates in-place without allocations.
     */
    private static void mergeAndValidateIntervals(CairoConfiguration configuration, LongList out, int startIndex, int errorPos) throws SqlException {
        if (out.size() > startIndex + 2) {
            unionBracketExpandedIntervals(out, startIndex);
            int intervalCount = (out.size() - startIndex) / 2;
            int maxIntervalsAfterMerge = configuration.getSqlIntervalMaxIntervalsAfterMerge();
            if (intervalCount > maxIntervalsAfterMerge) {
                throw SqlException.$(errorPos, "Bracket expansion produces too many intervals (max ")
                        .put(maxIntervalsAfterMerge).put(')');
            }
        }
    }

    /**
     * Computes the stride (days) from currentDow to the next matching day in the mask.
     * Uses bit manipulation with a doubled mask to handle week wrap-around.
     * <p>
     * Example for Tue+Fri (mask = 0b0010010, bits 1=Tue, 4=Fri):
     * - From Tue (dow=1): stride = 3 (to Fri)
     * - From Fri (dow=4): stride = 4 (to next Tue)
     */
    private static int nextMatchingDayStride(int currentDow, int dayFilterMask) {
        // Double the mask to handle wrap-around (7 days → 14 bits)
        int doubleMask = dayFilterMask | (dayFilterMask << 7);
        // Clear bits at and before currentDow
        int cleared = doubleMask & -(1 << (currentDow + 1));
        // Distance to next set bit
        return Integer.numberOfTrailingZeros(cleared) - currentDow;
    }

    /**
     * Parses a day filter specification and returns a bitmask of allowed days.
     * Supported formats:
     * <ul>
     *   <li>{@code workday} or {@code wd} - Monday through Friday</li>
     *   <li>{@code weekend} - Saturday and Sunday</li>
     *   <li>{@code Mon}, {@code Tue}, {@code Wed}, {@code Thu}, {@code Fri}, {@code Sat}, {@code Sun} - specific day</li>
     *   <li>{@code Mon,Wed,Fri} - comma-separated list of days</li>
     * </ul>
     *
     * @param seq      the input string
     * @param lo       start index of day filter spec (after '#')
     * @param hi       end index of day filter spec
     * @param position position for error reporting
     * @return bitmask of allowed days (bit 0 = Monday, bit 6 = Sunday)
     * @throws SqlException if the day filter spec is invalid
     */
    private static int parseDayFilter(CharSequence seq, int lo, int hi, int position) throws SqlException {
        if (lo >= hi) {
            throw SqlException.$(position, "Empty day filter after '#'");
        }

        // Check for keywords first based on length
        int len = hi - lo;
        if (len == 2) {
            // wd
            if ((seq.charAt(lo) | 32) == 'w' && (seq.charAt(lo + 1) | 32) == 'd') {
                return DAY_FILTER_WORKDAY;
            }
        } else if (len == 7) {
            // workday or weekend
            if ((seq.charAt(lo) | 32) == 'w'
                    && (seq.charAt(lo + 1) | 32) == 'o'
                    && (seq.charAt(lo + 2) | 32) == 'r'
                    && (seq.charAt(lo + 3) | 32) == 'k'
                    && (seq.charAt(lo + 4) | 32) == 'd'
                    && (seq.charAt(lo + 5) | 32) == 'a'
                    && (seq.charAt(lo + 6) | 32) == 'y') {
                return DAY_FILTER_WORKDAY;
            }
            if ((seq.charAt(lo) | 32) == 'w'
                    && (seq.charAt(lo + 1) | 32) == 'e'
                    && (seq.charAt(lo + 2) | 32) == 'e'
                    && (seq.charAt(lo + 3) | 32) == 'k'
                    && (seq.charAt(lo + 4) | 32) == 'e'
                    && (seq.charAt(lo + 5) | 32) == 'n'
                    && (seq.charAt(lo + 6) | 32) == 'd') {
                return DAY_FILTER_WEEKEND;
            }
        }

        // Parse comma-separated day names
        int mask = 0;
        int start = lo;
        for (int i = lo; i <= hi; i++) {
            char c = i < hi ? seq.charAt(i) : ',';
            if (c == ',') {
                if (i > start) {
                    int dayBit = parseSingleDay(seq, start, i, position);
                    mask |= dayBit;
                }
                start = i + 1;
            }
        }

        if (mask == 0) {
            throw SqlException.$(position, "Invalid day filter: ").put(seq, lo, hi);
        }

        return mask;
    }

    /**
     * Parses a fully-expanded interval string (no brackets) and adds result to output.
     * In static mode (applyEncoded=true), results are converted to 2-long format.
     * Union of bracket-expanded intervals is done at the end of parseTickExpr.
     * In dynamic mode (applyEncoded=false), results stay in 4-long format:
     * - For INTERSECT operations: first interval uses INTERSECT, subsequent use UNION
     * (intervals are combined, then intersected with previous constraints)
     * - For SUBTRACT operations: all intervals use SUBTRACT
     * (each interval is individually inverted and intersected, achieving NOT A AND NOT B)
     */
    private static void parseExpandedInterval(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int errorPos,
            LongList out,
            short operation,
            boolean applyEncoded,
            int outSizeBeforeExpansion
    ) throws SqlException {
        if (applyEncoded) {
            // Static mode: convert to 2-long format
            // Union is done at the end of bracket expansion in parseTickExpr
            parseIntervalSuffix(timestampDriver, seq, 0, seq.length(), errorPos, out, operation);
            applyLastEncodedInterval(timestampDriver, out);
        } else {
            // Dynamic mode: keep 4-long format
            boolean isFirstInterval = out.size() == outSizeBeforeExpansion;
            parseIntervalSuffix(timestampDriver, seq, 0, seq.length(), errorPos, out, getEffectiveOp(operation, isFirstInterval));
        }
    }

    private static void parseIntervalSuffix(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation
    ) throws SqlException {
        int writeIndex = out.size();
        int pos0 = 0, pos1 = 0, pos2 = 0;
        int p = -1;
        for (int i = lo; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                if (p > 1) {
                    throw SqlException.$(position, "Invalid interval format (too many semicolons): ").put(seq, lo, lim);
                }
                switch (++p) {
                    case 0 -> pos0 = i;
                    case 1 -> pos1 = i;
                    case 2 -> pos2 = i;
                }
            }
        }

        switch (p) {
            case -1:
                // no semicolons, just date part, which can be the interval in itself
                try {
                    timestampDriver.parseInterval(seq, lo, lim, operation, out);
                    break;
                } catch (NumericException ignore) {
                    // this must be a date then?
                }

                try {
                    long timestamp = timestampDriver.parseFloor(seq, lo, lim);
                    encodeInterval(timestamp, timestamp, operation, out);
                    break;
                } catch (NumericException e) {
                    try {
                        final long timestamp = Numbers.parseLong(seq);
                        encodeInterval(timestamp, timestamp, operation, out);
                        break;
                    } catch (NumericException e2) {
                        throw SqlException.$(position, "Invalid date: ").put(seq, lo, lim);
                    }
                }
            case 0:
                // single semicolon, expect a period format after date
                parseRange(timestampDriver, seq, lo, pos0, lim, position, operation, out);
                break;
            case 2:
                // 2018-01-10T10:30:00.000Z;30m;2d;2
                // means 10:30-11:00 every second day starting 2018-01-10
                int period;
                try {
                    period = Numbers.parseInt(seq, pos1 + 1, pos2 - 1);
                } catch (NumericException e) {
                    throw SqlException.$(position, "Period not a number");
                }
                int count;
                try {
                    count = Numbers.parseInt(seq, pos2 + 1, lim);
                } catch (NumericException e) {
                    throw SqlException.$(position, "Count not a number");
                }

                parseRange(timestampDriver, seq, lo, pos0, pos1, position, operation, out);
                char type = seq.charAt(pos2 - 1);
                long low = decodeIntervalLo(out, writeIndex);
                long hi = decodeIntervalHi(out, writeIndex);

                replaceHiLoInterval(low, hi, period, type, count, operation, out);
                switch (type) {
                    case PeriodType.YEAR:
                    case PeriodType.MONTH:
                    case PeriodType.HOUR:
                    case PeriodType.MINUTE:
                    case PeriodType.SECOND:
                    case PeriodType.DAY:
                        break;
                    default:
                        throw SqlException.$(position, "Unknown period: ").put(type).put(" at ").put(p - 1);
                }
                break;
            default:
                throw SqlException.$(position, "Invalid interval format: ").put(seq, lo, lim);
        }
    }

    private static void parseRange(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int p,
            int lim,
            int position,
            short operation,
            LongList out
    ) throws SqlException {
        try {
            int index = out.size();
            timestampDriver.parseInterval(seq, lo, p, operation, out);
            long low = decodeIntervalLo(out, index);
            long hi = addDuration(timestampDriver, low, seq, p + 1, lim, position) - 1;
            replaceHiLoInterval(low, hi, operation, out);
            return;
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMicros = timestampDriver.parseAnyFormat(seq, lo, p);
            long hiMicros = addDuration(timestampDriver, loMicros, seq, p + 1, lim, position) - 1;
            encodeInterval(loMicros, hiMicros, operation, out);
        } catch (NumericException e) {
            throw SqlException.$(position, "Invalid date: ").put(seq, lo, p);
        }
    }

    /**
     * Parses a single day name and returns its bitmask.
     *
     * @param seq      the input string
     * @param lo       start index
     * @param hi       end index
     * @param position position for error reporting
     * @return bitmask for the day (single bit set)
     * @throws SqlException if the day name is invalid
     */
    private static int parseSingleDay(CharSequence seq, int lo, int hi, int position) throws SqlException {
        int len = hi - lo;
        switch (len) {
            case 3:
                // Mon, Tue, Wed, Thu, Fri, Sat, Sun
                char c0 = (char) (seq.charAt(lo) | 32);
                char c1 = (char) (seq.charAt(lo + 1) | 32);
                char c2 = (char) (seq.charAt(lo + 2) | 32);
                if (c0 == 'm' && c1 == 'o' && c2 == 'n') return 1 << (MONDAY - 1);
                if (c0 == 't' && c1 == 'u' && c2 == 'e') return 1 << (TUESDAY - 1);
                if (c0 == 'w' && c1 == 'e' && c2 == 'd') return 1 << (WEDNESDAY - 1);
                if (c0 == 't' && c1 == 'h' && c2 == 'u') return 1 << (THURSDAY - 1);
                if (c0 == 'f' && c1 == 'r' && c2 == 'i') return 1 << (FRIDAY - 1);
                if (c0 == 's' && c1 == 'a' && c2 == 't') return 1 << (SATURDAY - 1);
                if (c0 == 's' && c1 == 'u' && c2 == 'n') return 1 << (SUNDAY - 1);
                break;
            case 6:
                // Monday, Friday, Sunday
                if ((seq.charAt(lo) | 32) == 'm'
                        && (seq.charAt(lo + 1) | 32) == 'o'
                        && (seq.charAt(lo + 2) | 32) == 'n'
                        && (seq.charAt(lo + 3) | 32) == 'd'
                        && (seq.charAt(lo + 4) | 32) == 'a'
                        && (seq.charAt(lo + 5) | 32) == 'y') {
                    return 1 << (MONDAY - 1);
                }
                if ((seq.charAt(lo) | 32) == 'f'
                        && (seq.charAt(lo + 1) | 32) == 'r'
                        && (seq.charAt(lo + 2) | 32) == 'i'
                        && (seq.charAt(lo + 3) | 32) == 'd'
                        && (seq.charAt(lo + 4) | 32) == 'a'
                        && (seq.charAt(lo + 5) | 32) == 'y') {
                    return 1 << (FRIDAY - 1);
                }
                if ((seq.charAt(lo) | 32) == 's'
                        && (seq.charAt(lo + 1) | 32) == 'u'
                        && (seq.charAt(lo + 2) | 32) == 'n'
                        && (seq.charAt(lo + 3) | 32) == 'd'
                        && (seq.charAt(lo + 4) | 32) == 'a'
                        && (seq.charAt(lo + 5) | 32) == 'y') {
                    return 1 << (SUNDAY - 1);
                }
                break;
            case 7:
                // Tuesday
                if ((seq.charAt(lo) | 32) == 't'
                        && (seq.charAt(lo + 1) | 32) == 'u'
                        && (seq.charAt(lo + 2) | 32) == 'e'
                        && (seq.charAt(lo + 3) | 32) == 's'
                        && (seq.charAt(lo + 4) | 32) == 'd'
                        && (seq.charAt(lo + 5) | 32) == 'a'
                        && (seq.charAt(lo + 6) | 32) == 'y') {
                    return 1 << (TUESDAY - 1);
                }
                break;
            case 8:
                // Thursday, Saturday
                if ((seq.charAt(lo) | 32) == 't'
                        && (seq.charAt(lo + 1) | 32) == 'h'
                        && (seq.charAt(lo + 2) | 32) == 'u'
                        && (seq.charAt(lo + 3) | 32) == 'r'
                        && (seq.charAt(lo + 4) | 32) == 's'
                        && (seq.charAt(lo + 5) | 32) == 'd'
                        && (seq.charAt(lo + 6) | 32) == 'a'
                        && (seq.charAt(lo + 7) | 32) == 'y') {
                    return 1 << (THURSDAY - 1);
                }
                if ((seq.charAt(lo) | 32) == 's'
                        && (seq.charAt(lo + 1) | 32) == 'a'
                        && (seq.charAt(lo + 2) | 32) == 't'
                        && (seq.charAt(lo + 3) | 32) == 'u'
                        && (seq.charAt(lo + 4) | 32) == 'r'
                        && (seq.charAt(lo + 5) | 32) == 'd'
                        && (seq.charAt(lo + 6) | 32) == 'a'
                        && (seq.charAt(lo + 7) | 32) == 'y') {
                    return 1 << (SATURDAY - 1);
                }
                break;
            case 9:
                // Wednesday
                if ((seq.charAt(lo) | 32) == 'w'
                        && (seq.charAt(lo + 1) | 32) == 'e'
                        && (seq.charAt(lo + 2) | 32) == 'd'
                        && (seq.charAt(lo + 3) | 32) == 'n'
                        && (seq.charAt(lo + 4) | 32) == 'e'
                        && (seq.charAt(lo + 5) | 32) == 's'
                        && (seq.charAt(lo + 6) | 32) == 'd'
                        && (seq.charAt(lo + 7) | 32) == 'a'
                        && (seq.charAt(lo + 8) | 32) == 'y') {
                    return 1 << (WEDNESDAY - 1);
                }
                break;
        }
        throw SqlException.$(position, "Invalid day name: ").put(seq, lo, hi);
    }

    static int append(LongList list, int writePoint, long lo, long hi) {
        if (writePoint > 0) {
            long prevHi = list.getQuick(2 * writePoint - 1) + 1;
            if (prevHi >= lo) {
                list.setQuick(2 * writePoint - 1, hi);
                return writePoint;
            }
        }

        list.extendAndSet(2 * writePoint + 1, hi);
        list.setQuick(2 * writePoint, lo);
        return writePoint + 1;
    }

    /**
     * Applies tick calendar filter by intersecting query intervals with trading schedule.
     * The trading schedule is obtained from {@link TickCalendarService} and contains
     * [lo, hi] pairs representing trading sessions in microseconds.
     *
     * @param timestampDriver the timestamp driver for unit conversion
     * @param schedule        the trading schedule as [lo, hi] pairs (in microseconds)
     * @param out             the interval list to filter
     * @param startIndex      index to start filtering from
     */
    static void applyTickCalendarFilter(
            TimestampDriver timestampDriver,
            LongList schedule,
            LongList out,
            int startIndex
    ) {
        int queryIntervalCount = (out.size() - startIndex) / 2;
        if (queryIntervalCount == 0 || schedule.size() == 0) {
            return;
        }
        assert schedule.size() % 2 == 0 : "odd element count in schedule";

        // Sort query intervals by lo value before intersection (intersectInPlace requires sorted input)
        LongGroupSort.quickSort(2, out, startIndex / 2, startIndex / 2 + queryIntervalCount);

        // Determine the query time range to narrow the schedule scan.
        // Query intervals are sorted by lo at this point.
        long queryLo = out.getQuick(startIndex);
        long queryHi = out.getQuick(out.size() - 1);

        // Find the sub-range of the schedule that overlaps with [queryLo, queryHi].
        // Schedule is sorted chronologically as [lo0, hi0, lo1, hi1, ...] in microseconds.
        // We need the first interval whose hi >= queryLo (after conversion) and
        // the last interval whose lo <= queryHi (after conversion).
        boolean isNanos = timestampDriver == NanosTimestampDriver.INSTANCE;
        // Convert query bounds to microseconds for comparison with the schedule.
        // queryLo: round down (floor) so we don't miss an overlapping schedule interval.
        // queryHi: round up (ceil) so we don't miss a schedule interval that starts
        //          within the last sub-microsecond of the query range.
        long queryLoMicros = isNanos ? (queryLo / 1000L) : queryLo;
        long queryHiMicros = isNanos ? (queryHi / 1000L + (queryHi % 1000L > 0 ? 1 : 0)) : queryHi;

        int scheduleIntervalCount = schedule.size() / 2;

        // Binary search for first schedule interval with hi >= queryLoMicros
        int lo = 0;
        int hi = scheduleIntervalCount;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (schedule.getQuick(mid * 2 + 1) < queryLoMicros) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        int schedStart = lo * 2;

        // Binary search for last schedule interval with lo <= queryHiMicros
        lo = schedStart / 2;
        hi = scheduleIntervalCount;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (schedule.getQuick(mid * 2) <= queryHiMicros) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        int schedEnd = lo * 2; // exclusive

        if (schedStart >= schedEnd) {
            // No overlap - remove all query intervals
            out.setPos(startIndex);
            return;
        }

        // Append the overlapping portion of the trading schedule.
        // Schedule intervals are [lo, hi] inclusive, matching the IntervalUtils convention.
        // Convert microseconds to nanoseconds if needed: hi bounds get +999 to cover
        // the full sub-microsecond range of the last inclusive microsecond.
        int dividerIndex = out.size();
        for (int i = schedStart; i < schedEnd; i++) {
            long ts = schedule.getQuick(i);
            if (isNanos) {
                ts = ts * 1000L;
                if ((i & 1) == 1) {
                    ts += 999L;
                }
            }
            out.add(ts);
        }

        // Intersect in place: query intervals [startIndex, dividerIndex) with schedule [dividerIndex, end)
        // But intersectInPlace expects intervals from index 0, so we need to handle the offset
        if (startIndex == 0) {
            intersectInPlace(out, dividerIndex);
        } else {
            // Use thread-local temporary list for the intersection to avoid heap allocation
            LongList temp = tlExchangeFilterTemp.get();
            temp.clear();
            for (int i = startIndex; i < dividerIndex; i++) {
                temp.add(out.getQuick(i));
            }
            int tempDivider = temp.size();
            for (int i = dividerIndex, n = out.size(); i < n; i++) {
                temp.add(out.getQuick(i));
            }
            intersectInPlace(temp, tempDivider);

            // Copy result back
            out.setPos(startIndex);
            for (int i = 0; i < temp.size(); i++) {
                out.add(temp.getQuick(i));
            }
        }
    }

    static void replaceHiLoInterval(long lo, long hi, short operation, LongList out) {
        replaceHiLoInterval(lo, hi, 0, (char) 0, 1, operation, out);
    }

    /**
     * Unions intervals in the range [startIndex, end) with each other.
     * Pre-existing intervals at [0, startIndex) are preserved unchanged.
     * Bracket values may be in any order (e.g., [20,10,15]), so we sort first.
     * This operates in-place without allocations.
     */
    static void unionBracketExpandedIntervals(LongList out, int startIndex) {
        int bracketCount = (out.size() - startIndex) / 2;
        // Note: caller guarantees bracketCount >= 2 via the guard: out.size() > outSize + 2

        // Sort intervals in-place by lo value
        LongGroupSort.quickSort(2, out, startIndex / 2, startIndex / 2 + bracketCount);

        // Merge overlapping intervals in-place (single linear pass since sorted)
        int writeIdx = startIndex + 2;  // First interval is already in place
        for (int readIdx = startIndex + 2; readIdx < out.size(); readIdx += 2) {
            long lo = out.getQuick(readIdx);
            long hi = out.getQuick(readIdx + 1);
            long prevHi = out.getQuick(writeIdx - 1);

            if (lo <= prevHi + 1) {
                // Overlapping or adjacent to previous interval - extend it
                out.setQuick(writeIdx - 1, Math.max(hi, prevHi));
            } else {
                // Non-overlapping - write new interval
                out.setQuick(writeIdx, lo);
                out.setQuick(writeIdx + 1, hi);
                writeIdx += 2;
            }
        }

        out.setPos(writeIdx);
    }
}
