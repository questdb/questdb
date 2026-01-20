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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public final class IntervalUtils {
    public static final int HI_INDEX = 1;
    public static final int LO_INDEX = 0;
    public static final int OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX = 2;
    public static final int PERIOD_COUNT_INDEX = 3;
    public static final int STATIC_LONGS_PER_DYNAMIC_INTERVAL = 4;
    /**
     * Maximum recursion depth for bracket expansion (one level per bracket group).
     */
    private static final int MAX_BRACKET_DEPTH = 8;

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
        return Numbers.decodeHighInt(intervals.getQuick(index + PERIOD_COUNT_INDEX));
    }

    public static char decodePeriodType(LongList intervals, int index) {
        int pts = Numbers.decodeHighShort(
                Numbers.decodeLowInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
        return (char) (pts - (int) Short.MIN_VALUE);
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
                out
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
        out.add(
                lo,
                hi,
                Numbers.encodeLowHighInts(
                        Numbers.encodeLowHighShorts(operation, (short) ((int) periodType + Short.MIN_VALUE)),
                        Numbers.encodeLowHighShorts(adjustment, dynamicIndicator)
                ),
                Numbers.encodeLowHighInts(period, periodCount)
        );
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

    public static void parseAndApplyInterval(
            TimestampDriver timestampDriver,
            @Nullable CharSequence seq,
            LongList out,
            int position,
            StringSink sink
    ) throws SqlException {
        if (seq != null) {
            parseBracketInterval(timestampDriver, seq, 0, seq.length(), position, out, IntervalOperation.INTERSECT, sink, true);
        } else {
            encodeInterval(Numbers.LONG_NULL, Numbers.LONG_NULL, IntervalOperation.INTERSECT, out);
            applyLastEncodedInterval(timestampDriver, out);
        }
    }

    /**
     * Parses interval strings with bracket expansion syntax.
     * <p>
     * Brackets allow concise specification of multiple disjoint intervals:
     * <ul>
     *   <li>Comma-separated: {@code 2018-01-[10,15]} → days 10 and 15</li>
     *   <li>Ranges: {@code 2018-01-[10..12]} → days 10, 11, 12</li>
     *   <li>Mixed: {@code 2018-01-[5,10..12,20]} → days 5, 10, 11, 12, 20</li>
     *   <li>Multiple groups (cartesian product): {@code 2018-[01,06]-[10,15]} → 4 intervals</li>
     * </ul>
     * <p>
     * Output format depends on input and {@code applyEncoded} parameter:
     * <ul>
     *   <li>With brackets: always returns simple format (2 longs per interval)</li>
     *   <li>Without brackets + applyEncoded=true: returns simple format</li>
     *   <li>Without brackets + applyEncoded=false: returns encoded format (4 longs)</li>
     * </ul>
     * <p>
     * This implementation uses recursion with depth limiting and does not allocate
     * intermediate objects. The provided StringSink is used as working storage and
     * is restored to its original state on return.
     *
     * @param timestampDriver timestamp driver for parsing
     * @param seq             input interval string
     * @param lo              start index in seq
     * @param lim             end index in seq
     * @param position        position for error reporting
     * @param out             output list for parsed intervals
     * @param operation       interval operation
     * @param sink            reusable StringSink for building expanded strings (will be cleared)
     * @param applyEncoded    if true, converts encoded format to simple format for non-bracket intervals
     * @throws SqlException if the interval format is invalid
     */
    public static void parseBracketInterval(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation,
            StringSink sink,
            boolean applyEncoded
    ) throws SqlException {
        // Single scan: detect brackets and find semicolon position
        int dateLim = lim;
        int depth = 0;
        boolean hasBrackets = false;
        for (int i = lo; i < lim; i++) {
            char c = seq.charAt(i);
            if (c == '[') {
                hasBrackets = true;
                depth++;
            } else if (c == ']') {
                depth--;
            } else if (c == ';' && depth == 0) {
                dateLim = i;
                break;
            }
        }

        if (!hasBrackets) {
            parseInterval0(timestampDriver, seq, lo, lim, position, out, operation);
            if (applyEncoded) {
                applyLastEncodedInterval(timestampDriver, out);
            }
            return;
        }

        int outSize = out.size();
        sink.clear();
        try {
            expandBracketsRecursive(
                    timestampDriver,
                    seq,
                    lo,
                    lo,
                    dateLim,
                    lim,
                    position,
                    out,
                    operation,
                    sink,
                    0,
                    applyEncoded,
                    outSize
            );
        } catch (SqlException e) {
            // Unwind output on error
            out.setPos(outSize);
            throw e;
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

    private static int determinePadWidth(CharSequence seq, int lo, int bracketStart) {
        // Determine field width based on bracket position in timestamp
        // Count separators before bracket to determine which field we're in
        // Format: YYYY-MM-DDTHH:MM:SS.ffffff
        int dashes = 0;
        boolean afterT = false;
        boolean afterDot = false;

        for (int i = lo; i < bracketStart; i++) {
            char c = seq.charAt(i);
            if (c == '-') {
                dashes++;
            } else if (c == 'T' || c == ' ') {
                afterT = true;
            } else if (c == '.') {
                afterDot = true;
            }
        }

        if (afterDot) {
            // Microseconds - no padding (variable width)
            return 0;
        }
        if (afterT) {
            // Time field: hour, minute, or second - 2 digits
            return 2;
        }
        if (dashes == 1) {
            // Month - 2 digits
            return 2;
        }
        if (dashes >= 2) {
            // Day - 2 digits
            return 2;
        }
        // Year - no padding
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
     */
    private static void expandBracketsRecursive(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int intervalStart, // start of interval in seq (for pad width calculation)
            int pos,        // current position in seq (within date part)
            int dateLim,    // end of date part (before semicolon)
            int fullLim,    // end of entire string (including duration suffix)
            int errorPos,
            LongList out,
            short operation,
            StringSink sink,
            int depth,
            boolean applyEncoded,
            int outSizeBeforeExpansion
    ) throws SqlException {
        if (depth > MAX_BRACKET_DEPTH) {
            throw SqlException.$(errorPos, "Too many bracket groups (max " + MAX_BRACKET_DEPTH + ")");
        }

        // Find first bracket starting from pos
        int bracketStart = -1;
        for (int i = pos; i < dateLim; i++) {
            if (seq.charAt(i) == '[') {
                bracketStart = i;
                break;
            }
        }

        if (bracketStart < 0) {
            // No more brackets - append remaining text and parse
            int sinkLen = sink.length();
            sink.put(seq, pos, fullLim);
            parseExpandedInterval(timestampDriver, sink, errorPos, out, operation, applyEncoded, outSizeBeforeExpansion);
            sink.clear(sinkLen);
            return;
        }

        // Save sink state before adding prefix
        int startLen = sink.length();

        // Copy text before bracket
        sink.put(seq, pos, bracketStart);
        int afterPrefixLen = sink.length();

        // Find closing bracket
        int bracketEnd = findMatchingBracket(seq, bracketStart, dateLim, errorPos);

        // Determine zero-padding width based on position in timestamp
        int padWidth = determinePadWidth(seq, intervalStart, bracketStart);

        // Iterate through values in bracket without allocating collections
        int i = bracketStart + 1;
        int valueCount = 0;

        while (i < bracketEnd) {
            // Skip whitespace
            while (i < bracketEnd && Character.isWhitespace(seq.charAt(i))) {
                i++;
            }
            if (i >= bracketEnd) {
                break;
            }

            // Parse number
            int numStart = i;
            while (i < bracketEnd && Character.isDigit(seq.charAt(i))) {
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

            // Skip whitespace
            while (i < bracketEnd && Character.isWhitespace(seq.charAt(i))) {
                i++;
            }

            // Check for range (..)
            int rangeEnd = value;
            if (i + 1 < bracketEnd && seq.charAt(i) == '.' && seq.charAt(i + 1) == '.') {
                i += 2;
                // Skip whitespace
                while (i < bracketEnd && Character.isWhitespace(seq.charAt(i))) {
                    i++;
                }
                // Parse range end
                int endStart = i;
                while (i < bracketEnd && Character.isDigit(seq.charAt(i))) {
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
                    throw SqlException.$(errorPos, "Range must be ascending: " + value + ".." + rangeEnd);
                }
            }

            // Expand range (or single value if value == rangeEnd)
            for (int v = value; v <= rangeEnd; v++) {
                appendPaddedInt(sink, v, padWidth);
                expandBracketsRecursive(
                        timestampDriver, seq, intervalStart, bracketEnd + 1, dateLim, fullLim,
                        errorPos, out, operation, sink, depth + 1, applyEncoded, outSizeBeforeExpansion
                );
                sink.clear(afterPrefixLen);
            }

            valueCount++;

            // Skip whitespace
            while (i < bracketEnd && Character.isWhitespace(seq.charAt(i))) {
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
     * Parses a fully-expanded interval string (no brackets) and adds result to output.
     * In static mode (applyEncoded=true), results are converted to 2-long format and unioned.
     * In dynamic mode (applyEncoded=false), results stay in 4-long format:
     * - For INTERSECT operations: first interval uses INTERSECT, subsequent use UNION
     *   (intervals are combined, then intersected with previous constraints)
     * - For SUBTRACT operations: all intervals use SUBTRACT
     *   (each interval is individually inverted and intersected, achieving NOT A AND NOT B)
     */
    private static void parseExpandedInterval(
            TimestampDriver timestampDriver,
            CharSequence expanded,
            int errorPos,
            LongList out,
            short operation,
            boolean applyEncoded,
            int outSizeBeforeExpansion
    ) throws SqlException {
        if (applyEncoded) {
            // Static mode: convert to 2-long format and union
            int divider = out.size();
            parseInterval0(timestampDriver, expanded, 0, expanded.length(), errorPos, out, operation);
            applyLastEncodedInterval(timestampDriver, out);
            if (divider > 0) {
                unionInPlace(out, divider);
            }
        } else {
            // Dynamic mode: keep 4-long format
            boolean isFirstInterval = out.size() == outSizeBeforeExpansion;
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
            parseInterval0(timestampDriver, expanded, 0, expanded.length(), errorPos, out, effectiveOp);
        }
    }

    private static void parseInterval0(
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
                    throw SqlException.$(position, "Invalid interval format");
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
                        throw SqlException.$(position, "Invalid date");
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
                        throw SqlException.$(position, "Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw SqlException.$(position, "Invalid interval format");
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
        char type = seq.charAt(lim - 1);
        int period;
        try {
            period = Numbers.parseInt(seq, p + 1, lim - 1);
        } catch (NumericException e) {
            throw SqlException.$(position, "Range not a number");
        }
        try {
            int index = out.size();
            timestampDriver.parseInterval(seq, lo, p, operation, out);
            long low = decodeIntervalLo(out, index);
            long hi = decodeIntervalHi(out, index);
            hi = timestampDriver.add(hi, type, period);
            if (hi < low) {
                throw SqlException.invalidDate(position);
            }
            replaceHiLoInterval(low, hi, operation, out);
            return;
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMicros = timestampDriver.parseAnyFormat(seq, lo, p);
            long hiMicros = timestampDriver.add(loMicros, type, period);
            if (hiMicros < loMicros) {
                throw SqlException.invalidDate(position);
            }
            encodeInterval(loMicros, hiMicros, operation, out);
        } catch (NumericException e) {
            throw SqlException.invalidDate(position);
        }
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

    static void replaceHiLoInterval(long lo, long hi, short operation, LongList out) {
        replaceHiLoInterval(lo, hi, 0, (char) 0, 1, operation, out);
    }
}
