/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;

public final class IntervalUtils {
    public static final int HI_INDEX = 1;
    public static final int LO_INDEX = 0;
    public static final int OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX = 2;
    public static final int PERIOD_COUNT_INDEX = 3;
    public static final int STATIC_LONGS_PER_DYNAMIC_INTERVAL = 4;

    public static void applyLastEncodedIntervalEx(int timestampType, LongList intervals) {
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
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
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
        return Numbers.decodeLowShort(Numbers.decodeHighInt(
                intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)));
    }

    public static short getEncodedDynamicIndicator(LongList intervals, int index) {
        return Numbers.decodeHighShort(Numbers.decodeHighInt(
                intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)));
    }

    public static short getEncodedOperation(LongList intervals, int index) {
        return Numbers.decodeLowShort(
                Numbers.decodeLowInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
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
     * Inverts intervals. This method also produces inclusive edges that differ from source ones by 1 milli.
     *
     * @param intervals collection of intervals
     */
    public static void invert(LongList intervals) {
        invert(intervals, 0);
    }

    /**
     * Inverts intervals. This method also produces inclusive edges that differ from source ones by 1 milli.
     *
     * @param intervals collection of intervals
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

    public static boolean isInIntervals(LongList intervals, long timestamp) {
        return findInterval(intervals, timestamp) != -1;
    }

    public static void replaceHiLoInterval(long lo, long hi, int period, char periodType, int periodCount, short operation, LongList out) {
        int lastIndex = out.size() - 4;
        out.setQuick(lastIndex, lo);
        out.setQuick(lastIndex + HI_INDEX, hi);
        out.setQuick(lastIndex + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX, Numbers.encodeLowHighInts(
                Numbers.encodeLowHighShorts(operation, (short) ((int) periodType + Short.MIN_VALUE)),
                0));
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
                    // Intersection with previously safed interval
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
