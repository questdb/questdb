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

package io.questdb.std.datetime;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.SqlException;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Utf8Sequence;

public class CommonUtils {
    public static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    public static final int DAY_HOURS = 24;
    public static final String DAY_PATTERN = "yyyy-MM-dd";
    public static final String GREEDY_MILLIS1_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.Sz";
    public static final String GREEDY_MILLIS2_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSz";
    public static final int HOUR_24 = 2;
    public static final int HOUR_AM = 0;
    public static final int HOUR_MINUTES = 60;
    public static final String HOUR_PATTERN = "yyyy-MM-ddTHH";
    public static final int HOUR_PM = 1;
    // "2261-12-31 23:59:59.999999999" for nano timestamp
    public static final long MAX_TIMESTAMP = 9214646399999999999L;
    public static final long MINUTE_SECONDS = 60;
    public static final String MONTH_PATTERN = "yyyy-MM";
    public static final String NSEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUNNNz";
    public static final String PG_TIMESTAMP_MILLI_TIME_Z_PATTERN = "y-MM-dd HH:mm:ss.SSSz";
    public static final String SEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ssz";
    public static final byte TIMESTAMP_UNIT_NANOS = 1;
    public static final byte TIMESTAMP_UNIT_MICROS = TIMESTAMP_UNIT_NANOS + 1;
    public static final byte TIMESTAMP_UNIT_MILLIS = TIMESTAMP_UNIT_MICROS + 1;
    public static final byte TIMESTAMP_UNIT_SECONDS = TIMESTAMP_UNIT_MILLIS + 1;
    public static final byte TIMESTAMP_UNIT_MINUTES = TIMESTAMP_UNIT_SECONDS + 1;
    public static final byte TIMESTAMP_UNIT_HOURS = TIMESTAMP_UNIT_MINUTES + 1;
    public static final byte TIMESTAMP_UNIT_UNSET = 0;
    public static final String USEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUz";
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    public static final String WEEK_PATTERN = "YYYY-Www";
    public static final int YEAR_MONTHS = 12;
    public static final String YEAR_PATTERN = "yyyy";

    public static void assembleCheckTail(
            int assertNoTailIndex,
            BytecodeAssembler asm,
            int localPos,
            int pHi,
            int pLocale,
            int localEra,
            int localYear,
            int localMonth,
            int localDay,
            int localHour,
            int localMinute,
            int localSecond,
            int localMillis,
            int localTimezone
    ) {
        asm.iload(localPos);
        asm.iload(pHi);
        asm.invokeStatic(assertNoTailIndex);
        asm.aload(pLocale);
        asm.iload(localEra);
        asm.iload(localYear);
        asm.iload(localMonth);
        asm.iload(localDay);
        asm.iload(localHour);
        asm.iload(localMinute);
        asm.iload(localSecond);
        asm.iload(localMillis);
        asm.iload(localTimezone);
    }

    public static void assembleDefault(
            int assertStringIndex,
            int assertCharIndex,
            IntList delimIndices,
            int op,
            ObjList<String> delimiters, BytecodeAssembler asm,
            int pInputStr,
            int localPos,
            int pHi
    ) {
        String delimiter = delimiters.getQuick(-op - 1);
        int len = delimiter.length();
        if (len == 1) {
            asm.iconst(delimiter.charAt(0));
            asm.aload(pInputStr);
            asm.iload(localPos);
            asm.iinc(localPos, 1);
            asm.iload(pHi);
            asm.invokeStatic(assertCharIndex);
        } else {
            asm.ldc(delimIndices.getQuick(-op - 1));
            asm.iconst(len);
            asm.aload(pInputStr);
            asm.iload(localPos);
            asm.iload(pHi);
            asm.invokeStatic(assertStringIndex);
            asm.istore(localPos);
        }
    }

    public static void assembleTimeZone(ObjList<String> delimiters, int sinkPutStrIndex, int sinkPutChrIndex, int op, BytecodeAssembler asm, int faLocalSink, IntList delimiterIndexes) {
        if (op < 0) {
            String delimiter = delimiters.getQuick(-op - 1);
            if (delimiter.length() > 1) {
                asm.aload(faLocalSink);
                asm.ldc(delimiterIndexes.getQuick(-op - 1));
                asm.invokeInterface(sinkPutStrIndex, 1);
            } else {
                asm.aload(faLocalSink);
                asm.iconst(delimiter.charAt(0));
                asm.invokeInterface(sinkPutChrIndex, 1);
            }
            asm.pop();
        }
    }

    public static int assembleYear(
            int assertRemainingIndex,
            int parseIntIndex,
            int charAtIndex,
            int stackState,
            BytecodeAssembler asm,
            int localPos,
            int pHi,
            int pInputStr,
            int localYear,
            LongList frameOffsets
    ) {
        asm.iload(localPos);
        asm.iload(pHi);
        int b1 = asm.if_icmpge();
        asm.aload(pInputStr);
        asm.iload(localPos);
        asm.invokeInterface(charAtIndex, 1); //charAt
        asm.iconst('-');
        int b2 = asm.if_icmpne();
        asm.iload(localPos);
        asm.iconst(4);
        asm.iadd();
        asm.iload(pHi);
        asm.invokeStatic(assertRemainingIndex);
        asm.aload(pInputStr);
        asm.iload(localPos);
        asm.iconst(1);
        asm.iadd();
        asm.iinc(localPos, 5);
        asm.iload(localPos);
        asm.invokeStatic(parseIntIndex);
        asm.ineg();
        asm.istore(localYear);
        int b3 = asm.goto_();

        int p = asm.position();
        frameOffsets.add(Numbers.encodeLowHighInts(stackState, p));
        asm.setJmp(b1, p);
        asm.setJmp(b2, p);

        asm.iload(localPos);
        asm.iconst(3);
        asm.iadd();
        asm.iload(pHi);
        asm.invokeStatic(assertRemainingIndex);

        asm.aload(pInputStr);
        asm.iload(localPos);
        asm.iinc(localPos, 4);
        asm.iload(localPos);
        asm.invokeStatic(parseIntIndex);
        asm.istore(localYear);

        stackState &= ~(1 << localYear);

        p = asm.position();
        frameOffsets.add(Numbers.encodeLowHighInts(stackState, p));
        asm.setJmp(b3, p);
        return stackState;
    }

    public static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.instance();
        }
    }

    public static void checkChar(Utf8Sequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.byteAt(p) != c) {
            throw NumericException.instance();
        }
    }

    public static boolean checkLen2(int p, int lim) throws NumericException {
        if (lim - p >= 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static boolean checkLen3(int p, int lim) throws NumericException {
        if (lim - p > 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static boolean checkLenStrict(int p, int lim) throws NumericException {
        if (lim - p == 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static void checkRange(int x, int min, int max) throws NumericException {
        if (x < min || x > max) {
            throw NumericException.instance();
        }
    }

    public static void checkSpecialChar(CharSequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.charAt(p) != 'T' && s.charAt(p) != ' ')) {
            throw NumericException.instance();
        }
    }

    public static void checkSpecialChar(Utf8Sequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.byteAt(p) != 'T' && s.byteAt(p) != ' ')) {
            throw NumericException.instance();
        }
    }

    /**
     * Days in a given month. This method expects you to know if month is in leap year.
     *
     * @param m    month from 1 to 12
     * @param leap true if this is for leap year
     * @return number of days in month.
     */
    public static int getDaysPerMonth(int m, boolean leap) {
        return leap & m == 2 ? 29 : DAYS_PER_MONTH[m - 1];
    }

    /**
     * Returns the timezone offset to use when converting a UTC timestamp to
     * local time for floor/round bucket computations. Sub-day units use the
     * standard (non-DST) offset to keep bucket widths uniform across DST
     * transitions. Super-day units use the actual offset at the given instant.
     * <p>
     * Both {@code timestamp_floor_utc} (SAMPLE BY query path) and the mat view
     * refresh iterator MUST call this method to ensure bucket boundaries agree.
     * <p>
     * Typical usage (zero-allocation, no closures):
     * <pre>
     *   long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, utcTs, unit);
     *   long local = utcTs + tzOff;
     *   long floored = ... // caller's own floor/round
     *   long utcResult = CommonUtils.offsetFlooredUtcResult(floored, tzOff, offset, tzRules, unit);
     * </pre>
     */
    public static long getFloorUtcTzOffset(TimeZoneRules tzRules, long utcTimestamp, char unit) {
        return isSubDayUnit(unit) ? tzRules.getStandardOffset() : tzRules.getOffset(utcTimestamp);
    }

    /**
     * Since ISO weeks don't always start on the first day of the year, there is an offset of days from the 1st day of the year.
     *
     * @param year of timestamp
     * @return difference in the days from the start of the year (January 1st) and the first ISO week
     */
    public static int getIsoYearDayOffset(int year) {
        int dayOfTheWeekOfEndOfPreviousYear = Micros.getDayOfTheWeekOfEndOfYear(year - 1);
        return ((dayOfTheWeekOfEndOfPreviousYear <= 3) ? 0 : 7) - dayOfTheWeekOfEndOfPreviousYear;
    }

    public static int getStrideMultiple(CharSequence str, int position) throws SqlException {
        if (str != null) {
            if (Chars.equals(str, '-')) {
                throw SqlException.position(position).put("positive number expected: ").put(str);
            }
            if (str.length() > 1) {
                try {
                    final int multiple = Numbers.parseInt(str, 0, str.length() - 1);
                    if (multiple <= 0) {
                        throw SqlException.position(position).put("positive number expected: ").put(str);
                    }
                    return multiple;
                } catch (NumericException ignored) {
                }
            }
        }
        return 1;
    }

    public static char getStrideUnit(CharSequence str, int position) throws SqlException {
        assert !str.isEmpty();
        final char unit = str.charAt(str.length() - 1);
        return switch (unit) {
            case 'M', 'y', 'w', 'd', 'h', 'm', 's', 'T', 'U', 'n' -> unit;
            default -> throw SqlException.position(position).put("Invalid unit: ").put(str);
        };
    }

    public static int getWeeks(int y) {
        if (Micros.getDayOfTheWeekOfEndOfYear(y) == 4 || Micros.getDayOfTheWeekOfEndOfYear(y - 1) == 3) {
            return 53;
        }
        return 52;
    }

    /**
     * Calculates if year is leap year using following algorithm:
     * <p>
     * <a href="http://en.wikipedia.org/wiki/Leap_year">...</a>
     *
     * @param year the year
     * @return true if year is leap
     */
    public static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    public static boolean isSubDayUnit(char unit) {
        return switch (unit) {
            case 'h', 'm', 's', 'T', 'U', 'n' -> true;
            default -> false;
        };
    }

    public static long microsToNanos(long micros) {
        try {
            return micros == Numbers.LONG_NULL ? Numbers.LONG_NULL : Math.multiplyExact(micros, Micros.MICRO_NANOS);
        } catch (ArithmeticException e) {
            throw ImplicitCastException.inconvertibleValue(micros, ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO);
        }
    }

    public static long nanosToMicros(long nanos) {
        return nanos == Numbers.LONG_NULL ? Numbers.LONG_NULL : nanos / 1000L;
    }

    /**
     * Converts a floored local timestamp back to UTC and applies the user
     * offset. Must be paired with {@link #getFloorUtcTzOffset} — the
     * {@code tzOff} parameter must come from that method for the same
     * input timestamp. Pass {@code offset = 0} when the caller's floor
     * operation already incorporates the offset (e.g. TimestampSampler).
     */
    public static long offsetFlooredUtcResult(long flooredLocal, long tzOff, long offset, TimeZoneRules tzRules, char unit) {
        if (isSubDayUnit(unit)) {
            return flooredLocal - tzOff + offset;
        }
        final long resultTzOff = tzRules.getOffset(flooredLocal - tzOff);
        return flooredLocal - resultTzOff + offset;
    }

    public static void parseDigits(int assertRemainingIndex, int parseIntIndex, int digitCount, BytecodeAssembler asm, int localPos, int pHi, int pInputStr) {
        asm.iload(localPos);
        if (digitCount > 1) {
            asm.iconst(digitCount - 1);
            asm.iadd();
        }
        asm.iload(pHi);
        asm.invokeStatic(assertRemainingIndex);

        asm.aload(pInputStr);
        asm.iload(localPos);
        asm.iinc(localPos, digitCount);
        asm.iload(localPos);
        asm.invokeStatic(parseIntIndex);
    }

    public static int parseSign(char c) throws NumericException {
        return switch (c) {
            case '+' -> -1;
            case '-' -> 1;
            default -> throw NumericException.instance();
        };
    }

    public static int tenPow(int i) throws NumericException {
        return switch (i) {
            case 0 -> 1;
            case 1 -> 10;
            case 2 -> 100;
            case 3 -> 1000;
            case 4 -> 10000;
            case 5 -> 100000;
            case 6 -> 1000000;
            case 7 -> 10000000;
            case 8 -> 100000000;
            default -> throw NumericException.instance();
        };
    }

    /**
     * Returns a duration value in TTL format: if positive, it's in hours; if negative, it's in months (and
     * the actual value is positive)
     *
     * @param value           the number of units, must be a non-negative number
     * @param partitionByUnit the time unit, one of `PartitionBy` constants
     * @param tokenPos        the position of the number token in the SQL string
     * @return the TTL value as described
     * @throws SqlException if the passed value is out of range
     */
    public static int toHoursOrMonths(int value, int partitionByUnit, int tokenPos) throws SqlException {
        if (value < 0) {
            throw new AssertionError("The value must be non-negative");
        }
        if (value == 0) {
            return 0;
        }
        switch (partitionByUnit) {
            case PartitionBy.HOUR:
                return value;
            case PartitionBy.DAY:
                int maxDays = Integer.MAX_VALUE / DAY_HOURS;
                if (value > maxDays) {
                    throw SqlException.$(tokenPos, "value out of range: ")
                            .put(value).put(" days. Max value: ").put(maxDays).put(" days");
                }
                return DAY_HOURS * value;
            case PartitionBy.WEEK:
                int maxWeeks = Integer.MAX_VALUE / Micros.WEEK_DAYS / DAY_HOURS;
                if (value > maxWeeks) {
                    throw SqlException.$(tokenPos, "value out of range: ")
                            .put(value).put(" weeks. Max value: ").put(maxWeeks).put(" weeks");
                }
                return Micros.WEEK_DAYS * DAY_HOURS * value;
            case PartitionBy.MONTH:
                return -value;
            case PartitionBy.YEAR:
                int maxYears = Integer.MAX_VALUE / YEAR_MONTHS;
                if (value > maxYears) {
                    throw SqlException.$(tokenPos, "value out of range: ")
                            .put(value).put(" years. Max value: ").put(maxYears).put(" years");
                }
                return -(YEAR_MONTHS * value);
            default:
                throw new AssertionError("invalid value for partitionByUnit: " + partitionByUnit);
        }
    }

    public static boolean isOptionalFractionStart(CharSequence sequence, int p, int lim) {
        if (p + 1 >= lim || sequence.charAt(p) != '.') {
            return false;
        }
        final char next = sequence.charAt(p + 1);
        return next >= '0' && next <= '9';
    }

    @FunctionalInterface
    public interface TimestampUnitConverter {
        long convert(long timestamp);
    }
}
