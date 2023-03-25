/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.TableUtils.DEFAULT_PARTITION_NAME;
import static io.questdb.std.datetime.microtime.TimestampFormatUtils.*;

/**
 * Collection of static assets to provide time partitioning API. It should be
 * possible to express any time partitioning strategy via this API alone.
 * The rest of QuestDB should just pick it up natively.
 */
public final class PartitionBy {

    public static final int DAY = 0;
    public static final int HOUR = 4;
    public static final int MONTH = 1;
    /**
     * Data is not partitioned at all,
     * all data is stored in a single directory
     */
    public static final int NONE = 3;
    public static final int WEEK = 5;
    public static final int YEAR = 2;
    private static final PartitionAddMethod ADD_DD = Timestamps::addDays;
    private static final PartitionAddMethod ADD_HH = Timestamps::addHours;
    private static final PartitionAddMethod ADD_MM = Timestamps::addMonths;
    private static final PartitionAddMethod ADD_WW = Timestamps::addWeeks;
    private static final PartitionAddMethod ADD_YYYY = Timestamps::addYear;
    private static final PartitionCeilMethod CEIL_DD = Timestamps::ceilDD;
    private static final PartitionCeilMethod CEIL_HH = Timestamps::ceilHH;
    private static final PartitionCeilMethod CEIL_MM = Timestamps::ceilMM;
    private static final PartitionCeilMethod CEIL_WW = Timestamps::ceilWW;
    private static final PartitionCeilMethod CEIL_YYYY = Timestamps::ceilYYYY;
    private final static DateFormat DEFAULT_FORMAT = new DateFormat() {
        @Override
        public void format(long datetime, DateLocale locale, CharSequence timeZoneName, CharSink sink) {
            sink.put(DEFAULT_PARTITION_NAME);
        }

        @Override
        public long parse(CharSequence in, DateLocale locale) {
            return parse(in, 0, in.length(), locale);
        }

        @Override
        public long parse(CharSequence in, int lo, int hi, DateLocale locale) {
            return 0;
        }
    };
    private static final PartitionFloorMethod FLOOR_DD = Timestamps::floorDD;
    private static final PartitionFloorMethod FLOOR_HH = Timestamps::floorHH;
    private static final PartitionFloorMethod FLOOR_MM = Timestamps::floorMM;
    private static final PartitionFloorMethod FLOOR_WW = Timestamps::floorWW;
    private static final PartitionFloorMethod FLOOR_YYYY = Timestamps::floorYYYY;
    private final static LowerCaseCharSequenceIntHashMap nameToIndexMap = new LowerCaseCharSequenceIntHashMap();

    private PartitionBy() {
    }

    public static int fromString(CharSequence name) {
        return nameToIndexMap.get(name);
    }

    public static PartitionAddMethod getPartitionAddMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return ADD_DD;
            case MONTH:
                return ADD_MM;
            case YEAR:
                return ADD_YYYY;
            case HOUR:
                return ADD_HH;
            case WEEK:
                return ADD_WW;
            default:
                return null;
        }
    }

    public static PartitionCeilMethod getPartitionCeilMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return CEIL_DD;
            case MONTH:
                return CEIL_MM;
            case YEAR:
                return CEIL_YYYY;
            case HOUR:
                return CEIL_HH;
            case WEEK:
                return CEIL_WW;
            default:
                return null;
        }
    }

    public static DateFormat getPartitionDirFormatMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return DAY_FORMAT;
            case MONTH:
                return MONTH_FORMAT;
            case YEAR:
                return YEAR_FORMAT;
            case HOUR:
                return HOUR_FORMAT;
            case WEEK:
                return WEEK_FORMAT;
            case NONE:
                return DEFAULT_FORMAT;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
        }
    }

    public static PartitionFloorMethod getPartitionFloorMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return FLOOR_DD;
            case WEEK:
                return FLOOR_WW;
            case MONTH:
                return FLOOR_MM;
            case YEAR:
                return FLOOR_YYYY;
            case HOUR:
                return FLOOR_HH;
            default:
                return null;
        }
    }

    public static long getPartitionTimeIntervalFloor(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return Timestamps.DAY_MICROS;
            case WEEK:
                return Timestamps.DAY_MICROS * 7;
            case MONTH:
                return Timestamps.DAY_MICROS * 28;
            case YEAR:
                return Timestamps.DAY_MICROS * 365;
            case HOUR:
                return Timestamps.HOUR_MICROS;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static boolean isPartitioned(int partitionBy) {
        return partitionBy != NONE;
    }

    public static long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy) {
        CharSequence fmtStr;
        int limit = -1;
        try {
            DateFormat fmtMethod;
            switch (partitionBy) {
                case DAY:
                    fmtMethod = DAY_FORMAT;
                    fmtStr = DAY_PATTERN;
                    break;
                case MONTH:
                    fmtMethod = MONTH_FORMAT;
                    fmtStr = MONTH_PATTERN;
                    break;
                case YEAR:
                    fmtMethod = YEAR_FORMAT;
                    fmtStr = YEAR_PATTERN;
                    break;
                case HOUR:
                    fmtMethod = HOUR_FORMAT;
                    fmtStr = HOUR_PATTERN;
                    break;
                case WEEK:
                    fmtMethod = WEEK_FORMAT;
                    fmtStr = WEEK_PATTERN;
                    break;
                case NONE:
                    fmtMethod = DEFAULT_FORMAT;
                    fmtStr = partitionName;
                    break;
                default:
                    throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
            }
            limit = fmtStr.length();
            if (partitionName.length() < limit) {
                throw expectedPartitionDirNameFormatCairoException(partitionName, partitionName.length(), partitionBy);
            }
            return fmtMethod.parse(partitionName, 0, limit, null);
        } catch (NumericException e) {
            if (partitionBy == PartitionBy.WEEK) {
                // maybe the user used a timestamp, or a date, string.
                int localLimit = DAY_PATTERN.length();
                try {
                    // trim to lowest precision needed and get the timestamp
                    // convert timestamp to first day of the week
                    return Timestamps.floorDOW(DAY_FORMAT.parse(partitionName, 0, localLimit, null));
                } catch (NumericException ignore) {
                    throw expectedPartitionDirNameFormatCairoException(partitionName, Math.min(partitionName.length(), localLimit), partitionBy);
                }
            }
            throw expectedPartitionDirNameFormatCairoException(partitionName, limit, partitionBy);
        }
    }

    public static long setSinkForPartition(CharSink path, int partitionBy, long timestamp, boolean calculatePartitionMax) {
        int y, m, d;
        boolean leap;
        switch (partitionBy) {
            case DAY:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                d = Timestamps.getDayOfMonth(timestamp, y, m, leap);
                TimestampFormatUtils.appendYear000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);
                path.put('-');
                TimestampFormatUtils.append0(path, d);

                if (calculatePartitionMax) {
                    return Timestamps.yearMicros(y, leap)
                            + Timestamps.monthOfYearMicros(m, leap)
                            + (d - 1) * Timestamps.DAY_MICROS + 24 * Timestamps.HOUR_MICROS - 1;
                }
                return 0;
            case MONTH:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                TimestampFormatUtils.appendYear000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);

                if (calculatePartitionMax) {
                    return Timestamps.yearMicros(y, leap)
                            + Timestamps.monthOfYearMicros(m, leap)
                            + Timestamps.getDaysPerMonth(m, leap) * 24L * Timestamps.HOUR_MICROS - 1;
                }
                return 0;
            case YEAR:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                TimestampFormatUtils.appendYear000(path, y);
                if (calculatePartitionMax) {
                    return Timestamps.addYear(Timestamps.yearMicros(y, leap), 1) - 1;
                }
                return 0;
            case HOUR:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                d = Timestamps.getDayOfMonth(timestamp, y, m, leap);
                int h = Timestamps.getHourOfDay(timestamp);
                TimestampFormatUtils.appendYear000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);
                path.put('-');
                TimestampFormatUtils.append0(path, d);
                path.put('T');
                TimestampFormatUtils.append0(path, h);

                if (calculatePartitionMax) {
                    return Timestamps.yearMicros(y, leap)
                            + Timestamps.monthOfYearMicros(m, leap)
                            + (d - 1) * Timestamps.DAY_MICROS + (h + 1) * Timestamps.HOUR_MICROS - 1;
                }
                return 0;
            case WEEK:
                y = Timestamps.getIsoYear(timestamp);
                int w = Timestamps.getWeek(timestamp);
                TimestampFormatUtils.appendYear000(path, y);
                path.put("-W");
                TimestampFormatUtils.append0(path, w);

                if (calculatePartitionMax) {
                    return Timestamps.ceilWW(timestamp) - 1;
                }
                return 0;
            default:
                path.put(DEFAULT_PARTITION_NAME);
                return Long.MAX_VALUE;
        }
    }

    public static String toString(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return "DAY";
            case MONTH:
                return "MONTH";
            case YEAR:
                return "YEAR";
            case HOUR:
                return "HOUR";
            case WEEK:
                return "WEEK";
            case NONE:
                return "NONE";
            default:
                return "UNKNOWN";
        }
    }

    private static CairoException expectedPartitionDirNameFormatCairoException(CharSequence partitionName, int limit, int partitionBy) {
        final CairoException ee = CairoException.critical(0).put('\'');
        switch (partitionBy) {
            case DAY:
                ee.put(DAY_PATTERN);
                break;
            case WEEK:
                ee.put(WEEK_PATTERN).put("' or '").put(DAY_PATTERN);
                break;
            case MONTH:
                ee.put(MONTH_PATTERN);
                break;
            case YEAR:
                ee.put(YEAR_PATTERN);
                break;
            case HOUR:
                ee.put(HOUR_PATTERN);
                break;
        }
        ee.put("' expected, found [ts=").put(partitionName.subSequence(0, limit)).put(']');
        return ee;
    }

    @FunctionalInterface
    public interface PartitionAddMethod {
        long calculate(long timestamp, int increment);
    }

    @FunctionalInterface
    public interface PartitionCeilMethod {
        // returns exclusive ceiling for the give timestamp
        long ceil(long timestamp);
    }

    @FunctionalInterface
    public interface PartitionFloorMethod {
        long floor(long timestamp);
    }

    static {
        nameToIndexMap.put("day", DAY);
        nameToIndexMap.put("month", MONTH);
        nameToIndexMap.put("year", YEAR);
        nameToIndexMap.put("hour", HOUR);
        nameToIndexMap.put("week", WEEK);
        nameToIndexMap.put("none", NONE);
    }
}
