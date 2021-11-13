/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;

import static io.questdb.cairo.TableUtils.DEFAULT_PARTITION_NAME;

/**
 * Setting partition type on JournalKey to override default settings.
 */
public final class PartitionBy {

    public static final int DAY = 0;
    public static final int MONTH = 1;
    public static final int YEAR = 2;
    public static final int HOUR = 4;
    /**
     * Data is not partitioned at all,
     * all data is stored in a single directory
     */
    public static final int NONE = 3;
    private final static LowerCaseCharSequenceIntHashMap nameToIndexMap = new LowerCaseCharSequenceIntHashMap();
    private static final DateFormat fmtDay;
    private static final DateFormat fmtMonth;
    private static final DateFormat fmtYear;
    private final static DateFormat fmtDefault;
    private final static DateFormat fmtHour;

    private PartitionBy() {
    }

    public static boolean isPartitioned(int partitionBy) {
        return partitionBy != NONE;
    }

    public static int fromString(CharSequence name) {
        return nameToIndexMap.get(name);
    }

    public static Timestamps.TimestampAddMethod getPartitionAdd(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return Timestamps.ADD_DD;
            case MONTH:
                return Timestamps.ADD_MM;
            case YEAR:
                return Timestamps.ADD_YYYY;
            case HOUR:
                return Timestamps.ADD_HH;
            default:
                return null;
        }
    }

    public static DateFormat getPartitionDateFmt(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return fmtDay;
            case MONTH:
                return fmtMonth;
            case YEAR:
                return fmtYear;
            case HOUR:
                return fmtHour;
            case NONE:
                return fmtDefault;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
        }
    }

    public static Timestamps.TimestampFloorMethod getPartitionFloor(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return Timestamps.FLOOR_DD;
            case MONTH:
                return Timestamps.FLOOR_MM;
            case YEAR:
                return Timestamps.FLOOR_YYYY;
            case HOUR:
                return Timestamps.FLOOR_HH;
            default:
                return null;
        }
    }

    public static long parsePartitionDirName(CharSequence partitionName, int partitionBy) {
        try {
            if (partitionBy != NONE) {
                return getPartitionDateFmt(partitionBy).parse(partitionName, null);
            } else {
                throw CairoException.instance(0).put("table is not partitioned");
            }
        } catch (NumericException e) {
            final CairoException ee = CairoException.instance(0);
            switch (partitionBy) {
                case DAY:
                    ee.put("'YYYY-MM-DD'");
                    break;
                case MONTH:
                    ee.put("'YYYY-MM'");
                    break;
                default:
                    ee.put("'YYYY'");
                    break;
            }
            ee.put(" expected");
            throw ee;
        }
    }

    public static DateFormat selectPartitionDirFmt(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return fmtDay;
            case MONTH:
                return fmtMonth;
            case YEAR:
                return fmtYear;
            case HOUR:
                return fmtHour;
            default:
                return null;
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
                TimestampFormatUtils.append000(path, y);
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
                TimestampFormatUtils.append000(path, y);
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
                TimestampFormatUtils.append000(path, y);
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
                TimestampFormatUtils.append000(path, y);
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
            case NONE:
                return "NONE";
            default:
                return "UNKNOWN";
        }
    }

    static Timestamps.TimestampCeilMethod getPartitionCeil(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return Timestamps.CEIL_DD;
            case MONTH:
                return Timestamps.CEIL_MM;
            case YEAR:
                return Timestamps.CEIL_YYYY;
            case HOUR:
                return Timestamps.CEIL_HH;
            default:
                return null;
        }
    }

    static boolean isSamePartition(long timestampA, long timestampB, int partitionBy) {
        switch (partitionBy) {
            case NONE:
                return true;
            case DAY:
                return Timestamps.floorDD(timestampA) == Timestamps.floorDD(timestampB);
            case MONTH:
                return Timestamps.floorMM(timestampA) == Timestamps.floorMM(timestampB);
            case YEAR:
                return Timestamps.floorYYYY(timestampA) == Timestamps.floorYYYY(timestampB);
            case HOUR:
                return Timestamps.floorHH(timestampA) == Timestamps.floorHH(timestampB);
            default:
                throw CairoException.instance(0).put("Cannot compare timestamps for unsupported partition type: [").put(partitionBy).put(']');
        }
    }

    static {
        nameToIndexMap.put("day", DAY);
        nameToIndexMap.put("month", MONTH);
        nameToIndexMap.put("year", YEAR);
        nameToIndexMap.put("hour", HOUR);
        nameToIndexMap.put("none", NONE);
    }

    static {
        TimestampFormatCompiler compiler = new TimestampFormatCompiler();
        fmtDay = compiler.compile("yyyy-MM-dd");
        fmtMonth = compiler.compile("yyyy-MM");
        fmtYear = compiler.compile("yyyy");
        fmtHour = compiler.compile("yyyy-MM-ddTHH");
        fmtDefault = new DateFormat() {
            @Override
            public void format(long datetime, DateLocale locale, CharSequence timeZoneName, CharSink sink) {
                sink.put(DEFAULT_PARTITION_NAME);
            }

            @Override
            public long parse(CharSequence in, DateLocale locale) {
                return 0;
            }

            @Override
            public long parse(CharSequence in, int lo, int hi, DateLocale locale) {
                return 0;
            }
        };
    }
}
