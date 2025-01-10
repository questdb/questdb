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

package io.questdb.cairo;

import io.questdb.cairo.ptt.IsoDatePartitionFormat;
import io.questdb.cairo.ptt.IsoWeekPartitionFormat;
import io.questdb.griffin.SqlException;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseUtf8SequenceIntHashMap;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
     * Data is not partitioned at all, all data is stored in a single directory
     */
    public static final int NONE = 3;
    public static final int WEEK = 5;
    public static final int YEAR = 2;
    private static final PartitionAddMethod ADD_DD = Timestamps::addDays;
    private static final PartitionAddMethod ADD_HH = Timestamps::addHours;
    private static final PartitionAddMethod ADD_MM = Timestamps::addMonths;
    private static final PartitionAddMethod ADD_WW = Timestamps::addWeeks;
    private static final PartitionAddMethod ADD_YYYY = Timestamps::addYears;
    private static final PartitionCeilMethod CEIL_DD = Timestamps::ceilDD;
    private static final PartitionCeilMethod CEIL_HH = Timestamps::ceilHH;
    private static final PartitionCeilMethod CEIL_MM = Timestamps::ceilMM;
    private static final PartitionCeilMethod CEIL_WW = Timestamps::ceilWW;
    private static final PartitionCeilMethod CEIL_YYYY = Timestamps::ceilYYYY;
    private static final DateFormat DEFAULT_FORMAT = new DateFormat() {
        @Override
        public void format(long datetime, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            sink.putAscii(DEFAULT_PARTITION_NAME);
        }

        @Override
        public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) {
            return parse(in, 0, in.length(), locale);
        }

        @Override
        public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) {
            return 0;
        }
    };
    private static final PartitionFloorMethod FLOOR_DD = Timestamps::floorDD;
    private static final PartitionFloorMethod FLOOR_HH = Timestamps::floorHH;
    private static final PartitionFloorMethod FLOOR_MM = Timestamps::floorMM;
    private static final PartitionFloorMethod FLOOR_WW = Timestamps::floorWW;
    private static final PartitionFloorMethod FLOOR_YYYY = Timestamps::floorYYYY;
    private static final DateFormat PARTITION_DAY_FORMAT = new IsoDatePartitionFormat(FLOOR_DD, DAY_FORMAT);
    private static final DateFormat PARTITION_HOUR_FORMAT = new IsoDatePartitionFormat(FLOOR_HH, HOUR_FORMAT);
    private static final DateFormat PARTITION_MONTH_FORMAT = new IsoDatePartitionFormat(FLOOR_MM, MONTH_FORMAT);
    private static final DateFormat PARTITION_WEEK_FORMAT = new IsoWeekPartitionFormat();
    private static final DateFormat PARTITION_YEAR_FORMAT = new IsoDatePartitionFormat(FLOOR_YYYY, YEAR_FORMAT);
    private static final LowerCaseCharSequenceIntHashMap nameToIndexMap = new LowerCaseCharSequenceIntHashMap();
    private static final LowerCaseUtf8SequenceIntHashMap nameToIndexMapUtf8 = new LowerCaseUtf8SequenceIntHashMap();
    private static final LowerCaseCharSequenceIntHashMap ttlUnitToIndexMap = new LowerCaseCharSequenceIntHashMap();

    private PartitionBy() {
    }

    public static int fromString(CharSequence name) {
        return nameToIndexMap.get(name);
    }

    public static int fromUtf8String(Utf8Sequence name) {
        return nameToIndexMapUtf8.get(name);
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
                return PARTITION_DAY_FORMAT;
            case MONTH:
                return PARTITION_MONTH_FORMAT;
            case YEAR:
                return PARTITION_YEAR_FORMAT;
            case HOUR:
                return PARTITION_HOUR_FORMAT;
            case WEEK:
                return PARTITION_WEEK_FORMAT;
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

    public static boolean isPartitioned(int partitionBy) {
        return partitionBy != NONE;
    }

    public static long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy) {
        return parsePartitionDirName(partitionName, partitionBy, 0, partitionName.length());
    }

    public static long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy, int lo, int hi) {
        CharSequence fmtStr;
        try {
            DateFormat fmtMethod;
            switch (partitionBy) {
                case DAY:
                    fmtMethod = PARTITION_DAY_FORMAT;
                    fmtStr = DAY_PATTERN;
                    break;
                case MONTH:
                    fmtMethod = PARTITION_MONTH_FORMAT;
                    fmtStr = MONTH_PATTERN;
                    break;
                case YEAR:
                    fmtMethod = PARTITION_YEAR_FORMAT;
                    fmtStr = YEAR_PATTERN;
                    break;
                case HOUR:
                    fmtMethod = PARTITION_HOUR_FORMAT;
                    fmtStr = HOUR_PATTERN;
                    break;
                case WEEK:
                    fmtMethod = PARTITION_WEEK_FORMAT;
                    fmtStr = WEEK_PATTERN;
                    break;
                case NONE:
                    fmtMethod = DEFAULT_FORMAT;
                    fmtStr = partitionName;
                    break;
                default:
                    throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
            }
            int limit = fmtStr.length();
            if (hi < 0) {
                // Automatic partition name trimming.
                hi = lo + Math.min(limit, partitionName.length());
            }
            if (hi - lo < limit) {
                throw expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
            }
            return fmtMethod.parse(partitionName, lo, hi, DateFormatUtils.EN_LOCALE);
        } catch (NumericException e) {
            if (partitionBy == PartitionBy.WEEK) {
                // maybe the user used a timestamp, or a date, string.
                int localLimit = DAY_PATTERN.length();
                try {
                    // trim to lowest precision needed and get the timestamp
                    // convert timestamp to first day of the week
                    return Timestamps.floorDOW(DAY_FORMAT.parse(partitionName, 0, localLimit, DateFormatUtils.EN_LOCALE));
                } catch (NumericException ignore) {
                    throw expectedPartitionDirNameFormatCairoException(partitionName, 0, Math.min(partitionName.length(), localLimit), partitionBy);
                }
            }
            throw expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
        }
    }

    public static void setSinkForPartition(CharSink<?> path, int partitionBy, long timestamp) {
        if (partitionBy != PartitionBy.NONE) {
            getPartitionDirFormatMethod(partitionBy).format(timestamp, DateFormatUtils.EN_LOCALE, null, path);
            return;
        }
        path.putAscii(DEFAULT_PARTITION_NAME);
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

    public static int ttlUnitFromString(CharSequence name, int start, int limit) {
        return ttlUnitToIndexMap.valueAt(ttlUnitToIndexMap.keyIndex(name, start, limit));
    }

    public static void validateTtlGranularity(int partitionBy, int ttlHoursOrMonths, int ttlValuePos) throws SqlException {
        switch (partitionBy) {
            case NONE:
                throw SqlException.position(ttlValuePos).put("cannot set TTL on a non-partitioned table");
            case DAY:
                if (ttlHoursOrMonths < 0 || ttlHoursOrMonths % 24 == 0) {
                    return;
                }
                break;
            case WEEK:
                if (ttlHoursOrMonths < 0 || ttlHoursOrMonths % (24 * 7) == 0) {
                    return;
                }
                break;
            case MONTH:
                if (ttlHoursOrMonths < 0) {
                    return;
                }
                break;
            case YEAR:
                if (ttlHoursOrMonths < 0 && ttlHoursOrMonths % 12 == 0) {
                    return;
                }
                break;
            default:
                return;
        }
        throw SqlException.position(ttlValuePos)
                .put("TTL value must be an integer multiple of partition size");
    }

    private static CairoException expectedPartitionDirNameFormatCairoException(CharSequence partitionName, int lo, int hi, int partitionBy) {
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
        ee.put("' expected, found [ts=").put(partitionName.subSequence(lo, hi)).put(']');
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

        nameToIndexMapUtf8.put(new Utf8String("day"), DAY);
        nameToIndexMapUtf8.put(new Utf8String("month"), MONTH);
        nameToIndexMapUtf8.put(new Utf8String("year"), YEAR);
        nameToIndexMapUtf8.put(new Utf8String("hour"), HOUR);
        nameToIndexMapUtf8.put(new Utf8String("week"), WEEK);
        nameToIndexMapUtf8.put(new Utf8String("none"), NONE);

        ttlUnitToIndexMap.put("h", HOUR);
        ttlUnitToIndexMap.put("hour", HOUR);
        ttlUnitToIndexMap.put("hours", HOUR);
        ttlUnitToIndexMap.put("d", DAY);
        ttlUnitToIndexMap.put("day", DAY);
        ttlUnitToIndexMap.put("days", DAY);
        ttlUnitToIndexMap.put("w", WEEK);
        ttlUnitToIndexMap.put("week", WEEK);
        ttlUnitToIndexMap.put("weeks", WEEK);
        ttlUnitToIndexMap.put("m", MONTH);
        ttlUnitToIndexMap.put("month", MONTH);
        ttlUnitToIndexMap.put("months", MONTH);
        ttlUnitToIndexMap.put("y", YEAR);
        ttlUnitToIndexMap.put("year", YEAR);
        ttlUnitToIndexMap.put("years", YEAR);
    }
}
