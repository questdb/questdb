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

package io.questdb.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseUtf8SequenceIntHashMap;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.TableUtils.DEFAULT_PARTITION_NAME;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

/**
 * Collection of static assets to provide time partitioning API. It should be
 * possible to express any time partitioning strategy via this API alone.
 * The rest of QuestDB should just pick it up natively.
 */
public final class PartitionBy {

    public static final int DAY = 0;
    public static final int HOUR = 4;
    public static final int MINUTE_10 = 8;
    public static final int MINUTE_15 = 9;
    public static final int MINUTE_20 = 10;
    public static final int MINUTE_30 = 11;
    /**
     * Sub-hour partitioning: partitions are aligned to UTC wall-clock multiples of the given
     * number of minutes. Valid minute values divide an hour evenly, so partition boundaries
     * never cross hour boundaries.
     */
    public static final int MINUTE_5 = 7;
    public static final int MONTH = 1;
    /**
     * Data is not partitioned at all, all data is stored in a single directory
     */
    public static final int NONE = 3;
    public static final int NOT_APPLICABLE = 6;
    public static final int WEEK = 5;
    public static final int YEAR = 2;
    private static final LowerCaseCharSequenceIntHashMap nameToIndexMap = new LowerCaseCharSequenceIntHashMap();
    private static final LowerCaseUtf8SequenceIntHashMap nameToIndexMapUtf8 = new LowerCaseUtf8SequenceIntHashMap();
    private static final LowerCaseCharSequenceIntHashMap ttlUnitToIndexMap = new LowerCaseCharSequenceIntHashMap();

    private PartitionBy() {
    }

    /**
     * Returns the PartitionBy constant for the given minute count, or -1 when the
     * count is not a supported sub-hour partition size (5, 10, 15, 20 or 30).
     */
    public static int fromMinutes(long minutes) {
        return switch ((int) Math.min(minutes, Integer.MAX_VALUE)) {
            case 5 -> MINUTE_5;
            case 10 -> MINUTE_10;
            case 15 -> MINUTE_15;
            case 20 -> MINUTE_20;
            case 30 -> MINUTE_30;
            default -> -1;
        };
    }

    /**
     * Parses a partitioning name: NONE, HOUR, DAY, WEEK, MONTH, YEAR, or a sub-hour
     * size written as {@code <n> MINUTE[S]}, e.g. "15 MINUTES". Case-insensitive.
     * Returns -1 for an unknown name.
     */
    public static int fromString(CharSequence name) {
        final int partitionBy = nameToIndexMap.get(name);
        if (partitionBy != -1 || name == null) {
            return partitionBy;
        }
        return parseMinutes(name);
    }

    public static int fromUtf8String(Utf8Sequence name) {
        final int partitionBy = nameToIndexMapUtf8.get(name);
        if (partitionBy != -1 || name == null) {
            return partitionBy;
        }
        // No isAscii() guard: HTTP URL params are DirectUtf8String instances that never set the ASCII
        // flag. A non-ASCII byte is neither a digit nor a letter of the unit, so parseMinutes() rejects it.
        return parseMinutes(name.asAsciiCharSequence());
    }

    /**
     * Returns the size in minutes of a sub-hour partitioning, or 0 for any other partitioning.
     */
    public static int getMinutes(int partitionBy) {
        return switch (partitionBy) {
            case MINUTE_5 -> 5;
            case MINUTE_10 -> 10;
            case MINUTE_15 -> 15;
            case MINUTE_20 -> 20;
            case MINUTE_30 -> 30;
            default -> 0;
        };
    }

    public static TimestampDriver.PartitionAddMethod getPartitionAddMethod(int timestampType, int partitionBy) {
        return ColumnType.getTimestampDriver(timestampType).getPartitionAddMethod(partitionBy);
    }

    public static TimestampDriver.TimestampCeilMethod getPartitionCeilMethod(int timestampType, int partitionBy) {
        return ColumnType.getTimestampDriver(timestampType).getPartitionCeilMethod(partitionBy);
    }

    public static DateFormat getPartitionDirFormatMethod(int timestampType, int partitionBy) {
        return ColumnType.getTimestampDriver(timestampType).getPartitionDirFormatMethod(partitionBy);
    }

    public static TimestampDriver.TimestampFloorMethod getPartitionFloorMethod(int timestampType, int partitionBy) {
        return ColumnType.getTimestampDriver(timestampType).getPartitionFloorMethod(partitionBy);
    }

    public static boolean isMinutePartitioned(int partitionBy) {
        return partitionBy >= MINUTE_5 && partitionBy <= MINUTE_30;
    }

    public static boolean isPartitioned(int partitionBy) {
        return partitionBy != NONE;
    }

    public static long parsePartitionDirName(@NotNull CharSequence partitionName, int timestampType, int partitionBy) {
        return parsePartitionDirName(partitionName, timestampType, partitionBy, 0, partitionName.length());
    }

    public static long parsePartitionDirName(@NotNull CharSequence partitionName, int timestampType, int partitionBy, int lo, int hi) {
        return ColumnType.getTimestampDriver(timestampType).parsePartitionDirName(partitionName, partitionBy, lo, hi);
    }

    public static void setSinkForPartition(CharSink<?> path, int timestampType, int partitionBy, long timestamp) {
        if (isPartitioned(partitionBy)) {
            getPartitionDirFormatMethod(timestampType, partitionBy).format(timestamp, EN_LOCALE, null, path);
            return;
        }
        path.putAscii(DEFAULT_PARTITION_NAME);
    }

    public static String toString(int partitionBy) {
        return switch (partitionBy) {
            case DAY -> "DAY";
            case MONTH -> "MONTH";
            case YEAR -> "YEAR";
            case HOUR -> "HOUR";
            case WEEK -> "WEEK";
            case NONE -> "NONE";
            case NOT_APPLICABLE -> "N/A";
            case MINUTE_5 -> "5 MINUTES";
            case MINUTE_10 -> "10 MINUTES";
            case MINUTE_15 -> "15 MINUTES";
            case MINUTE_20 -> "20 MINUTES";
            case MINUTE_30 -> "30 MINUTES";
            default -> "UNKNOWN";
        };
    }

    public static int ttlUnitFromString(CharSequence name, int start, int limit) {
        return ttlUnitToIndexMap.valueAt(ttlUnitToIndexMap.keyIndex(name, start, limit));
    }

    public static void validateTtlGranularity(int partitionBy, int ttlHoursOrMonths, int ttlValuePos) throws SqlException {
        switch (partitionBy) {
            case NONE:
            case NOT_APPLICABLE:
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
                // ttl == 0 means "unset" (no TTL), negative values are months-based TTL
                if (ttlHoursOrMonths < 1) {
                    return;
                }
                break;
            case YEAR:
                // ttl == 0 means "unset" (no TTL), negative values are months-based TTL
                if (ttlHoursOrMonths < 1 && ttlHoursOrMonths % 12 == 0) {
                    return;
                }
                break;
            default:
                return;
        }
        throw SqlException.position(ttlValuePos)
                .put("TTL value must be an integer multiple of the partition size (its time interval)");
    }

    /**
     * Parses "{@code <n> MINUTE}" / "{@code <n> MINUTES}", with any whitespace, including none,
     * between the number and the unit. Returns -1 when the text does not match or n is not a
     * supported minute partition size.
     */
    private static int parseMinutes(CharSequence name) {
        final int len = name.length();
        int p = 0;
        long n = 0;
        while (p < len) {
            final char c = name.charAt(p);
            if (c < '0' || c > '9') {
                break;
            }
            n = Math.min(n * 10 + (c - '0'), Integer.MAX_VALUE);
            p++;
        }
        if (p == 0) {
            return -1;
        }
        while (p < len && Character.isWhitespace(name.charAt(p))) {
            p++;
        }
        final int unitLen = len - p;
        if (unitLen != 6 && unitLen != 7) {
            return -1;
        }
        if (!Chars.equalsIgnoreCase("minute", name, p, p + 6)) {
            return -1;
        }
        if (unitLen == 7 && (name.charAt(len - 1) | 32) != 's') {
            return -1;
        }
        return fromMinutes(n);
    }


    static {
        nameToIndexMap.put("day", DAY);
        nameToIndexMap.put("month", MONTH);
        nameToIndexMap.put("year", YEAR);
        nameToIndexMap.put("hour", HOUR);
        nameToIndexMap.put("week", WEEK);
        nameToIndexMap.put("none", NONE);
        nameToIndexMap.put("n/a", NOT_APPLICABLE);

        nameToIndexMapUtf8.put(new Utf8String("day"), DAY);
        nameToIndexMapUtf8.put(new Utf8String("month"), MONTH);
        nameToIndexMapUtf8.put(new Utf8String("year"), YEAR);
        nameToIndexMapUtf8.put(new Utf8String("hour"), HOUR);
        nameToIndexMapUtf8.put(new Utf8String("week"), WEEK);
        nameToIndexMapUtf8.put(new Utf8String("none"), NONE);
        nameToIndexMapUtf8.put(new Utf8String("n/a"), NOT_APPLICABLE);

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
