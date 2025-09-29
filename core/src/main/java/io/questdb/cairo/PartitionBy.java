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

import io.questdb.griffin.SqlException;
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
    public static final int MONTH = 1;
    /**
     * Data is not partitioned at all, all data is stored in a single directory
     */
    public static final int NONE = 3;
    public static final int WEEK = 5;
    public static final int YEAR = 2;
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
        if (partitionBy != PartitionBy.NONE) {
            getPartitionDirFormatMethod(timestampType, partitionBy).format(timestamp, EN_LOCALE, null, path);
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
