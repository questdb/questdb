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

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.SqlUtil.castPGDates;

public interface TimestampDriver {
    long addMonths(long timestamp, int months);

    long addPeriod(long lo, char type, int period);

    long addYears(long timestamp, int years);

    void append(CharSink<?> sink, long timestamp);

    default void appendMem(CharSequence value, MemoryA mem) {
        try {
            mem.putLong(parseFloorLiteral(value));
        } catch (NumericException e) {
            mem.putLong(Numbers.LONG_NULL);
        }
    }

    void appendPGWireText(CharSink<?> sink, long timestamp);

    @SuppressWarnings("unused")
        // used by the row copier
    long castAsDate(long timestamp);

    @SuppressWarnings("unused")
        // used by the row copier
    long castDateAs(long timestamp);

    default long castStr(CharSequence value, int tupleIndex, int fromType, int toType) {
        try {
            return parseFloorLiteral(value);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(tupleIndex, value, fromType, toType);
        }
    }

    // todo: explore static ref
    boolean convertToVar(long fixedAddr, CharSink<?> stringSink);

    long fromDays(int days);

    long fromHours(int hours);

    long fromMinutes(int minutes);

    long fromSeconds(int seconds);

    int getColumnType();

    PartitionAddMethod getPartitionAddMethod(int partitionBy);

    PartitionCeilMethod getPartitionCeilMethod(int partitionBy);

    DateFormat getPartitionDirFormatMethod(int partitionBy);

    PartitionFloorMethod getPartitionFloorMethod(int partitionBy);

    TimestampUtils.TimestampUnitConverter getTimestampUnitConverter(int srcTimestampType);

    default long implicitCast(CharSequence value, int typeFrom) {
        assert typeFrom == ColumnType.STRING || typeFrom == ColumnType.SYMBOL;
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException ignore) {
            }

            // Parse as ISO with variable length.
            try {
                return parseFloorLiteral(value);
            } catch (NumericException ignore) {
            }

            return castPGDates(value, typeFrom, getColumnType());
        }
        return Numbers.LONG_NULL;
    }

    default long implicitCast(CharSequence value) {
        return implicitCast(value, ColumnType.STRING);
    }

    default long implicitCastVarchar(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException ignore) {
            }

            // Parse as ISO with variable length.
            try {
                return parseFloorLiteral(value);
            } catch (NumericException ignore) {
            }

            // all formats are ascii
            if (value.isAscii()) {
                return castPGDates(value.asAsciiCharSequence(), ColumnType.VARCHAR, getColumnType());
            }
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, getColumnType());
        }
        return Numbers.LONG_NULL;
    }

    long monthsBetween(long hi, long lo);

    long parseAnyFormat(CharSequence token, int start, int len) throws NumericException;

    long parseFloor(CharSequence str, int lo, int hi) throws NumericException;

    long parseFloor(Utf8Sequence str, int lo, int hi) throws NumericException;

    default long parseFloorConstant(@NotNull CharSequence quotedTimestampStr) throws NumericException {
        return parseFloor(quotedTimestampStr, 1, quotedTimestampStr.length() - 1);
    }

    default long parseFloorLiteral(@Nullable CharSequence timestampLiteral) throws NumericException {
        return timestampLiteral != null ? parseFloor(timestampLiteral, 0, timestampLiteral.length()) : Numbers.LONG_NULL;
    }

    default long parseFloorLiteral(@Nullable Utf8Sequence timestampLiteral) throws NumericException {
        return timestampLiteral != null ? parseFloor(timestampLiteral, 0, timestampLiteral.size()) : Numbers.LONG_NULL;
    }

    void parseInterval(CharSequence input, int pos, int lim, short operation, LongList out) throws NumericException;

    long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy, int lo, int hi);

    long toMicros(long timestamp);

    long toNanos(long timestamp);

    long toNanosScale();

    void validateBounds(long timestamp);

    @FunctionalInterface
    interface PartitionAddMethod {
        long calculate(long timestamp, int increment);
    }

    @FunctionalInterface
    interface PartitionCeilMethod {
        // returns exclusive ceiling for the give timestamp
        long ceil(long timestamp);
    }

    @FunctionalInterface
    interface PartitionFloorMethod {
        long floor(long timestamp);
    }
}
