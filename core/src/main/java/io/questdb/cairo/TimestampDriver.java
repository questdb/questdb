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
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public interface TimestampDriver {

    long addMonths(long timestamp, int months);

    long addYears(long timestamp, int years);

    void append(CharSink<?> sink, long timestamp);

    void appendMem(CharSequence value, MemoryA mem);

    void appendPGWireText(CharSink<?> sink, long timestamp);

    long castStr(CharSequence value, int tupleIndex, short fromType, short toType);

    // todo: explore static ref
    boolean convertToVar(long fixedAddr, CharSink<?> stringSink);

    long fromDays(int days);

    long fromHours(int hours);

    long fromMinutes(int minutes);

    long fromSeconds(int seconds);

    PartitionAddMethod getPartitionAddMethod(int partitionBy);

    PartitionCeilMethod getPartitionCeilMethod(int partitionBy);

    DateFormat getPartitionDirFormatMethod(int partitionBy);

    PartitionFloorMethod getPartitionFloorMethod(int partitionBy);

    long implicitCast(CharSequence value, int typeFrom);

    default long implicitCast(CharSequence value) {
        return implicitCast(value, ColumnType.STRING);
    }

    long implicitCastVarchar(Utf8Sequence value);

    long parseAnyFormat(CharSequence token, int start, int len) throws NumericException;

    long parseFloor(CharSequence str, int lo, int hi) throws NumericException;

    default long parseFloorConstant(CharSequence quotedTimestampStr) throws NumericException {
        return parseFloor(quotedTimestampStr, 1, quotedTimestampStr.length() - 1);
    }

    default long parseFloorLiteral(CharSequence timestampLiteral) throws NumericException {
        return parseFloor(timestampLiteral, 0, timestampLiteral.length());
    }

    long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy, int lo, int hi);

    long toNanosScale();

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
