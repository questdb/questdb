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

package io.questdb.cairo.sql;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

/**
 * Access the value of columns of a table record by column index.
 * <p>
 * Type checking is not performed beforehand, meaning the type of the
 * element being retrieved by the following methods should be known by
 * performing a prior lookup using {@link io.questdb.cairo.sql.RecordMetadata}
 */
public interface Record {

    CharSequenceFunction GET_STR = (record, col, sink) -> record.getStrA(col);

    CharSequenceFunction GET_SYM = (record, col, sink) -> record.getSymA(col);

    CharSequenceFunction GET_VARCHAR = (record, col, sink) -> {
        Utf8Sequence vch = record.getVarcharA(col);
        if (vch == null) {
            return null;
        }
        if (vch.isAscii()) {
            return vch.asAsciiCharSequence();
        }
        sink.clear();
        sink.put(vch);
        return sink;
    };

    default ArrayView getArray(int col, int columnType) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a binary column by index
     *
     * @param col numeric index of the column
     * @return the binary content
     */
    default BinarySequence getBin(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the length of the binary value of a column by index
     *
     * @param col numeric index of the column
     * @return length of the binary content
     */
    default long getBinLen(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a boolean column by index
     *
     * @param col numeric index of the column
     * @return boolean
     */
    default boolean getBool(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a byte column by index
     *
     * @param col numeric index of the column
     * @return byte as 8-bit signed integer
     */
    default byte getByte(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a char column by index
     *
     * @param col numeric index of the column
     * @return 16-bit char value
     */
    default char getChar(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a date column by index
     *
     * @param col numeric index of the column
     * @return date value of the column as 64-bit signed long integer
     */
    default long getDate(int col) {
        return getLong(col);
    }

    default void getDecimal128(int col, Decimal128 sink) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the 16-bit decimal value by index.
     *
     * @param col numeric index of the column
     * @return 16-bit signed integer
     */
    default short getDecimal16(int col) {
        throw new UnsupportedOperationException();
    }

    default void getDecimal256(int col, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the 32-bit decimal value by index.
     *
     * @param col numeric index of the column
     * @return 32-bit signed integer
     */
    default int getDecimal32(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the 64-bit decimal value by index.
     *
     * @param col numeric index of the column
     * @return 64-bit signed integer
     */
    default long getDecimal64(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the 8-bit decimal value by index.
     *
     * @param col numeric index of the column
     * @return 8-bit signed integer
     */
    default byte getDecimal8(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a double column by index
     *
     * @param col numeric index of the column
     * @return 64-bit double floating point
     */
    default double getDouble(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a float column by index
     *
     * @param col numeric index of the column
     * @return 32-bit floating point value
     */
    default float getFloat(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a byte GeoHash column by index
     *
     * @param col numeric index of the column
     * @return geohash
     */
    default byte getGeoByte(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of an int GeoHash column by index
     *
     * @param col numeric index of the column
     * @return geohash
     */
    default int getGeoInt(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a long GeoHash column by index
     *
     * @param col numeric index of the column
     * @return geohash
     */
    default long getGeoLong(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a short GeoHash column by index
     *
     * @param col numeric index of the column
     * @return geohash
     */
    default short getGeoShort(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of an IPv4 column by index
     * Distinct from getInt(int col) because INT and IPv4 have different null values
     *
     * @param col numeric index of the column
     * @return 32-bit integer
     */
    default int getIPv4(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of an integer column by index
     *
     * @param col numeric index of the column
     * @return 32-bit integer
     */
    default int getInt(int col) {
        throw new UnsupportedOperationException();
    }

    default Interval getInterval(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a long column by index
     *
     * @param col numeric index of the column
     * @return 64-bit signed integer
     */
    default long getLong(int col) {
        throw new UnsupportedOperationException();
    }

    default long getLong128Hi(int col) {
        throw new UnsupportedOperationException();
    }

    default long getLong128Lo(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a long256 column by index
     *
     * @param col  numeric index of the column
     * @param sink an ASCII sink
     */
    default void getLong256(int col, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a long256 column by index.
     * getLong256A used for A/B comparison with getLong256B to compare references.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column
     * @return unsigned 256-bit integer
     */
    default Long256 getLong256A(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a long256 column by index.
     * getLong256B used for A/B comparison with getLong256A to compare references.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column
     * @return unsigned 256-bit integer
     */
    default Long256 getLong256B(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of an IPv4 column by index as a long (only needed for sorting)
     * Distinct from getInt(int col) because INT and IPv4 have different null values
     *
     * @param col numeric index of the column
     * @return 64-bit integer
     */
    @SuppressWarnings("unused")
    default long getLongIPv4(int col) {
        return Numbers.ipv4ToLong(getIPv4(col));
    }

    /**
     * Get record by column index
     *
     * @param col numeric index of the column
     * @return record
     */
    default Record getRecord(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the numeric ID of this row. This can be not real table row id
     *
     * @return numeric ID of the current row
     */
    default long getRowId() {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a short column by index
     *
     * @param col numeric index of the column
     * @return 16-bit signed integer
     */
    default short getShort(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads string-specific storage and presents the value as
     * UTF16-encoded sequence of bytes. It is a part of value comparison
     * system, which utilizes A and B objects to represent values of
     * multiple fields of the same record. Functions, such as "=" must
     * always compare getStrA(col) = getStrB(col) to make sure CharSequence
     * containers are not being spuriously reused.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column, 0-based
     * @return lightweight container that avoids creating copies of strings in
     * memory. Null if sting value is null.
     */
    @Nullable
    default CharSequence getStrA(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads string-specific storage and presents the value as
     * UTF16-encoded sequence of bytes. It is a part of value comparison
     * system, which utilizes A and B objects to represent values of
     * multiple fields of the same record. Functions, such as "=" must
     * always compare getStrA(col) = getStrB(col) to make sure CharSequence
     * containers are not being spuriously reused.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column, 0-based
     * @return lightweight container that avoids creating copies of strings in
     * memory. Null if sting value is null.
     */
    default CharSequence getStrB(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the length of the string value of a column by index
     *
     * @param col numeric index of the column
     * @return length of the string value
     */
    default int getStrLen(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a symbol column by index.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column
     * @return symbol value as string
     */
    default CharSequence getSymA(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the string-based value of a symbol column by index.
     * getSymB used for A/B comparison with getSym to compare references.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column
     * @return symbol value as string
     */
    default CharSequence getSymB(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the value of a timestamp column by index
     *
     * @param col numeric index of the column
     * @return 64-bit signed integer
     */
    default long getTimestamp(int col) {
        return getLong(col);
    }

    /**
     * Gets the numeric ID of this row. This must be real table row id
     *
     * @return numeric ID of the current row
     */
    default long getUpdateRowId() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads bytes from varchar-specific storage and prints them into UTF16 encoded
     * sink.
     *
     * @param col       numeric index of the column, 0-based
     * @param utf16Sink the destination sink
     */
    default void getVarchar(int col, Utf16Sink utf16Sink) {
        utf16Sink.put(getVarcharA(col));
    }

    /**
     * Reads varchar-specific storage and presents the value as
     * UTF8-encoded sequence of bytes. It is a part of value comparison
     * system, which utilizes A and B objects to represent values of
     * multiple fields of the same record. Functions, such as "=" must
     * always compare getVarcharA(col) = getVarcharB(col) to make sure Utf8Sequence
     * containers are not being spuriously reused.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column, 0-based
     * @return lightweight container that avoids creating copies of strings in
     * memory. Null if sting value is null.
     */
    @Nullable
    default Utf8Sequence getVarcharA(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads varchar-specific storage and presents the value as
     * UTF8-encoded sequence of bytes. It is a part of value comparison
     * system, which utilizes A and B objects to represent values of
     * multiple fields of the same record. Functions, such as "=" must
     * always compare getVarcharA(col) = getVarcharB(col) to make sure Utf8Sequence
     * containers are not being spuriously reused.
     * <p>
     * Important: record implementations must not reuse a single flyweight
     * across all columns.
     *
     * @param col numeric index of the column, 0-based
     * @return lightweight container that avoids creating copies of strings in
     * memory. Null if sting value is null.
     */
    @Nullable
    default Utf8Sequence getVarcharB(int col) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the size of the varchar value of a column by index
     *
     * @param col numeric index of the column
     * @return size of the varchar value or {@link TableUtils#NULL_LEN} in case of NULL
     */
    default int getVarcharSize(int col) {
        throw new UnsupportedOperationException();
    }

    @FunctionalInterface
    interface CharSequenceFunction {
        /**
         * @param record to retrieve CharSequence from
         * @param col    numeric index of the column
         * @param sink   sink the function can use if a conversion is required
         * @return record as a char sequence
         */
        CharSequence get(Record record, int col, MutableUtf16Sink sink);
    }
}
