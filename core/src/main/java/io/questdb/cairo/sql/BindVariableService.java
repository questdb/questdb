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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.griffin.SqlException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;

/**
 * Allows for setting the values of bind variables passed
 * in an SQL query by their index (position in a list of bind variables).
 * <p>
 * Types of bind variables can be defined either via setting them explicitly before
 * SQL is executed or having SQL compiler infer types from expression where bind variable
 * is used. Once type is set, bind variable can be assigned value only from a compatible type.
 */
public interface BindVariableService extends Mutable {

    int define(int variableIndex, int type, int position) throws SqlException;

    Function getFunction(CharSequence name);

    Function getFunction(int index);

    /**
     * @return number of bind variables in a query
     */
    int getIndexedVariableCount();

    /**
     * @return list of named variables in a query
     */
    ObjList<CharSequence> getNamedVariables();

    void setArray(int i, ArrayView ab) throws SqlException;

    /**
     * Set the type of bind variable by name as binary and provide a value
     *
     * @param name  of the bind variable
     * @param value as binary
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Binary
     */
    void setBin(CharSequence name, BinarySequence value) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Binary
     */
    void setBin(int index) throws SqlException;

    /**
     * Set type of bind variable by index as binary and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as binary
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Binary
     */
    void setBin(int index, BinarySequence value) throws SqlException;

    /**
     * Set type of bind variable by name as boolean and provide a value
     *
     * @param name  of the bind variable
     * @param value as boolean
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Boolean
     */
    void setBoolean(CharSequence name, boolean value) throws SqlException;

    /**
     * Set type of bind variable by index as boolean
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Boolean
     */
    void setBoolean(int index) throws SqlException;

    /**
     * Set type of bind variable by index as boolean and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as boolean
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Boolean
     */
    void setBoolean(int index, boolean value) throws SqlException;

    /**
     * Set type of bind variable by name as byte and provide a value
     *
     * @param name  of the bind variable
     * @param value as byte
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Byte
     */
    void setByte(CharSequence name, byte value) throws SqlException;

    /**
     * Set type of bind variable by index as byte
     *
     * @param index numeric index of the bind variable
     * @param value value as byte
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Byte
     */
    void setByte(int index, byte value) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Byte
     */
    void setByte(int index) throws SqlException;

    /**
     * Set type of bind variable by index as char and provide a value
     *
     * @param name  of the bind variable
     * @param value as character
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Character
     */
    void setChar(CharSequence name, char value) throws SqlException;

    /**
     * Set type of bind variable by index as char
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Character
     */
    void setChar(int index) throws SqlException;

    /**
     * Set type of bind variable by index as char and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value character
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Character
     */
    void setChar(int index, char value) throws SqlException;

    /**
     * Set type of bind variable by name as date and provide a value
     *
     * @param name  of the bind variable
     * @param value date as long
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Date
     */
    void setDate(CharSequence name, long value) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Date
     */
    void setDate(int index) throws SqlException;

    /**
     * Set type of bind variable by index as date and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value date as long
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Date
     */
    void setDate(int index, long value) throws SqlException;

    /**
     * Set type of bind variable by index as Decimal and provide a value
     *
     * @param index numeric index of the bind variable
     * @param hh    highest 64 bits of Decimal
     * @param hl    high 64 bits of Decimal
     * @param lh    middle 64 bits of Decimal
     * @param ll    lower 64 bits of Decimal
     * @param type  type of Decimal, containing the precision/scale
     * @throws SqlException is throw when variable has already been defined with type that is not compatible with UUID
     */
    void setDecimal(int index, long hh, long hl, long lh, long ll, int type) throws SqlException;

    /**
     * Set type of bind variable by name as Decimal and provide a value
     *
     * @param name of the bind variable
     * @param hh   highest 64 bits of Decimal
     * @param hl   high 64 bits of Decimal
     * @param lh   middle 64 bits of Decimal
     * @param ll   lower 64 bits of Decimal
     * @param type type of Decimal, containing the precision/scale
     * @throws SqlException is throw when variable has already been defined with type that is not compatible with UUID
     */
    void setDecimal(CharSequence name, long hh, long hl, long lh, long ll, int type) throws SqlException;

    /**
     * Set type of bind variable by name as double and provide a value
     *
     * @param name  of the bind variable
     * @param value as double
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Double
     */
    void setDouble(CharSequence name, double value) throws SqlException;

    /**
     * Set type of bind variable by index as double
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Double
     */
    void setDouble(int index) throws SqlException;

    /**
     * @param index numeric index of the bind variable
     * @param value as double
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Double
     */
    void setDouble(int index, double value) throws SqlException;

    /**
     * Set type of bind variable by name as float and provide a value
     *
     * @param name  of the bind variable
     * @param value as float
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Float
     */
    void setFloat(CharSequence name, float value) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Float
     */
    void setFloat(int index) throws SqlException;

    /**
     * Set type of bind variable by index as float and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as float
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Float
     */
    void setFloat(int index, float value) throws SqlException;

    /**
     * Set type of bind variable by index as geo hash
     *
     * @param name  of the bind variable
     * @param value value as geo hash
     * @param type  geo hash type
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with geo hash
     */
    void setGeoHash(CharSequence name, long value, int type) throws SqlException;

    /**
     * Set type of bind variable by index as geo hash
     *
     * @param index numeric index of the bind variable
     * @param value value as geo hash
     * @param type  geo hash type
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with geo hash
     */
    void setGeoHash(int index, long value, int type) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @param type  type of GeoHash, specifies number of bits
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Byte
     */
    void setGeoHash(int index, int type) throws SqlException;

    /**
     * Set type of bind variable by index as ipv4 (int form) and provide a value
     * Distinct from int because of different null values
     *
     * @param index numeric index of the bind variable
     * @param value as integer
     */
    void setIPv4(int index, int value);

    /**
     * Set type of bind variable by index as ipv4 (CharSequence form) and provide a value
     * Distinct from int because of different null values
     *
     * @param index numeric index of the bind variable
     * @param value as CharSequence
     */
    void setIPv4(int index, CharSequence value);

    /**
     * Set type of bind variable by index as binary
     * Distinct from int because of different null values
     *
     * @param index numeric index of the bind variable
     */
    void setIPv4(int index);

    /**
     * Set type of bind variable by name as integer and provide a value
     *
     * @param name  of the bind variable
     * @param value as integer
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Int
     */
    void setInt(CharSequence name, int value) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Int
     */
    void setInt(int index) throws SqlException;

    /**
     * Set type of bind variable by index as integer and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as integer
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Int
     */
    void setInt(int index, int value) throws SqlException;

    /**
     * Set type of bind variable by name as long and provide a value
     *
     * @param name  of the bind variable
     * @param value as long
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long
     */
    void setLong(CharSequence name, long value) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long
     */
    void setLong(int index) throws SqlException;

    /**
     * Set type of bind variable by index as long and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as long
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long
     */
    void setLong(int index, long value) throws SqlException;

    /**
     * Set type of bind variable by name as long256 and provide a value
     *
     * @param name of the bind variable
     * @param l0   64 bit long 0
     * @param l1   64 bit long 1
     * @param l2   64 bit long 2
     * @param l3   64 bit long 3
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long256
     */
    void setLong256(CharSequence name, long l0, long l1, long l2, long l3) throws SqlException;

    /**
     * Set type of bind variable by name as long256 and provide a value
     *
     * @param name  of the bind variable
     * @param value as long256
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long256
     */
    void setLong256(CharSequence name, Long256 value) throws SqlException;

    /**
     * Set type of bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long256
     */
    void setLong256(int index) throws SqlException;

    /**
     * Set type of bind variable by index as long256
     *
     * @param index numeric index of the bind variable
     * @param l0    64 bit long 0
     * @param l1    64 bit long 1
     * @param l2    64 bit long 2
     * @param l3    64 bit long 3
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long256
     */
    void setLong256(int index, long l0, long l1, long l2, long l3) throws SqlException;

    /**
     * Set type of bind variable by name as long256
     *
     * @param name of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Long256
     */
    void setLong256(CharSequence name) throws SqlException;

    /**
     * Set type of bind variable by index as short
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Short
     */
    void setShort(int index) throws SqlException;

    /**
     * Set type of bind variable by index as short and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as short
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Short
     */
    void setShort(int index, short value) throws SqlException;

    /**
     * Set type of bind variable by name as long256 and provide a value
     *
     * @param name  of the bind variable
     * @param value as short
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Short
     */
    void setShort(CharSequence name, short value) throws SqlException;

    /**
     * Set type of bind variable by index as string
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with String
     */
    void setStr(int index) throws SqlException;

    /**
     * Set type of bind variable by index as string
     *
     * @param index numeric index of the bind variable
     * @param value as string
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with String
     */
    void setStr(int index, CharSequence value) throws SqlException;

    /**
     * Set type of bind variable by name as string and provide a value
     *
     * @param name  of the bind variable
     * @param value as string
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with String
     */
    void setStr(CharSequence name, CharSequence value) throws SqlException;

    /**
     * Set type of bind variable by index as timestamp (microsecond precision)
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestamp(int index) throws SqlException;

    /**
     * Set type of bind variable by index as timestamp (microsecond precision) and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value timestamp value in microseconds
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestamp(int index, long value) throws SqlException;

    /**
     * Set type of bind variable by name as timestamp (microsecond precision) and provide a value
     *
     * @param name  of the bind variable
     * @param value timestamp value in microseconds
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestamp(CharSequence name, long value) throws SqlException;

    /**
     * Set type of bind variable by name as timestamp (nanosecond precision) and provide a value
     *
     * @param name  of the bind variable
     * @param value timestamp value in nanoseconds
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestampNano(CharSequence name, long value) throws SqlException;

    /**
     * Set type of bind variable by index as timestamp (nanosecond precision)
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestampNano(int index) throws SqlException;

    /**
     * Set type of bind variable by index as timestamp (nanosecond precision) and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value timestamp value in nanoseconds
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestampNano(int index, long value) throws SqlException;

    /**
     * Set type of bind variable by index as timestamp with specified precision type and provide a value
     *
     * @param index         numeric index of the bind variable
     * @param timestampType timestamp columnType ({@link io.questdb.cairo.ColumnType#TIMESTAMP_MICRO} or {@link io.questdb.cairo.ColumnType#TIMESTAMP_NANO})
     * @param value         timestamp value
     * @throws SqlException is thrown when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestampWithType(int index, int timestampType, long value) throws SqlException;

    /**
     * Set type of bind variable by index as UUID and provide a value
     *
     * @param index numeric index of the bind variable
     * @param lo    lower 64 bits of UUID
     * @param hi    higher 64 bits of UUID
     * @throws SqlException is throw when variable has already been defined with type that is not compatible with UUID
     */
    void setUuid(int index, long lo, long hi) throws SqlException;

    /**
     * Set type of bind variable by name as UUID and provide a value
     *
     * @param name of the bind variable
     * @param lo   lower 64 bits of UUID
     * @param hi   higher 64 bits of UUID
     * @throws SqlException is throw when variable has already been defined with type that is not compatible with UUID
     */
    void setUuid(CharSequence name, long lo, long hi) throws SqlException;

    /**
     * Set type of bind variable by index as varchar
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with UTF8 encoded String
     */
    void setVarchar(int index) throws SqlException;

    /**
     * Set type of bind variable by index as varchar and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as Utf8Sequence
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with UTF8 encoded String
     */
    void setVarchar(int index, @Transient Utf8Sequence value) throws SqlException;

    /**
     * Set type of bind variable by name as varchar and provide a value
     *
     * @param name  of the bind variable
     * @param value as Utf8Sequence
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with UTF8 encoded String
     */
    void setVarchar(CharSequence name, Utf8Sequence value) throws SqlException;
}