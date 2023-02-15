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

package io.questdb.cairo.sql;

import io.questdb.griffin.SqlException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Allows for setting the values of bind variables passed
 * in an SQL query by their index (position in a list of bind variables).
 * <p>
 * Types of bind variables are can be defined either via setting them explicitly before
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
     * Set type of bind variable by index as timestamp
     *
     * @param index numeric index of the bind variable
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestamp(int index) throws SqlException;

    /**
     * Set type of bind variable by index as timestamp and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as long
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestamp(int index, long value) throws SqlException;

    /**
     * Set type of bind variable by name as timestamp and provide a value
     *
     * @param name  of the bind variable
     * @param value as long
     * @throws SqlException is throw when variable has already been defined with type
     *                      that is not compatible with Timestamp
     */
    void setTimestamp(CharSequence name, long value) throws SqlException;

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
}
