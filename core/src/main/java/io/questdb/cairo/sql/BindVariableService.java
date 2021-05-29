/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

/**
 * Allows for setting the values of bind variables passed
 * in a SQL query by their index (position in a list of bind variables).
 *
 * Type checking is not performed beforehand, meaning the type of the
 * element being set by the following methods should be known by
 * performing a prior lookup using {@link io.questdb.cairo.sql.RecordMetadata}
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
     * Set the type of a bind variable by name as binary and provide a value
     *
     * @param name of the bind variable
     * @param value as binary
     * @throws SqlException
     */
    void setBin(CharSequence name, BinarySequence value) throws SqlException;

    /**
     * Set type of a bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setBin(int index) throws SqlException;


    /**
     * Set type of a bind variable by index as binary and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as binary
     * @throws SqlException
     */
    void setBin(int index, BinarySequence value) throws SqlException;

    /**
     * Set type of a bind variable by name as boolean and provide a value
     *
     * @param name of the bind variable
     * @param value
     * @throws SqlException
     */
    void setBoolean(CharSequence name, boolean value) throws SqlException;

    /**
     * Set type of a bind variable by index as boolean
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setBoolean(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as boolean and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as boolean
     * @throws SqlException
     */
    void setBoolean(int index, boolean value) throws SqlException;

    /**
     * Set type of a bind variable by name as byte and provide a value
     *
     * @param name of the bind variable
     * @param value as byte
     * @throws SqlException
     */
    void setByte(CharSequence name, byte value) throws SqlException;

    /**
     * Set type of a bind variable by index as byte
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setByte(int index, byte value) throws SqlException;

    /**
     * Set type of a bind variable by index as char and provide a value
     *
     * @param name of the bind variable
     * @param value
     * @throws SqlException
     */
    void setChar(CharSequence name, char value) throws SqlException;

    /**
     * Set type of a bind variable by index as char
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setChar(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as char and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value char
     * @throws SqlException
     */
    void setChar(int index, char value) throws SqlException;

    /**
     * Set type of a bind variable by name as date and provide a value
     *
     * @param name of the bind variable
     * @param value date as long
     * @throws SqlException
     */
    void setDate(CharSequence name, long value) throws SqlException;

    /**
     * Set type of a bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setDate(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as date and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value date as long
     * @throws SqlException
     */
    void setDate(int index, long value) throws SqlException;

    /**
     * Set type of a bind variable by name as double and provide a value
     *
     * @param name of the bind variable
     * @param value as double
     * @throws SqlException
     */
    void setDouble(CharSequence name, double value) throws SqlException;

    /**
     * Set type of a bind variable by index as double
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setDouble(int index) throws SqlException;

    /**
     * @param index numeric index of the bind variable
     * @param value as double
     * @throws SqlException
     */
    void setDouble(int index, double value) throws SqlException;

    /**
     * Set type of a bind variable by name as float and provide a value
     *
     * @param name of the bind variable
     * @param value as float
     * @throws SqlException
     */
    void setFloat(CharSequence name, float value) throws SqlException;

    /**
     * Set type of a bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setFloat(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as float and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as float
     * @throws SqlException
     */
    void setFloat(int index, float value) throws SqlException;

    /**
     * Set type of a bind variable by name as integer and provide a value
     *
     * @param name of the bind variable
     * @param value as integer
     * @throws SqlException
     */
    void setInt(CharSequence name, int value) throws SqlException;

    /**
     * Set type of a bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setInt(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as integer and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as integer
     * @throws SqlException
     */
    void setInt(int index, int value) throws SqlException;

    /**
     * Set type of a bind variable by name as long and provide a value
     *
     * @param name of the bind variable
     * @param value as long
     * @throws SqlException
     */
    void setLong(CharSequence name, long value) throws SqlException;

    /**
     * Set type of a bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setLong(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as long and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as long
     * @throws SqlException
     */
    void setLong(int index, long value) throws SqlException;

    /**
     * Set type of a bind variable by name as long256 and provide a value
     *
     * @param name of the bind variable
     * @param l0 TODO
     * @param l1 TODO
     * @param l2 TODO
     * @param l3 TODO
     * @throws SqlException
     */
    void setLong256(CharSequence name, long l0, long l1, long l2, long l3) throws SqlException;

    /**
     * Set type of a bind variable by name as long256 and provide a value
     *
     * @param name of the bind variable
     * @param value as long256
     * @throws SqlException
     */
    void setLong256(CharSequence name, Long256 value) throws SqlException;

    /**
     * Set type of a bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setLong256(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as long256
     *
     * @param index numeric index of the bind variable
     * @param l0 TODO
     * @param l1 TODO
     * @param l2 TODO
     * @param l3 TODO
     * @throws SqlException
     */
    void setLong256(int index, long l0, long l1, long l2, long l3) throws SqlException;

    /**
     * Set type of a bind variable by name as long256
     *
     * @param name of the bind variable
     * @throws SqlException
     */
    void setLong256(CharSequence name) throws SqlException;

    /**
     * Set type of a bind variable by index as short
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setShort(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as short and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as short
     * @throws SqlException
     */
    void setShort(int index, short value) throws SqlException;

    /**
     * Set type of a bind variable by name as long256 and provide a value
     *
     * @param name of the bind variable
     * @param value as short
     * @throws SqlException
     */
    void setShort(CharSequence name, short value) throws SqlException;

    /**
     * Set type of a bind variable by index as string
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setStr(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as string
     *
     * @param index numeric index of the bind variable
     * @param value as string
     * @throws SqlException
     */
    void setStr(int index, CharSequence value) throws SqlException;

    /**
     * Set type of a bind variable by name as string and provide a value
     *
     * @param name of the bind variable
     * @param value as string
     * @throws SqlException
     */
    void setStr(CharSequence name, CharSequence value) throws SqlException;

    /**
     * Set type of a bind variable by index as timestamp
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setTimestamp(int index) throws SqlException;

    /**
     * Set type of a bind variable by index as timestamp and provide a value
     *
     * @param index numeric index of the bind variable
     * @param value as long
     * @throws SqlException
     */
    void setTimestamp(int index, long value) throws SqlException;

    /**
     * Set type of a bind variable by name as timestamp and provide a value
     *
     * @param name of the bind variable
     * @param value as long
     * @throws SqlException
     */
    void setTimestamp(CharSequence name, long value) throws SqlException;

    /**
     * Set type of a bind variable by index as binary
     *
     * @param index numeric index of the bind variable
     * @throws SqlException
     */
    void setByte(int index) throws SqlException;
}
