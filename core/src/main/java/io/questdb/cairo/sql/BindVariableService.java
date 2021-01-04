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

public interface BindVariableService extends Mutable {

    int define(int variableIndex, int type, int position) throws SqlException;

    Function getFunction(CharSequence name);

    Function getFunction(int index);

    int getIndexedVariableCount();

    void setBin(CharSequence name, BinarySequence value) throws SqlException;

    void setBin(int index) throws SqlException;

    void setBin(int index, BinarySequence value) throws SqlException;

    void setBoolean(CharSequence name, boolean value) throws SqlException;

    void setBoolean(int index) throws SqlException;

    void setBoolean(int index, boolean value) throws SqlException;

    void setByte(CharSequence name, byte value) throws SqlException;

    void setByte(int index, byte value) throws SqlException;

    void setChar(CharSequence name, char value) throws SqlException;

    void setChar(int index) throws SqlException;

    void setChar(int index, char value) throws SqlException;

    void setDate(CharSequence name, long value) throws SqlException;

    void setDate(int index) throws SqlException;

    void setDate(int index, long value) throws SqlException;

    void setDouble(CharSequence name, double value) throws SqlException;

    void setDouble(int index) throws SqlException;

    void setDouble(int index, double value) throws SqlException;

    void setFloat(CharSequence name, float value) throws SqlException;

    void setFloat(int index) throws SqlException;

    void setFloat(int index, float value) throws SqlException;

    void setInt(CharSequence name, int value) throws SqlException;

    void setInt(int index) throws SqlException;

    void setInt(int index, int value) throws SqlException;

    void setLong(CharSequence name, long value) throws SqlException;

    void setLong(int index) throws SqlException;

    void setLong(int index, long value) throws SqlException;

    void setLong256(CharSequence name, long l0, long l1, long l2, long l3) throws SqlException;

    void setLong256(CharSequence name, Long256 value) throws SqlException;

    void setLong256(int index) throws SqlException;

    void setLong256(int index, long l0, long l1, long l2, long l3) throws SqlException;

    void setLong256(CharSequence name) throws SqlException;

    void setShort(int index) throws SqlException;

    void setShort(int index, short value) throws SqlException;

    void setShort(CharSequence name, short value) throws SqlException;

    void setStr(int index) throws SqlException;

    void setStr(int index, CharSequence value) throws SqlException;

    void setStr(CharSequence name, CharSequence value) throws SqlException;

    void setTimestamp(int index) throws SqlException;

    void setTimestamp(int index, long value) throws SqlException;

    void setTimestamp(CharSequence name, long value) throws SqlException;

    void setByte(int index) throws SqlException;
}
