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

import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Mutable;

public interface BindVariableService extends Mutable {

    void define(int variableIndex, int type);

    Function getFunction(CharSequence name);

    Function getFunction(int index);

    int getIndexedVariableCount();

    void setBin(CharSequence name, BinarySequence value);

    void setBin(int index);

    void setBin(int index, BinarySequence value);

    void setBoolean(CharSequence name, boolean value);

    void setBoolean(int index);

    void setBoolean(int index, boolean value);

    void setByte(CharSequence name, byte value);

    void setByte(int index, byte value);

    void setChar(CharSequence name, char value);

    void setChar(int index);

    void setChar(int index, char value);

    void setDate(CharSequence name, long value);

    void setDate(int index);

    void setDate(int index, long value);

    void setDouble(CharSequence name, double value);

    void setDouble(int index);

    void setDouble(int index, double value);

    void setFloat(CharSequence name, float value);

    void setFloat(int index);

    void setFloat(int index, float value);

    void setInt(CharSequence name, int value);

    void setInt(int index);

    void setInt(int index, int value);

    void setLong(CharSequence name, long value);

    void setLong(int index);

    void setLong(int index, long value);

    void setLong256(CharSequence name, long l0, long l1, long l2, long l3);

    void setLong256(CharSequence name, Long256 value);

    void setLong256(int index);

    void setLong256(int index, long l0, long l1, long l2, long l3);

    void setLong256Null(CharSequence name);

    void setShort(int index);

    void setShort(int index, short value);

    void setShort(CharSequence name, short value);

    void setStr(int index);

    void setStr(int index, CharSequence value);

    void setStr(CharSequence name, CharSequence value);

    void setTimestamp(int index);

    void setTimestamp(int index, long value);

    void setTimestamp(CharSequence name, long value);

    void setByte(int index);
}
