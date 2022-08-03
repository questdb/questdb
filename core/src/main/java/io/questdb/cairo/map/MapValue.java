/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.map;

import io.questdb.cairo.sql.Record;
import io.questdb.std.Long256;

public interface MapValue extends Record {

    long getAddress();

    boolean getBool(int index);

    byte getByte(int index);

    long getDate(int index);

    double getDouble(int index);

    float getFloat(int index);

    char getChar(int index);

    CharSequence getStr(int index);

    int getInt(int index);

    long getLong(int index);

    short getShort(int index);

    long getTimestamp(int index);

    boolean isNew();

    void putBool(int index, boolean value);

    void putByte(int index, byte value);

    void addByte(int index, byte value);

    void putDate(int index, long value);

    void putDouble(int index, double value);

    void addDouble(int index, double value);

    void putStr(int index, CharSequence value);

    void putFloat(int index, float value);

    void addFloat(int index, float value);

    void putInt(int index, int value);

    void addInt(int index, int value);

    void putLong(int index, long value);

    void addLong(int index, long value);

    void putShort(int index, short value);

    void addShort(int index, short value);

    void putChar(int index, char value);

    void putTimestamp(int index, long value);

    void setMapRecordHere();

    void addLong256(int index, Long256 value);

    void putLong256(int index, Long256 value);
}
