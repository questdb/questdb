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

package io.questdb.cairo.mv;

import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8SplitString;


public interface ReadableBlock {
    long addressOf(long offset);

    byte flags();

    BinarySequence getBin(long offset);

    long getBinLen(long offset);

    boolean getBool(long offset);

    byte getByte(long offset);

    char getChar(long offset);

    double getDouble(long offset);

    float getFloat(long offset);

    int getIPv4(long offset);

    int getInt(long offset);

    long getLong(long offset);

    void getLong256(long offset, CharSink<?> sink);

    void getLong256(long offset, Long256Acceptor sink);

    Long256 getLong256(long offset);

    short getShort(long offset);

    Utf8SplitString getSplitVarchar(long auxLo, long dataLo, long dataLim, int size, boolean ascii);

    CharSequence getStr(long offset);

    int getStrLen(long offset);

    DirectUtf8Sequence getVarchar(long offset, int size, boolean ascii);

    long length();

    short type();

    byte version();
}
