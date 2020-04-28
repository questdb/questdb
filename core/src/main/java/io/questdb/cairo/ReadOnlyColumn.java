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

package io.questdb.cairo;

import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public interface ReadOnlyColumn extends Closeable {

    @Override
    void close();

    void of(FilesFacade ff, LPSZ name, long pageSize, long size);

    BinarySequence getBin(long offset);

    long getBinLen(long offset);

    boolean getBool(long offset);

    byte getByte(long offset);

    double getDouble(long offset);

    long getFd();

    float getFloat(long offset);

    int getInt(long offset);

    long getLong(long offset);

    BinarySequence getRawBytes(long offset, int len);

    short getShort(long offset);

    CharSequence getStr(long offset);

    CharSequence getStr2(long offset);

    Long256 getLong256A(long offset);

    void getLong256(long offset, CharSink sink);

    Long256 getLong256B(long offset);

    char getChar(long offset);

    int getStrLen(long offset);

    void grow(long size);

    boolean isDeleted();

    int getPageCount();

    long getPageSize(int pageIndex);

    long getPageAddress(int pageIndex);
}
