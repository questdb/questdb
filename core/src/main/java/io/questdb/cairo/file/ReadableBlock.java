/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.file;

import io.questdb.std.BinarySequence;
import io.questdb.std.str.Utf8Sequence;


/**
 * Interface for reading various types of data from a memory block.
 * Provides a random access API where the offset is relative to the block start.
 * The size of the block can be obtained via the {@link #length()} method.
 */
public interface ReadableBlock {

    long addressOf(long offset);

    BinarySequence getBin(long offset);

    boolean getBool(long offset);

    byte getByte(long offset);

    char getChar(long offset);

    double getDouble(long offset);

    float getFloat(long offset);

    int getInt(long offset);

    long getLong(long offset);

    short getShort(long offset);

    CharSequence getStr(long offset);

    Utf8Sequence getVarchar(long offset);

    long length();

    int type();
}
