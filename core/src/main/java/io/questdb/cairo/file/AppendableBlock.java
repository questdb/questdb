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
import org.jetbrains.annotations.Nullable;

/**
 * Interface for appending various types of data to a memory block.
 * The block is extendable, meaning it can grow as more data is appended.
 * <p>
 * For a pre-allocated block with random access API, see {@link WritableBlock}.
 */
public interface AppendableBlock {

    void commit(int type);

    int length();

    long putBin(BinarySequence value);

    void putBool(boolean value);

    void putByte(byte value);

    void putChar(char value);

    void putDouble(double value);

    void putFloat(float value);

    void putInt(int value);

    void putLong(long value);

    void putShort(short value);

    long putStr(CharSequence value);

    long putVarchar(@Nullable Utf8Sequence value);

    void skip(long bytes);
}
