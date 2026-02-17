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
 * Interface for writing various types of data to a pre-allocated memory block.
 * The block has a fixed size and provides a random access API.
 * The offset is relative to the block start.
 * <p>
 * For an extendable block with append API, see {@link AppendableBlock}.
 */
public interface WritableBlock {

    void commit(int type);

    int length();

    void putBin(long offset, BinarySequence value);

    void putBool(long offset, boolean value);

    void putByte(long offset, byte value);

    void putChar(long offset, char value);

    void putDouble(long offset, double value);

    void putFloat(long offset, float value);

    void putInt(long offset, int value);

    void putLong(long offset, long value);

    void putShort(long offset, short value);

    void putStr(long offset, CharSequence value);

    void putVarchar(long offset, @Nullable Utf8Sequence value);
}
