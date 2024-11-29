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

package io.questdb.std;

import io.questdb.std.bytes.DirectSequence;

/**
 * A flyweight to an immutable int slice stored in native memory.
 */
public class DirectIntSlice implements DirectSequence {
    private long ptr = 0;
    private int size = 0;

    public int get(int index) {
        return Unsafe.getUnsafe().getInt(ptr + ((long) index << 2));
    }

    public int length() {
        return size() / Integer.BYTES;
    }

    public DirectIntSlice of(long ptr, int size) {
        assert ptr > 0;
        assert size > 0;
        assert size % Integer.BYTES == 0;
        this.ptr = ptr;
        this.size = size;
        return this;
    }

    @Override
    public long ptr() {
        assert ptr != 0;
        return ptr;
    }

    public void reset() {
        ptr = 0;
        size = 0;
    }

    @Override
    public int size() {
        assert size >= 0;
        return size;
    }
}
