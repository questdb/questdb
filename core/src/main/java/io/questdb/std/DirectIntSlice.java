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

package io.questdb.std;

import io.questdb.std.bytes.DirectSequence;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.TestOnly;

/**
 * A flyweight to an immutable int slice stored in native memory.
 */
public class DirectIntSlice implements DirectSequence {
    private int length = 0;
    private long ptr = 0;

    public int get(int index) {
        assert index >= 0;
        assert index < length;
        return Unsafe.getUnsafe().getInt(ptr + ((long) index << 2));
    }

    public int length() {
        return length;
    }

    /**
     * Set from start address and length (element count).
     */
    public DirectIntSlice of(long ptr, int length) {
        assert ptr >= 0;
        assert length >= 0;
        assert (ptr != 0) || (length == 0);
        this.ptr = ptr;
        this.length = length;
        return this;
    }

    @Override
    public long ptr() {
        assert ptr != 0;
        return ptr;
    }

    public void reset() {
        ptr = 0;
        length = 0;
    }

    @Override
    public int size() {
        assert length >= 0;
        return length * Integer.BYTES;
    }

    public void shl(long delta) {
        this.ptr -= delta;
    }

    @TestOnly
    public int[] toArray() {
        final int[] res = new int[length];
        for (int i = 0; i < length; ++i) {
            res[i] = get(i);
        }
        return res;
    }

    @Override
    public String toString() {
        Utf16Sink sb = Misc.getThreadLocalSink();
        sb.put('[');
        final int maxElementsToPrint = 1000; // Do not try to print too much, it can hang IntelliJ debugger.
        for (int i = 0, n = Math.min(maxElementsToPrint, length); i < n; i++) {
            if (i > 0) {
                sb.put(',').put(' ');
            }
            sb.put(get(i));
        }
        if (size() > maxElementsToPrint) {
            sb.put(", .. ");
        }
        sb.put(']');
        return sb.toString();
    }
}
