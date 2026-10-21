/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.TestOnly;

/**
 * Native copy of the offsets and chars of the first {@link #MAX_KEYS} keys of one dictionary
 * version, guarded by the owning entry's lock. Bytes below a published key count never change
 * and retired blocks stay allocated until close, so bound readers read without the lock.
 */
final class SymbolMapLowSlice implements QuietCloseable {
    static final int MAX_KEYS = 4096;
    private static final int CHARS_BASE_OFFSET = 8;
    private static final int CHARS_CAPACITY_OFFSET = 16;
    private static final int CHARS_USED_OFFSET = 24;
    private static final int HEADER_SIZE = 40;
    private static final int KEY_CAPACITY_OFFSET = 0;
    private static final int KEY_COUNT_OFFSET = 4;
    private static final int RETIRED_BLOCK_OFFSET = 32;
    private long block;

    @TestOnly
    public long allocatedBytes() {
        long bytes = 0;
        for (long b = block; b != 0; b = Unsafe.getLong(b + RETIRED_BLOCK_OFFSET)) {
            bytes += blockSize(Unsafe.getInt(b + KEY_CAPACITY_OFFSET), Unsafe.getLong(b + CHARS_CAPACITY_OFFSET));
        }
        return bytes;
    }

    public void bind(SymbolMapReaderImpl reader, int want) {
        if (block == 0) {
            reader.bindLow(0, 0, 0, 0);
        } else {
            long offsets = block + HEADER_SIZE;
            reader.bindLow(offsets, offsets + (long) Unsafe.getInt(block + KEY_CAPACITY_OFFSET) * Long.BYTES,
                    Unsafe.getLong(block + CHARS_BASE_OFFSET), Math.min(want, Unsafe.getInt(block + KEY_COUNT_OFFSET)));
        }
    }

    @Override
    public void close() {
        long b = block;
        block = 0;
        while (b != 0) {
            long next = Unsafe.getLong(b + RETIRED_BLOCK_OFFSET);
            Unsafe.free(
                    b,
                    blockSize(Unsafe.getInt(b + KEY_CAPACITY_OFFSET), Unsafe.getLong(b + CHARS_CAPACITY_OFFSET)),
                    MemoryTag.NATIVE_SYMBOL_CACHE
            );
            b = next;
        }
    }

    public boolean ensure(MemoryR offsetMem, MemoryR charMem, int want) {
        assert want >= 0 && want <= MAX_KEYS;
        long b = block;
        int count = b == 0 ? 0 : Unsafe.getInt(b + KEY_COUNT_OFFSET);
        if (want <= count) {
            return true;
        }
        long base = offsetMem.getLong(SymbolMapWriter.keyToOffset(0));
        long charsNeeded = offsetMem.getLong(SymbolMapWriter.keyToOffset(want)) - base;
        int keyCapacity = b == 0 ? 0 : Unsafe.getInt(b + KEY_CAPACITY_OFFSET);
        long charCapacity = b == 0 ? 0 : Unsafe.getLong(b + CHARS_CAPACITY_OFFSET);
        if (want <= keyCapacity && charsNeeded <= charCapacity) {
            append(b, count, want, charsNeeded, offsetMem, charMem);
            return true;
        }
        int newKeyCapacity = want > keyCapacity ? Math.min(MAX_KEYS, Math.max(want, 2 * keyCapacity)) : keyCapacity;
        long newCharCapacity = charsNeeded > charCapacity ? Math.max(charsNeeded, 2 * charCapacity) : charCapacity;
        long bytes = blockSize(newKeyCapacity, newCharCapacity);
        long next;
        try {
            next = Unsafe.malloc(bytes, MemoryTag.NATIVE_SYMBOL_CACHE);
        } catch (CairoException e) {
            if (e.isOutOfMemory()) {
                return false;
            }
            throw e;
        }
        try {
            long charsUsed = b == 0 ? 0 : Unsafe.getLong(b + CHARS_USED_OFFSET);
            Unsafe.putInt(next + KEY_CAPACITY_OFFSET, newKeyCapacity);
            Unsafe.putInt(next + KEY_COUNT_OFFSET, count);
            Unsafe.putLong(next + CHARS_BASE_OFFSET, base);
            Unsafe.putLong(next + CHARS_CAPACITY_OFFSET, newCharCapacity);
            Unsafe.putLong(next + CHARS_USED_OFFSET, charsUsed);
            Unsafe.putLong(next + RETIRED_BLOCK_OFFSET, b);
            if (b != 0) {
                Vect.memcpy(next + HEADER_SIZE, b + HEADER_SIZE, (long) count * Long.BYTES);
                Vect.memcpy(next + HEADER_SIZE + (long) newKeyCapacity * Long.BYTES,
                        b + HEADER_SIZE + (long) keyCapacity * Long.BYTES, charsUsed);
            }
            append(next, count, want, charsNeeded, offsetMem, charMem);
            block = next;
        } catch (Throwable e) {
            Unsafe.free(next, bytes, MemoryTag.NATIVE_SYMBOL_CACHE);
            throw e;
        }
        return true;
    }

    private static void append(long b, int count, int want, long charsNeeded, MemoryR offsetMem, MemoryR charMem) {
        long offsets = b + HEADER_SIZE;
        long chars = offsets + (long) Unsafe.getInt(b + KEY_CAPACITY_OFFSET) * Long.BYTES;
        long charsUsed = Unsafe.getLong(b + CHARS_USED_OFFSET);
        Vect.memcpy(offsets + (long) count * Long.BYTES,
                offsetMem.addressOf(SymbolMapWriter.keyToOffset(count)), (long) (want - count) * Long.BYTES);
        Vect.memcpy(chars + charsUsed, charMem.addressOf(Unsafe.getLong(b + CHARS_BASE_OFFSET) + charsUsed), charsNeeded - charsUsed);
        Unsafe.putLong(b + CHARS_USED_OFFSET, charsNeeded);
        Unsafe.putInt(b + KEY_COUNT_OFFSET, want);
    }

    private static long blockSize(int keyCapacity, long charCapacity) {
        return HEADER_SIZE + (long) keyCapacity * Long.BYTES + charCapacity;
    }
}
