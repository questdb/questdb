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

package io.questdb.cairo;

import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.TestOnly;

public class TxnScoreboardV2 implements TxnScoreboard {
    private static final int RESERVED_ID_COUNT = 16;
    private static final long UNLOCKED = -1;
    private static final int VIRTUAL_ID_COUNT = 1;
    private final long activeReaderCountOffset;
    private final long entriesMem;
    private final int entryScanCount;
    private final long maxOffset;
    private final int pow2EntryCount;
    private long mem;

    public TxnScoreboardV2(int entryCount) {
        pow2EntryCount = Numbers.ceilPow2(entryCount + RESERVED_ID_COUNT);
        entryScanCount = entryCount + VIRTUAL_ID_COUNT;
        mem = Unsafe.malloc((long) pow2EntryCount * Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
        maxOffset = mem + Long.BYTES;
        activeReaderCountOffset = mem;
        entriesMem = mem + (long) (RESERVED_ID_COUNT - VIRTUAL_ID_COUNT) * Long.BYTES;
        Vect.memset(mem, (long) pow2EntryCount * Long.BYTES, -1);
        // Set max, reader count to 0.
        Vect.memset(mem, 2 * Long.BYTES, 0);
    }

    public boolean acquireTxn(int id, long txn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;
        if (!updateMax(txn)) {
            return false;
        }

        if (Unsafe.getUnsafe().compareAndSwapLong(null, entriesMem + internalId * Long.BYTES, UNLOCKED, txn)) {
            if (!updateMax(txn)) {
                // Max moved, cannot acquire the txn.
                Unsafe.getUnsafe().putLongVolatile(null, entriesMem + internalId * Long.BYTES, UNLOCKED);
                return false;
            }
            incrementActiveReaderCount();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        mem = Unsafe.free(mem, (long) pow2EntryCount * Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
    }

    @TestOnly
    public long getActiveReaderCount(long txn) {
        if (getActiveReaderCount() == 0) {
            return 0;
        }

        long count = 0;
        for (int i = 0; i < entryScanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn == txn) {
                count++;
            }
        }
        return count;
    }

    public int getEntryCount() {
        return entryScanCount;
    }

    public long getMax() {
        return Unsafe.getUnsafe().getLongVolatile(null, maxOffset);
    }

    @TestOnly
    public long getMin() {
        if (getActiveReaderCount() == 0) {
            return UNLOCKED;
        }

        long min = Long.MAX_VALUE;
        for (int i = 0; i < entryScanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn > UNLOCKED) {
                min = Math.min(min, lockedTxn);
            }
        }
        return min == Long.MAX_VALUE ? UNLOCKED : min;
    }

    @Override
    public boolean hasEarlierTxnLocks(long txn) {
        if (!updateMax(txn)) {
            return true;
        }

        if (getActiveReaderCount() == 0) {
            return false;
        }

        for (int i = 0; i < entryScanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn > UNLOCKED && lockedTxn < txn) {
                return true;
            }
        }
        return false;
    }

    public boolean isRangeAvailable(long fromTxn, long toTxn) {
        if (getActiveReaderCount() == 0) {
            return true;
        }
        for (int i = 0; i < entryScanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn > UNLOCKED && lockedTxn >= fromTxn && lockedTxn < toTxn) {
                return false;
            }
        }
        return true;
    }

    public boolean isTxnAvailable(long txn) {
        if (getActiveReaderCount() == 0) {
            return true;
        }
        for (int i = 0; i < entryScanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn == txn) {
                return false;
            }
        }
        return true;
    }

    public long releaseTxn(int id, long txn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;
        Unsafe.getUnsafe().putLongVolatile(null, entriesMem + internalId * Long.BYTES, UNLOCKED);
        return 0;
    }

    private static int toInternalId(int id) {
        return id + VIRTUAL_ID_COUNT;
    }

    private long getActiveReaderCount() {
        return Unsafe.getUnsafe().getLongVolatile(null, activeReaderCountOffset);
    }

    private void incrementActiveReaderCount() {
        long count;
        do {
            count = Unsafe.getUnsafe().getLong(activeReaderCountOffset);
        } while (!Unsafe.getUnsafe().compareAndSwapLong(null, activeReaderCountOffset, count, Math.min(0, count) + 1));
    }

    private boolean updateMax(long txn) {
        long max;
        do {
            max = Unsafe.getUnsafe().getLongVolatile(null, maxOffset);
            if (txn < max) {
                return false;
            }
        }
        while (txn > max && !Unsafe.getUnsafe().compareAndSwapLong(null, maxOffset, max, txn));
        return true;
    }
}
