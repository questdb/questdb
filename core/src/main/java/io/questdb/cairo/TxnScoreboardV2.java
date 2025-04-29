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
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.TestOnly;

/**
 * In-memory transaction scoreboard. Each table reader mutates its own
 * slot based on the assigned id. Cross-reader operations require all slots
 * to be checked. On the other hand, single-reader operations are cheap.
 */
public class TxnScoreboardV2 implements TxnScoreboard {
    private static final int VIRTUAL_ID_COUNT = 1;
    private static final int RESERVED_ID_COUNT = VIRTUAL_ID_COUNT + 3;
    private static final long UNLOCKED = -1;
    private final int entryScanCount;
    private final int pow2EntryCount;
    private long activeReaderCountMem;
    private long entriesMem;
    private long maxMem;
    private long maxReaderIdMem;
    // Record structure
    // 8 bytes - active reader count
    // 8 bytes - max txn
    // 8 bytes - max id
    // 8 bytes - slot for CHECKPOINT lock
    // N * 8 bytes - slots for every TableReader
    private long mem;

    private TableToken tableToken;

    public TxnScoreboardV2(int entryCount) {
        pow2EntryCount = entryCount + RESERVED_ID_COUNT;
        entryScanCount = entryCount + VIRTUAL_ID_COUNT;
        mem = Unsafe.malloc((long) pow2EntryCount * Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
        activeReaderCountMem = mem;
        maxMem = mem + Long.BYTES;
        maxReaderIdMem = maxMem + Long.BYTES;
        entriesMem = mem + (long) (RESERVED_ID_COUNT - VIRTUAL_ID_COUNT) * Long.BYTES;
        Vect.memset(mem, (long) pow2EntryCount * Long.BYTES, -1);
        // Set max, reader count to 0.
        Vect.memset(mem, 3 * Long.BYTES, 0);
    }

    @Override
    public boolean acquireTxn(int id, long txn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;
        if (!updateMax(txn)) {
            return false;
        }

        if (Unsafe.getUnsafe().compareAndSwapLong(null, entriesMem + internalId * Long.BYTES, UNLOCKED, txn)) {
            updateMaxReaderId(internalId);
            incrementActiveReaderCount();
            if (getMax() > txn) {
                // Max moved, cannot acquire the txn.
                Unsafe.getUnsafe().putLongVolatile(null, entriesMem + internalId * Long.BYTES, UNLOCKED);
                Unsafe.getUnsafe().getAndAddLong(null, activeReaderCountMem, -1);
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        mem = Unsafe.free(mem, (long) pow2EntryCount * Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
        entriesMem = 0;
        maxMem = 0;
        maxReaderIdMem = 0;
        activeReaderCountMem = 0;
    }

    @TestOnly
    public long getActiveReaderCount(long txn) {
        if (getActiveReaderCount() == 0) {
            return 0;
        }

        long count = 0;
        for (long p = entriesMem, lim = entriesMem + (long) entryScanCount * Long.BYTES; p < lim; p += Long.BYTES) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, p);
            if (lockedTxn == txn) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int getEntryCount() {
        return entryScanCount;
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
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public boolean hasEarlierTxnLocks(long txn) {
        // Push max txn to the latest, but don't stop checking if it's not the max.
        updateMax(txn);

        if (getActiveReaderCount() == 0) {
            return false;
        }

        long scanCount = getMaxReaderId();
        for (int i = 0; i < scanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn > UNLOCKED && lockedTxn < txn) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean incrementTxn(int id, long txn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;
        updateMaxReaderId(internalId);
        if (Unsafe.getUnsafe().compareAndSwapLong(null, entriesMem + internalId * Long.BYTES, UNLOCKED, txn)) {
            // It's ok to increment readers after CAS
            // there must be another reader already that holds the same transaction lock.
            incrementActiveReaderCount();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isMax(long txn) {
        return txn >= getMax();
    }

    @Override
    public boolean isRangeAvailable(long fromTxn, long toTxn) {
        if (getActiveReaderCount() == 0) {
            return true;
        }

        long scanCount = getMaxReaderId();
        for (int i = 0; i < scanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn > UNLOCKED && lockedTxn >= fromTxn && lockedTxn < toTxn) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTxnAvailable(long txn) {
        if (getActiveReaderCount() == 0) {
            return true;
        }

        long scanCount = getMaxReaderId();
        for (int i = 0; i < scanCount; i++) {
            long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + (long) i * Long.BYTES);
            if (lockedTxn == txn) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long releaseTxn(int id, long txn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;
        long lockedTxn = Unsafe.getUnsafe().getLongVolatile(null, entriesMem + internalId * Long.BYTES);
        assert lockedTxn == txn : "Invalid release, expected " + txn + " but got " + lockedTxn;

        Unsafe.getUnsafe().putLongVolatile(null, entriesMem + internalId * Long.BYTES, UNLOCKED);
        Unsafe.getUnsafe().getAndAddLong(null, activeReaderCountMem, -1);
        // It's too expensive to count how many readers are left for the txn, caller will have to do additional checks.
        return 0;
    }

    public void setTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private static int toInternalId(int id) {
        return id + VIRTUAL_ID_COUNT;
    }

    private long getActiveReaderCount() {
        return Unsafe.getUnsafe().getLongVolatile(null, activeReaderCountMem);
    }

    private long getMax() {
        return Unsafe.getUnsafe().getLongVolatile(null, maxMem);
    }

    private long getMaxReaderId() {
        // Max reader Id does not go down, only up.
        // This can in rare cases result in performance hit in calls to isRangeAvailable(), hasEarlierTxnLocks()
        // This problem to be addressed in future PRs
        return Unsafe.getUnsafe().getLongVolatile(null, maxReaderIdMem) + 1;
    }

    private void incrementActiveReaderCount() {
        Unsafe.getUnsafe().getAndAddLong(null, activeReaderCountMem, 1);
    }

    private boolean updateMax(long txn) {
        long max;
        do {
            max = Unsafe.getUnsafe().getLongVolatile(null, maxMem);
            if (txn < max) {
                return false;
            }
        }
        while (txn > max && !Unsafe.getUnsafe().compareAndSwapLong(null, maxMem, max, txn));
        return true;
    }

    private void updateMaxReaderId(long internalId) {
        long val;
        // Update max reader id seen so far.
        do {
            val = Unsafe.getUnsafe().getLongVolatile(null, maxReaderIdMem);
        } while (val < internalId && !Unsafe.getUnsafe().compareAndSwapLong(null, maxReaderIdMem, val, internalId));
    }
}
