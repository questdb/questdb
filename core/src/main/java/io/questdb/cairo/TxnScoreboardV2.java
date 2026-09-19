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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.TestOnly;

/**
 * In-memory transaction scoreboard. Each table reader mutates its own
 * slot based on the assigned id. Cross-reader operations require all slots
 * to be checked. On the other hand, single-reader operations are inexpensive.
 */
public class TxnScoreboardV2 implements TxnScoreboard {
    private static final long RELEASING = -2;
    private static final long UNLOCKED = -1;
    private static final int VIRTUAL_ID_COUNT = 1;
    private final int bitmapCount;
    private final int entryScanCount;
    private final int memSize;
    private long activeReaderCountMem;
    private long bitmapMem;
    private long entriesMem;
    private long maxMem;
    private long seqTxnEntriesMem;
    // Record structure
    // 8 bytes - active reader count
    // 8 bytes - max txn
    // ceil[(N + 1) / 64] * 8 bytes - bitmap index
    // 8 bytes - slot for CHECKPOINT txn
    // N * 8 bytes - slots for every TableReader table txn
    // N * 8 bytes - slots for every TableReader seqTxn
    private long mem;

    private TableToken tableToken;

    public TxnScoreboardV2(int entryCount) {
        entryScanCount = entryCount + VIRTUAL_ID_COUNT;
        long bitMapSize = (entryScanCount + Long.SIZE - 1) & -Long.SIZE;
        bitmapCount = (int) (bitMapSize / Long.SIZE);
        memSize = (2 * entryScanCount + 2 + bitmapCount) * Long.BYTES;
        mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_TABLE_READER);

        activeReaderCountMem = mem;
        maxMem = mem + Long.BYTES;
        bitmapMem = maxMem + Long.BYTES;
        entriesMem = bitmapMem + bitmapCount * Long.BYTES;
        seqTxnEntriesMem = entriesMem + (long) entryScanCount * Long.BYTES;

        Vect.memset(entriesMem, (long) entryScanCount * Long.BYTES, -1);
        Vect.memset(seqTxnEntriesMem, (long) entryScanCount * Long.BYTES, -1);
        // Set max, reader count and bitmap index to 0.
        Vect.memset(mem, (2 + bitmapCount) * Long.BYTES, 0);
    }

    @Override
    public boolean acquireTxn(int id, long tableTxn, long seqTxn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;
        if (!updateMax(tableTxn)) {
            return false;
        }

        final long entryAddress = entriesMem + internalId * Long.BYTES;
        if (Unsafe.cas(null, entryAddress, UNLOCKED, tableTxn)) {
            Unsafe.putLongVolatile(seqTxnEntriesMem + internalId * Long.BYTES, seqTxn);
            incrementActiveReaderCount();
            setBitmapBit(internalId);
            if (getMax() > tableTxn) {
                // Max moved, cannot acquire the txn.
                clearBitmapBit(internalId);
                Unsafe.putLongVolatile(seqTxnEntriesMem + internalId * Long.BYTES, UNKNOWN_SEQ_TXN);
                Unsafe.putLongVolatile(entryAddress, UNLOCKED);
                Unsafe.getAndAddLong(null, activeReaderCountMem, -1);
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        mem = Unsafe.free(mem, memSize, MemoryTag.NATIVE_TABLE_READER);
        entriesMem = 0;
        maxMem = 0;
        activeReaderCountMem = 0;
        bitmapMem = 0;
        seqTxnEntriesMem = 0;
    }

    @TestOnly
    public long getActiveReaderCount(long txn) {
        if (getActiveReaderCount() == 0) {
            return 0;
        }

        long count = 0;
        for (int i = 0; i < bitmapCount; i++) {
            long bitmap = Unsafe.getLongVolatile(bitmapMem + (long) i * Long.BYTES);
            if (bitmap == 0) {
                continue;
            }

            int base = i * Long.SIZE;
            while (bitmap != 0) {
                final long lowestBit = Long.lowestOneBit(bitmap);
                final int bit = Long.numberOfTrailingZeros(lowestBit);
                int internalId = base + bit;
                long lockedTxn = Unsafe.getLongVolatile(entriesMem + (long) internalId * Long.BYTES);
                if (lockedTxn == txn) {
                    count++;
                }
                bitmap ^= lowestBit;
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
        for (int i = 0; i < bitmapCount; i++) {
            long bitmap = Unsafe.getLongVolatile(bitmapMem + (long) i * Long.BYTES);
            if (bitmap == 0) {
                continue;
            }

            int base = i * Long.SIZE;
            while (bitmap != 0) {
                final long lowestBit = Long.lowestOneBit(bitmap);
                final int bit = Long.numberOfTrailingZeros(lowestBit);
                int internalId = base + bit;
                long lockedTxn = Unsafe.getLongVolatile(entriesMem + (long) internalId * Long.BYTES);
                if (lockedTxn > UNLOCKED) {
                    min = Math.min(min, lockedTxn);
                }
                bitmap ^= lowestBit;
            }
        }
        return min == Long.MAX_VALUE ? UNLOCKED : min;
    }

    @Override
    public long getMinSeqTxn(long currentTableTxn, long currentSeqTxn) {
        if (!updateMax(currentTableTxn) || currentSeqTxn < 0) {
            return UNKNOWN_SEQ_TXN;
        }

        // _txn publishes tableTxn and seqTxn in one stable view, and seqTxn does not decrease
        // as tableTxn advances. A reader admitted after updateMax() therefore cannot lower this
        // floor. Reader-pool copies keep their source reader at the copied snapshot until they close.
        // Callers must defer reclamation during a checkpoint, which may close its source reader.
        long min = currentSeqTxn;
        for (int i = 0; i < bitmapCount; i++) {
            long bitmap = Unsafe.getLongVolatile(bitmapMem + (long) i * Long.BYTES);
            if (bitmap == 0) {
                continue;
            }

            int base = i * Long.SIZE;
            while (bitmap != 0) {
                final long lowestBit = Long.lowestOneBit(bitmap);
                final int bit = Long.numberOfTrailingZeros(lowestBit);
                final long seqTxn = readSeqTxn(base + bit);
                if (seqTxn == UNKNOWN_SEQ_TXN) {
                    return UNKNOWN_SEQ_TXN;
                }
                if (seqTxn != Long.MAX_VALUE) {
                    min = Math.min(min, seqTxn);
                }
                bitmap ^= lowestBit;
            }
        }
        return min;
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

        for (int i = 0; i < bitmapCount; i++) {
            long bitmap = Unsafe.getLongVolatile(bitmapMem + (long) i * Long.BYTES);
            if (bitmap == 0) {
                continue;
            }

            int base = i * Long.SIZE;
            while (bitmap != 0) {
                final long lowestBit = Long.lowestOneBit(bitmap);
                final int bit = Long.numberOfTrailingZeros(lowestBit);
                int internalId = base + bit;
                long lockedTxn = Unsafe.getLongVolatile(entriesMem + (long) internalId * Long.BYTES);
                if (lockedTxn > UNLOCKED && lockedTxn < txn) {
                    return true;
                }

                bitmap ^= lowestBit;
            }
        }
        return false;
    }

    @Override
    public boolean incrementTxn(int id, long tableTxn, long seqTxn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;
        if (Unsafe.cas(null, entriesMem + internalId * Long.BYTES, UNLOCKED, tableTxn)) {
            Unsafe.putLongVolatile(seqTxnEntriesMem + internalId * Long.BYTES, seqTxn);
            // It's ok to increment readers after CAS
            // there must be another reader already that holds the same transaction lock.
            incrementActiveReaderCount();
            setBitmapBit(internalId);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isOutdated(long txn) {
        return txn < getMax();
    }

    @Override
    public boolean isRangeAvailable(long fromTxn, long toTxn) {
        // Push max txn to the latest, to avoid races with acquireTxn()
        // but don't stop checking if it's not the max.
        updateMax(toTxn);

        if (getActiveReaderCount() == 0) {
            return true;
        }

        for (int i = 0; i < bitmapCount; i++) {
            long bitmap = Unsafe.getLongVolatile(bitmapMem + (long) i * Long.BYTES);
            if (bitmap == 0) {
                continue;
            }

            int base = i * Long.SIZE;
            while (bitmap != 0) {
                final long lowestBit = Long.lowestOneBit(bitmap);
                final int bit = Long.numberOfTrailingZeros(lowestBit);
                int internalId = base + bit;
                long lockedTxn = Unsafe.getLongVolatile(entriesMem + (long) internalId * Long.BYTES);
                if (lockedTxn > UNLOCKED && lockedTxn >= fromTxn && lockedTxn < toTxn) {
                    return false;
                }

                bitmap ^= lowestBit;
            }
        }
        return true;
    }

    @Override
    public boolean isTxnAvailable(long txn) {
        // This call should only be used from TableReader
        // when maxTxn is already initialized.
        // Check that the txn we are scanning for is not the max, to avoid races with acquireTxn()
        if (getMax() <= txn) {
            return false;
        }

        if (getActiveReaderCount() == 0) {
            return true;
        }

        for (int i = 0; i < bitmapCount; i++) {
            long bitmap = Unsafe.getLongVolatile(bitmapMem + (long) i * Long.BYTES);
            if (bitmap == 0) {
                continue;
            }

            int base = i * Long.SIZE;
            while (bitmap != 0) {
                final long lowestBit = Long.lowestOneBit(bitmap);
                final int bit = Long.numberOfTrailingZeros(lowestBit);
                int internalId = base + bit;
                long lockedTxn = Unsafe.getLongVolatile(entriesMem + (long) internalId * Long.BYTES);
                if (lockedTxn == txn) {
                    return false;
                }

                bitmap ^= lowestBit;
            }
        }
        return true;
    }

    @Override
    public long releaseTxn(int id, long txn) {
        long internalId = toInternalId(id);
        assert internalId < entryScanCount;

        final long entryAddress = entriesMem + internalId * Long.BYTES;
        if (Unsafe.cas(null, entryAddress, txn, RELEASING)) {
            clearBitmapBit(internalId);
            Unsafe.putLongVolatile(seqTxnEntriesMem + internalId * Long.BYTES, UNKNOWN_SEQ_TXN);
            Unsafe.putLongVolatile(entryAddress, UNLOCKED);
            Unsafe.getAndAddLong(null, activeReaderCountMem, -1);
        } else {
            long lockedTxn;
            //noinspection AssertWithSideEffects
            assert (lockedTxn = Unsafe.getLongVolatile(entriesMem + internalId * Long.BYTES)) == UNLOCKED
                    : "Invalid scoreboard release, expected " + txn + " but got " + lockedTxn;
        }
        // It's too expensive to count how many readers are left for the txn, caller will have to do additional checks.
        return 0;
    }

    public void setTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private static int toInternalId(int id) {
        return id + VIRTUAL_ID_COUNT;
    }

    private void clearBitmapBit(long internalId) {
        final long offset = bitmapMem + (internalId >>> 6) * Long.BYTES;
        final long mask = ~(1L << (internalId & 0x3F));
        long l;
        do {
            l = Unsafe.getLongVolatile(offset);
        } while ((l & mask) != l &&
                !Unsafe.cas(null, offset, l, l & mask));
    }

    private long getActiveReaderCount() {
        return Unsafe.getLongVolatile(activeReaderCountMem);
    }

    private long getMax() {
        return Unsafe.getLongVolatile(maxMem);
    }

    private void incrementActiveReaderCount() {
        Unsafe.getAndAddLong(null, activeReaderCountMem, 1);
    }

    private long readSeqTxn(long internalId) {
        final long bitmapAddress = bitmapMem + (internalId >>> 6) * Long.BYTES;
        final long mask = 1L << (internalId & 0x3F);
        if ((Unsafe.getLongVolatile(bitmapAddress) & mask) == 0) {
            return Long.MAX_VALUE;
        }
        return Unsafe.getLongVolatile(seqTxnEntriesMem + internalId * Long.BYTES);
    }

    private void setBitmapBit(long internalId) {
        final long offset = bitmapMem + (internalId >>> 6) * Long.BYTES;
        final long mask = 1L << (internalId & 0x3F);
        long b;
        do {
            b = Unsafe.getLongVolatile(offset);
        } while ((b & mask) == 0 &&
                !Unsafe.cas(null, offset, b, b | mask));
    }

    private boolean updateMax(long txn) {
        long max;
        do {
            max = Unsafe.getLongVolatile(maxMem);
            if (txn < max) {
                return false;
            }
        }
        while (txn > max && !Unsafe.cas(null, maxMem, max, txn));
        return true;
    }
}
