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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;

// Implements TableWriterPressureControl interface and regulates number of transactions and partitions
// TableWriter uses when applying wal transactions.
// See TableWriterPressureControl interface for more information.
public class TableWriterPressureControlImpl implements TableWriterPressureControl {
    // To be used in multiple threads, safe and values will still be random
    private static final Rnd MEM_PRESSURE_RND = new Rnd();
    private static final int PARTITION_COUNT_SCALE_DOWN_FACTOR = 4;
    private static final int PARTITION_COUNT_SCALE_UP_FACTOR = 4;
    private static final int TXN_COUNT_SCALE_DOWN_FACTOR = 4;
    private static final int TXN_COUNT_SCALE_UP_FACTOR = 1000;
    private long inflightBlockRowCount;
    private long inflightTxnCount;
    private int maxRecordedInflightPartitions = 1;
    private long maxBlockRowCount = Integer.MAX_VALUE;
    // positive int: holds max parallelism
    // negative int: holds backoff counter
    private int memoryPressureRegulationValue = Integer.MAX_VALUE;
    private long walBackoffUntilEpochMs = -1;

    public long getMaxBlockRowCount() {
        return Math.max(1, maxBlockRowCount);
    }

    public int getMemoryPressureLevel() {
        if (memoryPressureRegulationValue == Integer.MAX_VALUE) {
            return 0;
        }
        if (memoryPressureRegulationValue < 0) {
            return 2;
        }
        return 1;
    }

    public int getMemoryPressureRegulationValue() {
        return Math.max(1, memoryPressureRegulationValue);
    }

    public boolean isReadyToProcess() {
        return getTicks() > walBackoffUntilEpochMs;
    }

    @Override
    public void onApplyBlockError() {
        maxBlockRowCount = inflightBlockRowCount / TXN_COUNT_SCALE_DOWN_FACTOR;
        inflightBlockRowCount = 1;
    }

    @Override
    public boolean onEnoughMemory() {
        maxRecordedInflightPartitions = 1;
        walBackoffUntilEpochMs = -1;
        maxBlockRowCount = Math.max(maxBlockRowCount, maxBlockRowCount * TXN_COUNT_SCALE_UP_FACTOR);

        if (memoryPressureRegulationValue == Integer.MAX_VALUE) {
            // already at max parallelism, can't go more optimistic
            return false;
        }
        if (memoryPressureRegulationValue < 0) {
            // was in backoff, go back to parallelism = 1
            memoryPressureRegulationValue = 1;
            return true;
        }
        if (MEM_PRESSURE_RND.nextInt(4) == 0) { // 25% chance to double parallelism
            int beforeDoubling = memoryPressureRegulationValue;
            memoryPressureRegulationValue *= PARTITION_COUNT_SCALE_UP_FACTOR;
            if (memoryPressureRegulationValue < beforeDoubling) {
                // overflow
                memoryPressureRegulationValue = Integer.MAX_VALUE;
            }
        }
        return true;
    }

    /**
     * Applies anti-OOM measures if possible, either by reducing job parallelism, or applying backoff.<br>
     * If it was possible to apply more measures, returns true → the operation can retry.<br>
     * If all measures were exhausted, returns false → the operation should now fail.
     */
    @Override
    public void onOutOfMemory() {
        long inflightRows = inflightBlockRowCount;
        inflightBlockRowCount = maxBlockRowCount = Math.max(1, inflightRows / TXN_COUNT_SCALE_DOWN_FACTOR);

        if (maxRecordedInflightPartitions == 1 && inflightTxnCount <= 1) {
            // There was no parallelism and no multi transaction block
            if (memoryPressureRegulationValue <= -5) {
                // Maximum backoff already tried => fail
                walBackoffUntilEpochMs = -1;
                return;
            }
            if (memoryPressureRegulationValue > 0) {
                // Switch from reducing parallelism to backoff
                memoryPressureRegulationValue = -1;
            } else {
                // Increase backoff counter
                memoryPressureRegulationValue--;
            }
            int delayMillis = MEM_PRESSURE_RND.nextInt(4_000);
            walBackoffUntilEpochMs = getTicks() + delayMillis;
            return;
        }
        // There was some parallelism, halve max parallelism
        walBackoffUntilEpochMs = -1;
        memoryPressureRegulationValue = maxRecordedInflightPartitions / PARTITION_COUNT_SCALE_DOWN_FACTOR;
        maxRecordedInflightPartitions = 1;
    }

    @Override
    public void updateInflightPartitions(int count) {
        maxRecordedInflightPartitions = Math.max(maxRecordedInflightPartitions, count);
    }

    @Override
    public void setMaxBlockRowCount(int count) {
        maxBlockRowCount = count;
    }

    @Override
    public void updateInflightTxnBlockSize(long txnCount, long rowCount) {
        inflightBlockRowCount = rowCount;
        inflightTxnCount = txnCount;
    }

    private static long getTicks() {
        return MillisecondClockImpl.INSTANCE.getTicks();
    }
}
