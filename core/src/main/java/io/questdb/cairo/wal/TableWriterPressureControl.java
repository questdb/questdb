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

package io.questdb.cairo.wal;

// This regulates how much memory TableWriter uses when applying wal transactions.
// There are 2 ways to limit the memory usage:
// 1. Limit the number of transactions it can apply in a single transaction block.
// 2. Limit the number of parallel partitions it can apply the transactions to.
//
// On Out Of Memory error, onOutOfMemory method is called and it checks how much transactions/partitions are inflight
// and reduces it for the next run. If the values are already reduced to minimum (value of 1), then it is
// paused for a random time before next wal application runs.
//
// If the Out Of Memory error is followed by successful wal application, then `onEnoughMemory` method is called
// and it increases the inflight transactions/partitions for the next run.
public interface TableWriterPressureControl {
    TableWriterPressureControl EMPTY = new NoPressureControl();

    long getMaxBlockRowCount();

    void updateInflightPartitions(int count);

    int getMemoryPressureLevel();

    int getMemoryPressureRegulationValue();

    boolean isReadyToProcess();

    boolean onEnoughMemory();

    void onOutOfMemory();

    void setMaxBlockRowCount(int count);

    void updateInflightTxnBlockLength(long txnCount, long rowCount);

    void onBlockApplyError();

    class NoPressureControl implements TableWriterPressureControl {
        @Override
        public long getMaxBlockRowCount() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getMemoryPressureLevel() {
            return 0;
        }

        @Override
        public int getMemoryPressureRegulationValue() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isReadyToProcess() {
            return true;
        }

        @Override
        public boolean onEnoughMemory() {
            return true;
        }

        @Override
        public void onOutOfMemory() {
        }

        @Override
        public void setMaxBlockRowCount(int count) {
        }

        @Override
        public void updateInflightPartitions(int count) {
        }

        @Override
        public void updateInflightTxnBlockLength(long txnCount, long rowCount) {
        }

        @Override
        public void onBlockApplyError() {
        }
    }
}
