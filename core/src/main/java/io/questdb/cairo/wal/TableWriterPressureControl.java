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

package io.questdb.cairo.wal;

public interface TableWriterPressureControl {
    TableWriterPressureControl EMPTY = new NoPressureControl();

    int getMaxTransactionCount();

    void updateInflightPartitions(int count);

    int getMemoryPressureLevel();

    int getMemoryPressureRegulationValue();

    boolean isReadyToProcess();

    boolean onEnoughMemory();

    void onOutOfMemory();

    void setMaxTransactionCount(int count);

    void updateInflightTransactions(int count);

    class NoPressureControl implements TableWriterPressureControl {
        @Override
        public int getMaxTransactionCount() {
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
        public void setMaxTransactionCount(int count) {

        }

        @Override
        public void updateInflightPartitions(int count) {
        }

        @Override
        public void updateInflightTransactions(int count) {
        }
    }
}
