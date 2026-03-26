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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;

/**
 * A lightweight PageFrame with correct structure but zero column addresses.
 * Used to populate the uninitialized address cache during the upfront phase.
 */
record UninitializedPageFrame(int partitionIndex, long lo, long hi, byte format) implements PageFrame {

    @Override
    public long getAuxPageAddress(int columnIndex) {
        return 0;
    }

    @Override
    public long getAuxPageSize(int columnIndex) {
        return 0;
    }

    @Override
    public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
        return null;
    }

    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public long getPageAddress(int columnIndex) {
        return 0;
    }

    @Override
    public long getPageSize(int columnIndex) {
        return 0;
    }

    @Override
    public PartitionDecoder getParquetPartitionDecoder() {
        return null;
    }

    @Override
    public int getParquetRowGroup() {
        return -1;
    }

    @Override
    public int getParquetRowGroupHi() {
        return -1;
    }

    @Override
    public int getParquetRowGroupLo() {
        return -1;
    }

    @Override
    public long getPartitionHi() {
        return hi;
    }

    @Override
    public long getPartitionLo() {
        return lo;
    }
}
