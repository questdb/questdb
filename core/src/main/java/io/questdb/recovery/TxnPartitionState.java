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

package io.questdb.recovery;

public final class TxnPartitionState {
    private final int index;
    private final long nameTxn;
    private final long parquetFileSize;
    private final boolean parquetFormat;
    private final long rawMaskedSize;
    private final boolean readOnly;
    private final long rowCount;
    private final long timestampLo;

    public TxnPartitionState(
            int index,
            long timestampLo,
            long rawMaskedSize,
            long rowCount,
            long nameTxn,
            long parquetFileSize,
            boolean parquetFormat,
            boolean readOnly
    ) {
        this.index = index;
        this.timestampLo = timestampLo;
        this.rawMaskedSize = rawMaskedSize;
        this.rowCount = rowCount;
        this.nameTxn = nameTxn;
        this.parquetFileSize = parquetFileSize;
        this.parquetFormat = parquetFormat;
        this.readOnly = readOnly;
    }

    public int getIndex() {
        return index;
    }

    public long getNameTxn() {
        return nameTxn;
    }

    public long getParquetFileSize() {
        return parquetFileSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getTimestampLo() {
        return timestampLo;
    }

    public boolean isParquetFormat() {
        return parquetFormat;
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}

