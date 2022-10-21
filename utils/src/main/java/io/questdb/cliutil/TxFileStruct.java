/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cliutil;

import java.util.ArrayList;

import static io.questdb.cairo.TableUtils.TX_BASE_HEADER_SIZE;
import static io.questdb.cairo.TableUtils.getPartitionTableIndexOffset;

class TxFileStruct {
    public long TX_OFFSET_COLUMN_VERSION;
    public long TX_OFFSET_TXN;
    public long TX_OFFSET_TRANSIENT_ROW_COUNT;
    public long TX_OFFSET_FIXED_ROW_COUNT;
    public long TX_OFFSET_MIN_TIMESTAMP;
    public long TX_OFFSET_MAX_TIMESTAMP;
    public long TX_OFFSET_DATA_VERSION;
    public long TX_OFFSET_STRUCT_VERSION;
    public long TX_OFFSET_PARTITION_TABLE_VERSION;
    public long TX_OFFSET_TRUNCATE_VERSION;
    public long TX_OFFSET_SEQ_TXN;
    public int TX_OFFSET_MAP_WRITER_COUNT;
    public ArrayList<SymbolInfo> SYMBOLS;
    public int ATTACHED_PARTITION_SIZE;
    public ArrayList<AttachedPartition> ATTACHED_PARTITIONS;

    public long calculateFileSize() {
        return getPartitionTableIndexOffset(TX_OFFSET_MAP_WRITER_COUNT, ATTACHED_PARTITION_SIZE * 4) + TX_BASE_HEADER_SIZE;
    }

    static class AttachedPartition {
        long TS;
        long SIZE;
        long NAME_TX;
        long DATA_TX;
    }

    static class SymbolInfo {
        int COUNT;
        int UNCOMMITTED_COUNT;
    }
}
