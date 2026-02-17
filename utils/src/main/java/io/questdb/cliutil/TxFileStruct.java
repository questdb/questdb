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

package io.questdb.cliutil;

import java.util.ArrayList;


class TxFileStruct {
    // Gson serializable class
    public ArrayList<AttachedPartition> ATTACHED_PARTITIONS;
    public int ATTACHED_PARTITIONS_COUNT;
    public ArrayList<SymbolInfo> SYMBOLS;
    public int TX_OFFSET_CHECKSUM;
    public long TX_OFFSET_COLUMN_VERSION;
    public long TX_OFFSET_DATA_VERSION;
    public long TX_OFFSET_FIXED_ROW_COUNT;
    public long TX_OFFSET_LAG_MAX_TIMESTAMP;
    public long TX_OFFSET_LAG_MIN_TIMESTAMP;
    public int TX_OFFSET_LAG_ROW_COUNT;
    public int TX_OFFSET_LAG_TXN_COUNT;
    public int TX_OFFSET_MAP_WRITER_COUNT;
    public long TX_OFFSET_MAX_TIMESTAMP;
    public long TX_OFFSET_MIN_TIMESTAMP;
    public long TX_OFFSET_PARTITION_TABLE_VERSION;
    public long TX_OFFSET_SEQ_TXN;
    public long TX_OFFSET_STRUCT_VERSION;
    public long TX_OFFSET_TRANSIENT_ROW_COUNT;
    public long TX_OFFSET_TRUNCATE_VERSION;
    public long TX_OFFSET_TXN;

    @Override
    public String toString() {
        return "TxFileStruct{" +
                "TX_OFFSET_TXN=" + TX_OFFSET_TXN +
                ", TX_OFFSET_COLUMN_VERSION=" + TX_OFFSET_COLUMN_VERSION +
                ", TX_OFFSET_FIXED_ROW_COUNT=" + TX_OFFSET_FIXED_ROW_COUNT +
                ", TX_OFFSET_TRANSIENT_ROW_COUNT=" + TX_OFFSET_TRANSIENT_ROW_COUNT +
                ", TX_OFFSET_STRUCT_VERSION=" + TX_OFFSET_STRUCT_VERSION +
                ", TX_OFFSET_DATA_VERSION=" + TX_OFFSET_DATA_VERSION +
                ", TX_OFFSET_PARTITION_TABLE_VERSION=" + TX_OFFSET_PARTITION_TABLE_VERSION +
                ", TX_OFFSET_TRUNCATE_VERSION=" + TX_OFFSET_TRUNCATE_VERSION +
                ", TX_OFFSET_MAP_WRITER_COUNT=" + TX_OFFSET_MAP_WRITER_COUNT +
                ", SYMBOLS=" + SYMBOLS +
                ", TX_OFFSET_MAX_TIMESTAMP=" + TX_OFFSET_MAX_TIMESTAMP +
                ", TX_OFFSET_MIN_TIMESTAMP=" + TX_OFFSET_MIN_TIMESTAMP +
                ", ATTACHED_PARTITIONS=" + ATTACHED_PARTITIONS +
                ", ATTACHED_PARTITIONS_COUNT=" + ATTACHED_PARTITIONS_COUNT +
                ", TX_OFFSET_SEQ_TXN=" + TX_OFFSET_SEQ_TXN +
                ", TX_OFFSET_LAG_ROW_COUNT=" + TX_OFFSET_LAG_ROW_COUNT +
                ", TX_OFFSET_LAG_TXN_COUNT=" + TX_OFFSET_LAG_TXN_COUNT +
                '}';
    }

    static class AttachedPartition {
        long DATA_TX;
        long MASK;
        long NAME_TX;
        long SIZE;
        long TS;

        @Override
        public String toString() {
            return "AttachedPartition{" +
                    "TS=" + TS +
                    ", MASK=" + MASK +
                    ", SIZE=" + SIZE +
                    ", NAME_TX=" + NAME_TX +
                    ", DATA_TX=" + DATA_TX +
                    '}';
        }
    }

    static class SymbolInfo {
        int COUNT;
        int UNCOMMITTED_COUNT;

        @Override
        public String toString() {
            return "SymbolInfo{COUNT=" + COUNT + ", UNCOMMITTED_COUNT=" + UNCOMMITTED_COUNT + '}';
        }
    }
}

