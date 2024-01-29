/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.MemorySerializer;
import io.questdb.std.str.Path;

import java.io.Closeable;

public interface TableTransactionLogFile extends Closeable {
    int HEADER_RESERVED = 6 * Long.BYTES + Integer.BYTES;
    long MAX_TXN_OFFSET_64 = Integer.BYTES;
    int STRUCTURAL_CHANGE_WAL_ID = -1;
    long TABLE_CREATE_TIMESTAMP_OFFSET_64 = MAX_TXN_OFFSET_64 + Long.BYTES;
    long SEQ_CHUNK_SIZE_32 = TABLE_CREATE_TIMESTAMP_OFFSET_64 + Long.BYTES;
    long HEADER_SIZE = SEQ_CHUNK_SIZE_32 + Integer.BYTES + HEADER_RESERVED;
    long TX_LOG_STRUCTURE_VERSION_OFFSET = 0L;
    long TX_LOG_WAL_ID_OFFSET = TX_LOG_STRUCTURE_VERSION_OFFSET + Long.BYTES;
    long TX_LOG_SEGMENT_OFFSET = TX_LOG_WAL_ID_OFFSET + Integer.BYTES;
    long TX_LOG_SEGMENT_TXN_OFFSET = TX_LOG_SEGMENT_OFFSET + Integer.BYTES;
    long TX_LOG_COMMIT_TIMESTAMP_OFFSET = TX_LOG_SEGMENT_TXN_OFFSET + Integer.BYTES;
    long RECORD_SIZE = TX_LOG_COMMIT_TIMESTAMP_OFFSET + Long.BYTES;

    long addEntry(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp);

    void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp);

    void create(Path path, long tableCreateTimestamp);

    long endMetadataChangeEntry();

    TransactionLogCursor getCursor(long txnLo, Path path);

    boolean isDropped();

    long lastTxn();

    long open(Path path);

    void sync();
}
