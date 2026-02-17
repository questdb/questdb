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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.MemorySerializer;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * This interface is used to read/write transactions to the files located in table_dir\\txn_seq\\_txnlog
 * <p>
 * The file header has fields
 * <p>
 * Version - 4 bytes
 * Max Txn - 8 bytes
 * Table Create Timestamp - 8 bytes
 * Partition Size - 4 bytes
 * <p>
 * The header Version and Partition Size determines the storage format:
 * 0 - single file, V1
 * 1 - multiple files, V2, additional fields
 * <p>
 * The transaction has fields
 * <p>
 * Structure Version - 8 bytes
 * WAL ID - 4 bytes
 * Segment ID - 4 bytes
 * Segment Txn - 4 bytes
 * Commit Timestamp - 8 bytes
 * <p>
 * Total V1 record size is 28 bytes
 * <p>
 * There are additional fields for V2 of the file format:
 * <p>
 * Txn Min Timestamp  8 bytes
 * Txn Max Timestamp  8 bytes
 * Txn Row Count      8 bytes
 * Reserved           8 bytes, initialized with 0s
 * Total V2 record size is 60 bytes
 * <p>
 * All the records are either stored in the single file for when the version is 0
 * or in multiple files with N records per file.
 * <p>
 * See different implementations of the interface for the storage details.
 */
public interface TableTransactionLogFile extends Closeable {
    int HEADER_RESERVED = 6 * Long.BYTES + Integer.BYTES;
    long MAX_TXN_OFFSET_64 = Integer.BYTES;
    int STRUCTURAL_CHANGE_WAL_ID = -1;
    long TABLE_CREATE_TIMESTAMP_OFFSET_64 = MAX_TXN_OFFSET_64 + Long.BYTES;
    long HEADER_SEQ_PART_SIZE_32 = TABLE_CREATE_TIMESTAMP_OFFSET_64 + Long.BYTES;
    long HEADER_SIZE = HEADER_SEQ_PART_SIZE_32 + Integer.BYTES + HEADER_RESERVED;
    long TX_LOG_STRUCTURE_VERSION_OFFSET = 0L;
    long TX_LOG_WAL_ID_OFFSET = TX_LOG_STRUCTURE_VERSION_OFFSET + Long.BYTES;
    long TX_LOG_SEGMENT_OFFSET = TX_LOG_WAL_ID_OFFSET + Integer.BYTES;
    long TX_LOG_SEGMENT_TXN_OFFSET = TX_LOG_SEGMENT_OFFSET + Integer.BYTES;
    long TX_LOG_COMMIT_TIMESTAMP_OFFSET = TX_LOG_SEGMENT_TXN_OFFSET + Integer.BYTES;

    /**
     * Adds a new data transaction to the log
     * <p>
     * s     * @param structureVersion version of the table structure
     *
     * @param walId           id of the WAL
     * @param segmentId       id of the segment
     * @param segmentTxn      transaction id within the segment
     * @param timestamp       commit timestamp
     * @param txnMinTimestamp minimum timestamp in the transaction
     * @param txnMaxTimestamp maximum timestamp in the transaction
     * @param txnRowCount     number of rows in the transaction
     * @return committed transaction id
     */
    long addEntry(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount);

    /**
     * Adds a new metadata transaction to the log. It's a 2-step process, this call must be
     * followed by endMetadataChangeEntry() call, otherwise the transaction is not considered to be committed.
     *
     * @param newStructureVersion new version of the table structure
     * @param serializer          serializer to write metadata
     * @param instance            instance to serialize
     * @param timestamp           commit timestamp
     */
    void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp);

    /**
     * Creates transaction log files on the disk
     *
     * @param path                 to create file
     * @param tableCreateTimestamp timestamp of table creation
     */
    void create(Path path, long tableCreateTimestamp);

    /**
     * Finishes commit of a metadata change transaction
     *
     * @return new version of the table structure
     */
    long endMetadataChangeEntry();

    /**
     * Syncs/flushes the log files to the disk unconditionally.
     */
    void fullSync();

    /**
     * Returns the cursor to read transactions from the log
     *
     * @param txnLo transaction id to start reading from
     * @param path  to the log
     * @return cursor
     */
    TransactionLogCursor getCursor(long txnLo, @Transient Path path);

    /**
     * @return Returns true if the table is marked as dropped in the sequencer log files
     */
    boolean isDropped();

    /**
     * @return Returns the last transaction id
     */
    long lastTxn();

    /**
     * Opens transaction log files for reading.
     *
     * @param path to the log
     * @return transaction id of the last committed transaction
     */
    long open(Path path);
}
