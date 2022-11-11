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

package io.questdb.cairo.wal.seq;

import io.questdb.std.QuietCloseable;

public interface TableSequencer extends QuietCloseable {
    long NO_TXN = Long.MIN_VALUE;

    TableMetadataChangeLog getMetadataChangeLogCursor(long structureVersionLo);

    int getNextWalId();

    long getStructureVersion();

    int getTableId();

    void getTableMetadata(TableRecordMetadataSink sink);

    // return txn cursor to apply transaction from given point
    TransactionLogCursor getTransactionLogCursor(long seqTxn);

    boolean isSuspended();

    long lastTxn();

    long nextStructureTxn(long structureVersion, TableMetadataChange change);

    // returns committed txn number if schema version is the expected one, otherwise returns NO_TXN
    long nextTxn(long expectedSchemaVersion, int walId, int segmentId, long segmentTxn);

    void suspendTable();
}
