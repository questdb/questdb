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

import java.io.Closeable;

/**
 * Reads transaction log files.
 * To start reading use {@code setPosition} method to set the starting position.
 * To read a record use {@code hasNext} method.
 */
public interface TransactionLogCursor extends Closeable {
    @Override
    void close();

    boolean extend();

    long getCommitTimestamp();

    long getMaxTxn();

    int getPartitionSize();

    int getSegmentId();

    int getSegmentTxn();

    long getStructureVersion();

    long getTxn();

    long getTxnMaxTimestamp();

    long getTxnMinTimestamp();

    long getTxnRowCount();

    int getVersion();

    int getWalId();

    boolean hasNext();

    void setPosition(long txn);

    // Sets cursor to minimum available position.
    // In case of chunked sequencer it will search for the min available position prior to the current position.
    void toMinTxn();

    void toTop();
}
