/*+*****************************************************************************
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

/**
 * Memory budget of one WAL apply transaction block, checked while
 * {@link WalTxnDetails#calculateInsertTransactionBlock} walks the transactions it could add to the block.
 */
public interface WalBlockBudget {

    /**
     * Starts a candidate block with its first transaction, which is in the block whatever it costs.
     */
    void begin(long minTimestamp, long maxTimestamp, long rowCount);

    /**
     * Adds the next transaction to the candidate block. Returns false when the block with it would not fit the
     * budget; the block then ends before it.
     */
    boolean tryAdd(long minTimestamp, long maxTimestamp, long rowCount);
}
