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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.TableToken;

/**
 * Listener for push notifications from the sequencer service.
 * In multi-primary mode, the central sequencer pushes events to all
 * connected nodes so they can update their local state.
 */
public interface SequencerServiceListener {

    /**
     * A new table has been registered on the sequencer.
     */
    void onTableRegistered(TableToken tableToken, long databaseVersion);

    /**
     * A table has been dropped from the sequencer.
     */
    void onTableDropped(TableToken tableToken, long databaseVersion);

    /**
     * A table has been renamed on the sequencer.
     */
    void onTableRenamed(TableToken oldToken, TableToken newToken, long databaseVersion);

    /**
     * New transactions are available for the given table.
     * Local nodes should apply WALs up to the given sequencer transaction.
     */
    void onTransactionsAvailable(TableToken tableToken, long seqTxn);
}
