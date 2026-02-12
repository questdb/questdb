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

package io.questdb.cairo;

import io.questdb.std.CharSequenceLongHashMap;

/**
 * Listener interface for checkpoint lifecycle events.
 */
public interface CheckpointListener {

    /**
     * Called when a checkpoint is released (CHECKPOINT RELEASE command completes).
     * <p>
     * The provided map contains table directory names (e.g., "my_table~1") as keys
     * and their corresponding sequencer transaction numbers as values. This represents
     * the consistent snapshot point for each WAL table at checkpoint time.
     * <p>
     * Note: This is called while the checkpoint lock is still held, before the
     * checkpoint files are cleaned up.
     *
     * @param timestampMicros       the timestamp when CHECKPOINT CREATE was initiated (epoch microseconds)
     * @param tableDirNamesToSeqTxn map of table directory names to sequencer txn numbers;
     *                              the map is owned by the caller and must not be retained
     */
    void onCheckpointReleased(long timestampMicros, CharSequenceLongHashMap tableDirNamesToSeqTxn);

    /**
     * Called after a checkpoint restore operation completes successfully.
     * <p>
     * This callback is invoked during database recovery when a checkpoint is restored.
     * Implementations can use this to reset any state that should not persist
     * across restores.
     */
    void onCheckpointRestoreComplete();
}
