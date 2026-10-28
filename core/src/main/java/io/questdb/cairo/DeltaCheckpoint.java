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

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.FilesFacade;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Captures reader-visible Delta catalogs and retains their files until close.
 */
public interface DeltaCheckpoint extends QuietCloseable {
    String CATALOG_SUFFIX = "_delta";
    String DIRECTORY_NAME = "_delta";

    /**
     * Whether the {@code _txn} at {@code txnPath} flags Delta state on any partition. Restore looks for
     * Delta files only in a table whose checkpoint or live {@code _txn} does, so a table without Delta
     * makes no per-partition probe. A missing {@code _txn} has none; one that cannot be read counts as
     * having it.
     */
    static boolean hasDeltaState(FilesFacade ff, TxReader txn, LPSZ txnPath, int timestampType, int partitionBy) {
        if (!ff.exists(txnPath)) {
            return false;
        }
        try {
            txn.ofRO(txnPath, timestampType, partitionBy);
            return !txn.unsafeLoadAll() || txn.hasDeltaState();
        } catch (CairoException e) {
            return true;
        } finally {
            txn.clear();
        }
    }

    void capture(TableReader reader, Path checkpoint, SqlExecutionCircuitBreaker circuitBreaker);

    /**
     * Releases all captures, including partial captures, and the restore scratch. The adapter can then be reused.
     */
    @Override
    default void close() {
    }

    /**
     * Validates and installs captured catalogs, then removes later Delta state before table repair.
     */
    void restore(FilesFacade ff, Path checkpoint, Path table);

    /**
     * Releases retained catalogs before recovery replaces their files. Called before tables open.
     */
    default void startRestore() {
    }
}
