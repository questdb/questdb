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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;

import java.io.File;

/**
 * Acts at the first flush of a table's WAL event file while armed: runs an action, or fails the flush, once.
 * A table is named by its directory prefix, such as {@code "b~"}. Arm it after the table's writer is open, so
 * that the flush is a commit's, not the segment open's.
 */
public class WalEventFlushFaultFacade extends WalFlushHookFacade {
    private volatile Runnable action;
    private volatile String actionTable;
    private volatile String failedTable;
    private volatile String failure;
    private volatile boolean hasFired;
    private volatile boolean isDataSyncFailure;

    public void failEventFlush(String table, boolean isDataSyncFailure) {
        failEventFlush(table, isDataSyncFailure, "test: prepare failed");
    }

    public void failEventFlush(String table, boolean isDataSyncFailure, String failure) {
        this.isDataSyncFailure = isDataSyncFailure;
        this.failure = failure;
        this.failedTable = table;
    }

    public boolean hasFired() {
        return hasFired;
    }

    public void onEventFlush(String table, Runnable action) {
        this.action = action;
        this.actionTable = table;
    }

    @Override
    protected boolean isTracked(LPSZ name) {
        return Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME);
    }

    @Override
    protected void onFlush(String path) {
        final String actionTable = this.actionTable;
        if (actionTable != null && path.contains(File.separator + actionTable)) {
            this.actionTable = null;
            hasFired = true;
            action.run();
        }
        final String failedTable = this.failedTable;
        if (failedTable != null && path.contains(File.separator + failedTable)) {
            this.failedTable = null;
            hasFired = true;
            if (isDataSyncFailure) {
                throw CairoException.dataSyncFailure(this, 5, "msync", failure);
            }
            throw CairoException.critical(0).put(failure);
        }
    }
}
