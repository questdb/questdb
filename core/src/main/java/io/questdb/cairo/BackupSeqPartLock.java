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

// Thread safety: all access is serialized by CairoEngine.walPurgeJobLock.
// The only exception is the cleared flag, which is volatile and can be set by other threads without locking.
public class BackupSeqPartLock {
    private final CharSequenceLongHashMap lockedSeqTxns = new CharSequenceLongHashMap();
    private volatile boolean cleared;

    public void clear() {
        cleared = true;
    }

    public void onLocked() {
        if (cleared) {
            lockedSeqTxns.clear();
            cleared = false;
        }
    }

    public long getLockedSeqTxn(TableToken tableToken) {
        if (cleared) {
            return CharSequenceLongHashMap.NO_ENTRY_VALUE;
        }
        return lockedSeqTxns.get(tableToken.getDirName());
    }

    public void lock(TableToken tableToken, long seqTxn) {
        lockedSeqTxns.put(tableToken.getDirName(), seqTxn);
    }
}