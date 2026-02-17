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

/**
 * Provides stub txn scoreboards for COPY SQL.
 */
public class EmptyTxnScoreboardPool implements TxnScoreboardPool {
    public static final EmptyTxnScoreboardPool INSTANCE = new EmptyTxnScoreboardPool();

    @Override
    public void clear() {
    }

    @Override
    public TxnScoreboard getTxnScoreboard(TableToken token) {
        return new TxnScoreboard() {
            @Override
            public boolean acquireTxn(int id, long txn) {
                return true;
            }

            @Override
            public void close() {
            }

            @Override
            public int getEntryCount() {
                return 0;
            }

            @Override
            public TableToken getTableToken() {
                return null;
            }

            @Override
            public boolean hasEarlierTxnLocks(long maxTxn) {
                return false;
            }

            @Override
            public boolean incrementTxn(int id, long txn) {
                return false;
            }

            @Override
            public boolean isOutdated(long txn) {
                return true;
            }

            @Override
            public boolean isRangeAvailable(long fromTxn, long toTxn) {
                return true;
            }

            @Override
            public boolean isTxnAvailable(long txn) {
                return true;
            }

            @Override
            public long releaseTxn(int id, long txn) {
                return 0;
            }
        };
    }

    @Override
    public boolean releaseInactive() {
        return false;
    }

    @Override
    public void remove(TableToken token) {
    }
}
