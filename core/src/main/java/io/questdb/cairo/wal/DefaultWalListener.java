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

package io.questdb.cairo.wal;

import io.questdb.cairo.TableToken;

public class DefaultWalListener implements WalListener {
    public static final DefaultWalListener INSTANCE = new DefaultWalListener();

    @Override
    public void dataTxnCommitted(TableToken tableToken, long txn, long timestamp, int walId, int segmentId, int segmentTxn) {
    }

    @Override
    public void nonDataTxnCommitted(TableToken tableToken, long txn, long timestamp) {
    }

    @Override
    public void segmentClosed(final TableToken tableToken, long txn, int walId, int segmentId) {
    }

    @Override
    public void tableCreated(TableToken tableToken, long timestamp) {
    }

    @Override
    public void tableDropped(TableToken tableToken, long txn, long timestamp) {
    }

    @Override
    public void tableRenamed(TableToken tableToken, long txn, long timestamp, TableToken oldTableToken) {
    }
}
