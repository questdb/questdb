/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.net;

import com.nfsdb.journal.net.bridge.JournalEventBridge;
import com.nfsdb.journal.tx.TxFuture;
import com.nfsdb.journal.tx.TxListener;

import java.util.concurrent.TimeUnit;

public class JournalEventPublisher implements TxListener {
    private final int journalIndex;
    private final JournalEventBridge bridge;

    public JournalEventPublisher(int journalIndex, JournalEventBridge bridge) {
        this.journalIndex = journalIndex;
        this.bridge = bridge;
    }


    @Override
    public boolean notifySync(long timeout, TimeUnit unit) {
        TxFuture future = notifyAsync();
        return future.waitFor(timeout, unit);
    }

    @Override
    public void notifyAsyncNoWait() {
        long timestamp = System.currentTimeMillis();
        bridge.publish(journalIndex, timestamp);
    }

    @Override
    public TxFuture notifyAsync() {
        long timestamp = System.currentTimeMillis();
        TxFuture future = bridge.createRemoteCommitFuture(journalIndex, timestamp);
        bridge.publish(journalIndex, timestamp);
        return future;
    }
}
