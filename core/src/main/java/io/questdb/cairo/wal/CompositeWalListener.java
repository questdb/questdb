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

import io.questdb.cairo.TableToken;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * Fans out WAL events to a dynamic list of listeners. Iteration on the hot path
 * (commit thread) reads a volatile snapshot. Add/remove rebuild the snapshot
 * under a monitor and publish atomically, so callbacks never see a torn list.
 * <p>
 * Listeners must execute quickly and must not block on I/O or long-running work,
 * since callbacks fire on the WAL writer's commit thread.
 */
public class CompositeWalListener implements WalListener {
    private final Object writeLock = new Object();
    private volatile ObjList<WalListener> listeners = new ObjList<>(0);

    public void add(@NotNull WalListener listener) {
        synchronized (writeLock) {
            ObjList<WalListener> snapshot = listeners;
            for (int i = 0, n = snapshot.size(); i < n; i++) {
                if (snapshot.getQuick(i) == listener) {
                    return;
                }
            }
            ObjList<WalListener> next = new ObjList<>(snapshot.size() + 1);
            for (int i = 0, n = snapshot.size(); i < n; i++) {
                next.add(snapshot.getQuick(i));
            }
            next.add(listener);
            listeners = next;
        }
    }

    @Override
    public void dataTxnCommitted(TableToken tableToken, long txn, long timestamp, int walId, int segmentId, int segmentTxn) {
        ObjList<WalListener> snapshot = listeners;
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            snapshot.getQuick(i).dataTxnCommitted(tableToken, txn, timestamp, walId, segmentId, segmentTxn);
        }
    }

    public boolean isEmpty() {
        return listeners.size() == 0;
    }

    @Override
    public void nonDataTxnCommitted(TableToken tableToken, long txn, long timestamp) {
        ObjList<WalListener> snapshot = listeners;
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            snapshot.getQuick(i).nonDataTxnCommitted(tableToken, txn, timestamp);
        }
    }

    public void remove(@NotNull WalListener listener) {
        synchronized (writeLock) {
            ObjList<WalListener> snapshot = listeners;
            int idx = -1;
            for (int i = 0, n = snapshot.size(); i < n; i++) {
                if (snapshot.getQuick(i) == listener) {
                    idx = i;
                    break;
                }
            }
            if (idx < 0) {
                return;
            }
            ObjList<WalListener> next = new ObjList<>(snapshot.size() - 1);
            for (int i = 0, n = snapshot.size(); i < n; i++) {
                if (i != idx) {
                    next.add(snapshot.getQuick(i));
                }
            }
            listeners = next;
        }
    }

    @Override
    public void segmentClosed(final TableToken tableToken, long txn, int walId, int segmentId) {
        ObjList<WalListener> snapshot = listeners;
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            snapshot.getQuick(i).segmentClosed(tableToken, txn, walId, segmentId);
        }
    }

    public int size() {
        return listeners.size();
    }

    @Override
    public void tableCreated(TableToken tableToken, long timestamp) {
        ObjList<WalListener> snapshot = listeners;
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            snapshot.getQuick(i).tableCreated(tableToken, timestamp);
        }
    }

    @Override
    public void tableDropped(TableToken tableToken, long txn, long timestamp) {
        ObjList<WalListener> snapshot = listeners;
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            snapshot.getQuick(i).tableDropped(tableToken, txn, timestamp);
        }
    }

    @Override
    public void tableRenamed(TableToken tableToken, long txn, long timestamp, TableToken oldTableToken) {
        ObjList<WalListener> snapshot = listeners;
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            snapshot.getQuick(i).tableRenamed(tableToken, txn, timestamp, oldTableToken);
        }
    }

    @Override
    public void walClosed(TableToken tableToken, long txn, int walId) {
        ObjList<WalListener> snapshot = listeners;
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            snapshot.getQuick(i).walClosed(tableToken, txn, walId);
        }
    }
}
