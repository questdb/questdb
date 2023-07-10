/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.ServerConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.SuspendEvent;
import io.questdb.network.SuspendEventFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WalTxnSuspendEventsImpl implements WalTxnSuspendEvents {

    private final CharSequence dbRoot;
    private final ConcurrentHashMap<TableToken, ObjList<SuspendEvent>> events = new ConcurrentHashMap<>();
    private final FilesFacade ff;
    private final IODispatcherConfiguration ioDispatcherConfiguration;
    private final MillisecondClock millisecondClock;
    private final long spinLockTimeout;
    private final TxReader txReader;

    public WalTxnSuspendEventsImpl(ServerConfiguration configuration) {
        this.dbRoot = configuration.getCairoConfiguration().getRoot();
        this.ff = configuration.getCairoConfiguration().getFilesFacade();
        this.txReader = new TxReader(ff);
        this.millisecondClock = configuration.getCairoConfiguration().getMillisecondClock();
        this.spinLockTimeout = configuration.getCairoConfiguration().getSpinLockTimeout();
        this.ioDispatcherConfiguration = configuration.getLineTcpReceiverConfiguration().getDispatcherConfiguration();
    }

    @Override
    public void close() {
        // TODO this is not thread-safe
        for (Map.Entry<TableToken, ObjList<SuspendEvent>> entry : events.entrySet()) {
            Misc.freeObjListAndClear(entry.getValue());
        }
    }

    @Override
    public @Nullable SuspendEvent register(TableToken tableToken, long txn) {
        Path tlPath = Path.PATH.get().of(dbRoot);
        tlPath.trimTo(dbRoot.length()).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
        if (ff.exists(tlPath)) {
            // TODO error handling (table dropped, reader timeout)
            long currentTxn = readSeqTxn(tableToken, tlPath);
            if (currentTxn >= txn) {
                // The txn is already visible.
                return null;
            }

            // TODO think of happens-before edge here
            final SuspendEvent suspendEvent = SuspendEventFactory.newInstance(ioDispatcherConfiguration);
            events.compute(tableToken, (t, list) -> {
                if (list == null) {
                    list = new ObjList<>();
                }
                list.add(suspendEvent);
                return list;
            });

            currentTxn = readSeqTxn(tableToken, tlPath);
            if (currentTxn >= txn) {
                // The txn is already visible, so we close the event and pretend it never existed.
                // The event will be triggered and closed one more time later either after
                // takeRegisteredEvents() or by close().
                suspendEvent.close();
                return null;
            }
        }
        // The table is dropped.
        return null;
    }

    @Override
    public void takeRegisteredEvents(TableToken tableToken, ObjList<SuspendEvent> dest) {
        // TODO method refs instead of lambdas
        events.computeIfPresent(tableToken, (t, list) -> {
            dest.addAll(list);
            list.clear();
            return list;
        });
    }

    private long readSeqTxn(TableToken tableToken, Path path) {
        path.trimTo(dbRoot.length()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
        try (TxReader txReader2 = txReader.ofRO(path, PartitionBy.NONE)) {
            TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
            return txReader2.getSeqTxn();
        }
    }
}
