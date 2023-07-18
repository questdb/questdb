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

import io.questdb.cairo.*;
import io.questdb.network.YieldEvent;
import io.questdb.network.YieldEventFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class WalTxnYieldEventsImpl implements WalTxnYieldEvents {

    // Used to pass arguments to avoid allocations due to capturing lambdas.
    private static final ThreadLocal<YieldEvent> tlEvent = new ThreadLocal<>();
    private static final ThreadLocal<ObjList<YieldEvent>> tlList = new ThreadLocal<>();
    private final CharSequence dbRoot;
    private final ConcurrentHashMap<TableToken, ObjList<YieldEvent>> events = new ConcurrentHashMap<>();
    private final FilesFacade ff;
    private final MillisecondClock millisecondClock;
    private final BiFunction<TableToken, ObjList<YieldEvent>, ObjList<YieldEvent>> registerComputeRef = this::registerCompute;
    private final long spinLockTimeout;
    private final BiFunction<TableToken, ObjList<YieldEvent>, ObjList<YieldEvent>> takeRegisteredEventsComputeRef = this::takeRegisteredEventsCompute;
    private final TxReader txReader;
    private final YieldEventFactory yieldEventFactory;

    public WalTxnYieldEventsImpl(@NotNull CairoConfiguration cairoConfiguration, @NotNull YieldEventFactory yieldEventFactory) {
        this.dbRoot = cairoConfiguration.getRoot();
        this.ff = cairoConfiguration.getFilesFacade();
        this.txReader = new TxReader(ff);
        this.millisecondClock = cairoConfiguration.getMillisecondClock();
        this.spinLockTimeout = cairoConfiguration.getSpinLockTimeout();
        this.yieldEventFactory = yieldEventFactory;
    }

    @Override
    public void close() {
        for (Map.Entry<TableToken, ObjList<YieldEvent>> entry : events.entrySet()) {
            Misc.freeObjListAndClear(entry.getValue());
        }
    }

    @Override
    public @Nullable YieldEvent register(TableToken tableToken, long txn) {
        Path tlPath = Path.PATH.get().of(dbRoot);
        tlPath.trimTo(dbRoot.length()).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
        if (ff.exists(tlPath)) {
            long currentTxn = readSeqTxn(tableToken, tlPath);
            if (currentTxn == -1 || currentTxn >= txn) {
                // The txn is already visible or the table is dropped.
                return null;
            }

            final YieldEvent yieldEvent = yieldEventFactory.newInstance();
            tlEvent.set(yieldEvent);
            try {
                events.compute(tableToken, registerComputeRef);
            } finally {
                tlEvent.set(null);
            }

            // Read the txn once again to make sure ApplyWal2TableJob sees our changes.
            try {
                currentTxn = readSeqTxn(tableToken, tlPath);
            } catch (CairoException e) {
                yieldEvent.close();
                throw e;
            }

            if (currentTxn == -1 || currentTxn >= txn) {
                // The txn is already visible or the table is dropped, so we close the event
                // and pretend it never existed. The event will be triggered and closed one
                // more time later either after takeRegisteredEvents() or by close().
                yieldEvent.close();
                return null;
            }
            return yieldEvent;
        }
        // The table is dropped.
        return null;
    }

    @Override
    public void takeRegisteredEvents(TableToken tableToken, ObjList<YieldEvent> dest) {
        tlList.set(dest);
        try {
            events.computeIfPresent(tableToken, takeRegisteredEventsComputeRef);
        } finally {
            tlList.set(null);
        }
    }

    private long readSeqTxn(TableToken tableToken, Path path) {
        path.trimTo(dbRoot.length()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
        try {
            try (TxReader txReader2 = txReader.ofRO(path, PartitionBy.NONE)) {
                TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                return txReader2.getSeqTxn();
            }
        } catch (CairoException e) {
            if (e.getErrno() == 0) {
                // This must be read timeout, so there is not so much we can do.
                throw e;
            }
            // The table must have been dropped, so we have nothing to wait for.
            return -1;
        }
    }

    private ObjList<YieldEvent> registerCompute(TableToken tableToken, ObjList<YieldEvent> list) {
        if (list == null) {
            list = new ObjList<>();
        }
        final YieldEvent yieldEvent = tlEvent.get();
        list.add(yieldEvent);
        return list;
    }

    private ObjList<YieldEvent> takeRegisteredEventsCompute(TableToken tableToken, ObjList<YieldEvent> list) {
        ObjList<YieldEvent> dest = tlList.get();
        dest.addAll(list);
        list.clear();
        return list;
    }
}
