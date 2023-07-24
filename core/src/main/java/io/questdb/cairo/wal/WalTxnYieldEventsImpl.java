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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.network.YieldEvent;
import io.questdb.network.YieldEventFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.BiFunction;

public class WalTxnYieldEventsImpl implements WalTxnYieldEvents {

    // Used to pass arguments to avoid allocations due to capturing lambdas.
    private static final ThreadLocal<YieldEvent> tlEvent = new ThreadLocal<>();
    private static final ThreadLocal<ObjList<YieldEvent>> tlList = new ThreadLocal<>();
    private final ConcurrentHashMap<ObjList<YieldEvent>> events = new ConcurrentHashMap<>(false);
    private final BiFunction<CharSequence, ObjList<YieldEvent>, ObjList<YieldEvent>> registerComputeRef = this::registerCompute;
    private final TableSequencerAPI tableSequencerAPI;
    private final BiFunction<CharSequence, ObjList<YieldEvent>, ObjList<YieldEvent>> takeRegisteredEventsComputeRef = this::takeRegisteredEventsCompute;
    private final YieldEventFactory yieldEventFactory;

    public WalTxnYieldEventsImpl(@NotNull TableSequencerAPI tableSequencerAPI, @NotNull YieldEventFactory yieldEventFactory) {
        this.tableSequencerAPI = tableSequencerAPI;
        this.yieldEventFactory = yieldEventFactory;
    }

    @Override
    public void close() {
        for (Map.Entry<CharSequence, ObjList<YieldEvent>> entry : events.entrySet()) {
            Misc.freeObjListAndClear(entry.getValue());
        }
    }

    @Override
    public @Nullable YieldEvent register(TableToken tableToken, long txn) {
        // TODO we don't handle table dropped vs. tracker not initialized scenarios here properly
        long currentTxn = readSeqTxn(tableToken);
        if (currentTxn == -1 || currentTxn >= txn) {
            // The txn is already visible or the table is dropped.
            return null;
        }

        final YieldEvent yieldEvent = yieldEventFactory.newInstance();
        tlEvent.set(yieldEvent);
        try {
            events.compute(tableToken.getDirName(), registerComputeRef);
        } finally {
            tlEvent.set(null);
        }

        // Read the txn once again to make sure ApplyWal2TableJob sees our changes.
        try {
            currentTxn = readSeqTxn(tableToken);
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

    @Override
    public void takeRegisteredEvents(TableToken tableToken, ObjList<YieldEvent> dest) {
        tlList.set(dest);
        try {
            events.computeIfPresent(tableToken.getDirName(), takeRegisteredEventsComputeRef);
        } finally {
            tlList.set(null);
        }
    }

    private long readSeqTxn(TableToken tableToken) {
        final SeqTxnTracker seqTxnTracker = tableSequencerAPI.getSeqTxnTracker(tableToken);
        return seqTxnTracker.getAppliedTxn();
    }

    private ObjList<YieldEvent> registerCompute(CharSequence dir, ObjList<YieldEvent> list) {
        if (list == null) {
            list = new ObjList<>();
        }
        final YieldEvent yieldEvent = tlEvent.get();
        list.add(yieldEvent);
        return list;
    }

    private ObjList<YieldEvent> takeRegisteredEventsCompute(CharSequence dir, ObjList<YieldEvent> list) {
        ObjList<YieldEvent> dest = tlList.get();
        dest.addAll(list);
        list.clear();
        return list;
    }
}
