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

package io.questdb.cairo.lv;

import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * Mutable bean carrying the contents of a checkpoint MANIFEST block.
 * <p>
 * Reused across reads and writes by callers that want zero-alloc on the
 * checkpoint hot path. Field semantics:
 * <ul>
 *     <li>{@link #getLvSeqTxn() lvSeqTxn} - live-view seqTxn this checkpoint
 *     follows; state reflects all rows up through this seqTxn.</li>
 *     <li>{@link #getLvRowPosition() lvRowPosition} - total live-view rows
 *     produced through this seqTxn.</li>
 *     <li>{@link #getBaseSeqTxn() baseSeqTxn} - for {@link #KIND_STEADY},
 *     the last fully-processed base seqTxn; for {@link #KIND_BACKFILL}, the
 *     backfillTargetSeqTxn (mirrors {@code _lv.s.backfillTargetSeqTxn}).</li>
 *     <li>{@link #getMaxTimestamp() maxTimestamp} - max ts of rows reflected
 *     in state, in base-table timestamp units. Lookup key for the
 *     {@code head.maxTimestamp <= late_row.ts} O3 routing rule.</li>
 *     <li>{@link #getKind() kind} - {@link #KIND_STEADY} for steady-state
 *     and O3 replay; {@link #KIND_BACKFILL} for in-progress backfill.</li>
 *     <li>{@link #getWindowNames() windowNames} - the names of every named
 *     WINDOW the live view's compiled SELECT defines (anchored and
 *     non-anchored). The window names are persisted so a future revision
 *     can rebuild dispatch tables without re-parsing the SELECT.</li>
 * </ul>
 */
public class LiveViewCheckpointManifest implements Mutable {

    public static final byte KIND_BACKFILL = 1;
    public static final byte KIND_STEADY = 0;

    private final ObjList<String> windowNames = new ObjList<>();
    private long baseSeqTxn = Numbers.LONG_NULL;
    private byte kind = KIND_STEADY;
    private long lvRowPosition = 0;
    private long lvSeqTxn = Numbers.LONG_NULL;
    private long maxTimestamp = Numbers.LONG_NULL;

    public LiveViewCheckpointManifest addWindowName(@NotNull String name) {
        windowNames.add(name);
        return this;
    }

    @Override
    public void clear() {
        baseSeqTxn = Numbers.LONG_NULL;
        kind = KIND_STEADY;
        lvRowPosition = 0;
        lvSeqTxn = Numbers.LONG_NULL;
        maxTimestamp = Numbers.LONG_NULL;
        windowNames.clear();
    }

    public long getBaseSeqTxn() {
        return baseSeqTxn;
    }

    public byte getKind() {
        return kind;
    }

    public long getLvRowPosition() {
        return lvRowPosition;
    }

    public long getLvSeqTxn() {
        return lvSeqTxn;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public ObjList<String> getWindowNames() {
        return windowNames;
    }

    public LiveViewCheckpointManifest setBaseSeqTxn(long baseSeqTxn) {
        this.baseSeqTxn = baseSeqTxn;
        return this;
    }

    public LiveViewCheckpointManifest setKind(byte kind) {
        if (kind != KIND_STEADY && kind != KIND_BACKFILL) {
            throw new IllegalArgumentException("unknown manifest kind: " + kind);
        }
        this.kind = kind;
        return this;
    }

    public LiveViewCheckpointManifest setLvRowPosition(long lvRowPosition) {
        this.lvRowPosition = lvRowPosition;
        return this;
    }

    public LiveViewCheckpointManifest setLvSeqTxn(long lvSeqTxn) {
        this.lvSeqTxn = lvSeqTxn;
        return this;
    }

    public LiveViewCheckpointManifest setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
        return this;
    }
}
