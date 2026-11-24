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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;

/**
 * Reads the repair marker on disk at each live-view commit made while the marker names
 * {@code markerBaseGeneration}, from the role-switch mint seam every live-view commit passes. A
 * repair writes its marker immediately before its replacement commit, so the marker read there
 * is the one that commit's repair wrote. Records the seqTxn each such marker holds and the view's
 * last seqTxn then, and fails the first such commit as {@code failure} says: with a critical
 * error, which lets the in-place recovery run, or with a cancellation, which ends the turn
 * without one, so nothing moves past the marker - the durable state a crash between the marker
 * and the commit leaves. Install {@link #onMint()} with
 * {@link CairoEngine#setRoleSwitchMintObserver} and uninstall it when done.
 */
final class LiveViewReplacementCommitWitness {
    static final int FAIL_CANCELLATION = 2;
    static final int FAIL_CRITICAL = 1;
    static final int FAIL_NONE = 0;
    private final CairoEngine engine;
    private final int failure;
    private final LiveViewInstance instance;
    private final long markerBaseGeneration;
    private final LongList recordedLvSeqTxns = new LongList();
    private final LongList viewLvSeqTxns = new LongList();
    private boolean hasFailed;

    LiveViewReplacementCommitWitness(CairoEngine engine, LiveViewInstance instance, long markerBaseGeneration, int failure) {
        this.engine = engine;
        this.instance = instance;
        this.markerBaseGeneration = markerBaseGeneration;
        this.failure = failure;
    }

    long getRecordedLvSeqTxn(int commit) {
        return recordedLvSeqTxns.getQuick(commit);
    }

    long getViewLvSeqTxn(int commit) {
        return viewLvSeqTxns.getQuick(commit);
    }

    boolean hasFailed() {
        return hasFailed;
    }

    void onMint() {
        try (Path dir = new Path()) {
            dir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            if (!LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir)
                    || LiveViewCheckpointRepairMarker.readBaseGeneration(engine.getConfiguration(), dir) != markerBaseGeneration) {
                return;
            }
            recordedLvSeqTxns.add(LiveViewCheckpointRepairMarker.readLvSeqTxn(engine.getConfiguration(), dir));
        }
        viewLvSeqTxns.add(engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
        if (failure != FAIL_NONE && !hasFailed) {
            hasFailed = true;
            if (failure == FAIL_CANCELLATION) {
                throw CairoException.queryCancelled();
            }
            throw CairoException.critical(0).put("simulated replacement commit failure");
        }
    }

    int size() {
        return recordedLvSeqTxns.size();
    }
}
