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

import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * A {@code _checkpoints/_ring} manifest the startup sweep read and structurally
 * validated, awaiting the trust decision.
 * <p>
 * A candidate is <em>not</em> a ring. It carries what the manifest claimed plus
 * the fact that the claim is self-consistent and every listed {@code .cp} still
 * exists; it carries no verdict on whether those entries may serve as resume
 * anchors. That verdict compares {@code coveredBaseSeqTxn} against the
 * reconciled applied floor, which exists only on the refresh worker: the sweep
 * runs inside {@code CairoEngine.buildViewGraphs()} on the startup thread,
 * before any worker, and can see only the raw and legitimately stale
 * {@code _lv.s}. Deciding trust there would discard the ring on exactly the
 * crash restarts the manifest exists for.
 * <p>
 * The sweep also consults a candidate as an allow-list while cleaning
 * {@code _checkpoints/}: a listed {@code .cp} is exempt from the orphan rule and
 * from the older-than-highest retirement, so the entries the reconciled floor is
 * about to validate survive to be rehydrated. Exemption keeps the file; it never
 * promotes it to the fallback head, which stays on the conservative raw-watermark
 * gate.
 * <p>
 * Held on {@link LiveViewInstance} from catalogue load until the first refresh
 * cycle consumes it. The startup thread populates it and the refresh worker
 * reads it, so the instance's field is volatile and a stashed candidate is
 * treated as immutable.
 */
public class LiveViewCheckpointRingCandidate implements Mutable {
    // Packed ring records, ENTRY_SIZE longs each, oldest first - the layout
    // LiveViewInstance's ring holds, so rehydration is a straight copy.
    private final LongList entries = new LongList();
    private long coveredBaseSeqTxn = Numbers.LONG_NULL;
    private long generation = Numbers.LONG_NULL;
    private boolean structurallyValid;

    @Override
    public void clear() {
        coveredBaseSeqTxn = Numbers.LONG_NULL;
        generation = Numbers.LONG_NULL;
        structurallyValid = false;
        entries.clear();
    }

    /**
     * Base seqTxn at which the manifest claims every listed entry is sealed.
     * Meaningful only once {@link #isStructurallyValid()} holds; the trust rule
     * is equality with the reconciled applied floor.
     */
    public long getCoveredBaseSeqTxn() {
        return coveredBaseSeqTxn;
    }

    /**
     * The packed ring, {@link LiveViewCheckpointRingManifest#ENTRY_SIZE} longs
     * per record, oldest first. Owned by this candidate - copy before retaining.
     */
    public LongList getEntries() {
        return entries;
    }

    public long getEntryBaseSeqTxn(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_BASE_SEQ_TXN);
    }

    public int getEntryCount() {
        return entries.size() / LiveViewCheckpointRingManifest.ENTRY_SIZE;
    }

    public long getEntryLvRowsTotal(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_LV_ROWS_TOTAL);
    }

    public long getEntryLvSeqTxn(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_LV_SEQ_TXN);
    }

    public long getEntryMaxTs(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_MAX_TS);
    }

    public long getEntryStateBytes(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_STATE_BYTES);
    }

    /**
     * Publication counter the on-disk manifest carried. Diagnostic, and the seed
     * for the refresh worker's counter so a recovered view does not republish
     * generations the manifest already used.
     */
    public long getGeneration() {
        return generation;
    }

    /**
     * Whether {@code lvSeqTxn} names a checkpoint this manifest lists.
     * <p>
     * The allow-list test the sweep applies per {@code .cp} file. An unlisted
     * {@code .cp} is garbage whatever its filename or contents - a stale file
     * whose retirement unlink failed is indistinguishable from a sealed one on
     * disk, which is why the manifest, not the directory, defines the ring.
     * <p>
     * A linear scan: the ring is capped at
     * {@code cairo.live.view.checkpoint.retention.count} entries (8 by default),
     * so this stays cheaper than a binary search's setup.
     */
    public boolean isListed(long lvSeqTxn) {
        if (!structurallyValid) {
            return false;
        }
        for (int i = 0, n = entries.size(); i < n; i += LiveViewCheckpointRingManifest.ENTRY_SIZE) {
            if (entries.getQuick(i + LiveViewCheckpointRingManifest.ENTRY_LV_SEQ_TXN) == lvSeqTxn) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the manifest parsed, satisfied every entry invariant, and named
     * only checkpoints that still exist. False also covers "no manifest on
     * disk", which is the legacy and never-published shape rather than an error.
     * <p>
     * Says nothing about trust: a structurally valid candidate whose
     * {@code coveredBaseSeqTxn} does not match the reconciled floor is discarded.
     */
    public boolean isStructurallyValid() {
        return structurallyValid;
    }

    /**
     * Copies a parsed manifest in and marks the candidate structurally valid.
     * The reader is the sweep's reusable parse scratch, so the copy is what lets
     * this candidate outlive the view's turn through the catalogue load.
     */
    public void of(@NotNull LiveViewCheckpointRingManifestReader reader) {
        clear();
        coveredBaseSeqTxn = reader.getCoveredBaseSeqTxn();
        generation = reader.getGeneration();
        entries.add(reader.getEntries());
        structurallyValid = true;
    }
}
