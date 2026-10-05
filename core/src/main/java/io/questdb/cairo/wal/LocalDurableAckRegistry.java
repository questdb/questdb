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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import org.jetbrains.annotations.NotNull;

/**
 * OSS default {@link DurableAckRegistry} that reports the local-fsync tier: the highest seqTxn
 * whose WAL commit was fdatasync'd (ADAPTIVE mode only) for each table.
 *
 * <p>This implementation resolves the table directory name to a {@link TableToken} via the engine's
 * table name registry, then reads {@link SeqTxnTracker#getLocalDurableSeqTxn()} from the
 * sequencer API. NOSYNC tables (or tables whose tracker has not yet recorded a local-durable txn)
 * return -1. A directory that no longer resolves to a live table returns
 * {@link DurableAckRegistry#TABLE_GONE}.
 *
 * <p>{@link #getReplicatedDurableSeqTxn(CharSequence)} always returns -1 in OSS (no upload pipeline).
 * Enterprise installations install their own registry via
 * {@link CairoEngine#setDurableAckRegistry(DurableAckRegistry)}, which can compose with or
 * supersede this local tier.
 *
 * <p>{@link #isEnabled()} and {@link #isTierAvailable(int)} answer {@code true} only under
 * ADAPTIVE commit mode, so a server that can never advance the local frontier denies the
 * {@code X-QWP-Request-Durable-Ack} opt-in at the handshake instead of leaving the client waiting
 * for frames that will not come.
 */
public class LocalDurableAckRegistry implements DurableAckRegistry {

    private final CairoEngine engine;

    public LocalDurableAckRegistry(@NotNull CairoEngine engine) {
        this.engine = engine;
    }

    /**
     * Whether this server can serve the {@link DurabilityTier#LOCAL} tier. Mirrors the producer
     * gate in {@code WalWriter.commit}: the local frontier advances only under ADAPTIVE, so any other
     * mode reports -1 for every table forever. Shared with Enterprise's upload-backed registry.
     */
    public static boolean canServeLocalTier(CairoEngine engine) {
        return !engine.isDurabilityFailed()
                && engine.getConfiguration().getCommitMode() == CommitMode.ADAPTIVE;
    }

    /**
     * Shared local-fsync tier lookup: resolves {@code tableDirName} to a {@link TableToken} via
     * the engine's table name registry, then reads
     * {@link SeqTxnTracker#getLocalDurableSeqTxn()} from the sequencer API. Returns -1 if the
     * table uses NOSYNC commit mode, has not yet committed a local-durable txn, or durability has
     * failed, and {@link DurableAckRegistry#TABLE_GONE} if the directory no longer resolves to a
     * live table.
     *
     * <p>"Does not resolve" covers a dropped table (the reverse-map item is marked dropped only
     * after {@code tables.d} recorded the drop and synced), a directory retired by
     * {@code REBASE WAL} (same, through the swap record) and a dropped token that the WAL purge
     * already removed. Rename replaces the reverse-map item in place, so a renamed table keeps
     * resolving. The registry is reloaded only at boot, before QWP accepts connections, so a live
     * table never reads as unknown here. The answer needs no per-drop state, which is why it also
     * covers the drop paths that never call {@link DurableAckRegistry#onTableDropped}.
     *
     * <p>Extracted so Enterprise's upload-backed registry can compose the local tier without
     * depending on a {@link LocalDurableAckRegistry} instance.
     */
    public static long resolveLocalDurableSeqTxn(CairoEngine engine, CharSequence tableDirName) {
        if (engine.isDurabilityFailed()) {
            return -1L;
        }
        TableToken token = engine.getTableTokenByDirName(tableDirName);
        if (token == null) {
            return TABLE_GONE;
        }
        try {
            SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            return tracker.getLocalDurableSeqTxn();
        } catch (Throwable ignored) {
            // Table may have been dropped or sequencer closed between the dir-name resolution
            // and the tracker fetch. Report no progress; the next lookup resolves TABLE_GONE.
            return -1L;
        }
    }

    /**
     * Returns the highest locally-fdatasync'd seqTxn for the given table, {@link #TABLE_GONE} if
     * the directory no longer resolves to a live table, or -1 if the table uses NOSYNC commit mode
     * or has not yet committed a local-durable txn.
     */
    @Override
    public long getLocalDurableSeqTxn(CharSequence tableDirName) {
        return resolveLocalDurableSeqTxn(engine, tableDirName);
    }

    /**
     * Returns -1 in OSS — no upload pipeline is available.
     */
    @Override
    public long getReplicatedDurableSeqTxn(CharSequence tableDirName) {
        return -1L;
    }

    /**
     * LOCAL is the only tier OSS offers, so the registry is enabled exactly when that tier is servable.
     */
    @Override
    public boolean isEnabled() {
        return canServeLocalTier(engine);
    }

    @Override
    public boolean isTierAvailable(int tier) {
        return tier == DurabilityTier.LOCAL && canServeLocalTier(engine);
    }
}
