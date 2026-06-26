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
 * return -1. Unknown directory names return -1.
 *
 * <p>{@link #getDurablyUploadedSeqTxn(CharSequence)} always returns -1 in OSS (no upload pipeline).
 * Enterprise installations install their own registry via
 * {@link CairoEngine#setDurableAckRegistry(DurableAckRegistry)}, which can compose with or
 * supersede this local tier.
 *
 * <p>{@link #isEnabled()} returns {@code true} so QWP honours the
 * {@code X-QWP-Request-Durable-Ack} opt-in header and emits durable-ack frames for ADAPTIVE
 * tables out of the box.
 */
public class LocalDurableAckRegistry implements DurableAckRegistry {

    private final CairoEngine engine;

    public LocalDurableAckRegistry(@NotNull CairoEngine engine) {
        this.engine = engine;
    }

    /**
     * Returns the highest locally-fdatasync'd seqTxn for the given table, or -1 if the table is
     * unknown, uses NOSYNC commit mode, or has not yet committed a local-durable txn.
     */
    @Override
    public long getLocalDurableSeqTxn(CharSequence tableDirName) {
        TableToken token = engine.getTableTokenByDirName(tableDirName);
        if (token == null) {
            return -1L;
        }
        try {
            SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            return tracker.getLocalDurableSeqTxn();
        } catch (Throwable ignored) {
            // Table may have been dropped or sequencer closed between the dir-name resolution
            // and the tracker fetch — harmless, return -1.
            return -1L;
        }
    }

    /**
     * Returns -1 in OSS — no upload pipeline is available.
     */
    @Override
    public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
        return -1L;
    }

    /**
     * Returns {@code true}: the local-fsync tier is always active in the OSS server,
     * enabling QWP durable-ack frames for ADAPTIVE tables.
     */
    @Override
    public boolean isEnabled() {
        return true;
    }
}
