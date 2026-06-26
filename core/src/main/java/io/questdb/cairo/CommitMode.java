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

package io.questdb.cairo;

public final class CommitMode {
    public static final int ASYNC = 0;
    public static final int SYNC = 1;
    public static final int NOSYNC = 2;
    /**
     * ADAPTIVE: every WAL commit is made durable (fdatasync of segment column data →
     * WAL-e events file → sequencer record, in that order) before the commit returns.
     * Unlike SYNC (which relies on msync alone), ADAPTIVE additionally calls fdatasync
     * after each msync so that a crash-replay can recover every acked transaction.
     * The table-apply (TableWriter) path is unchanged; laziness there is a separate task.
     */
    public static final int ADAPTIVE = 3;

    /**
     * Returns {@code true} iff this commit mode requires a per-commit msync/fdatasync flush of the
     * TABLE PARTITION COLUMN FILES on the apply path (the materialization of a WAL/O3 commit into
     * the table's partition column files: {@code TableWriter.syncColumns()},
     * {@code O3CopyJob}'s destination-column sync, and the partition split/squash and
     * parquet-conversion column writes).
     *
     * <p>SYNC and ASYNC both flush column files on every commit — SYNC blocks until durable, ASYNC
     * schedules writeback. NOSYNC never flushes.
     *
     * <p>ADAPTIVE is intentionally excluded (treated like NOSYNC here): under ADAPTIVE the WAL
     * commit is made durable (fdatasync of segment→events→sequencer, Plan 2 Task A), so the
     * materialized table is a REBUILDABLE CACHE of the durable WAL. Flushing the partition columns
     * on every apply would negate adaptive's whole point (fsync the small log, not the big table).
     * Crash-safety of the lazily-applied columns is provided END-TO-END by the durable EPOCH
     * ({@code TableWriter.fsyncMaterializedState()}, which force-flushes regardless of mode) plus
     * recovery roll-forward of {@code (epoch.seqTxn, frontier]} from the durable WAL (Plan 3).
     *
     * <p><b>Apply-path only.</b> This gate must be used ONLY at sites that write column DATA which is
     * re-derivable by replaying the WAL from the epoch. It must NOT be used for structural/DDL sync
     * sites ({@code _meta}, {@code _todo}, parquet {@code _pm} metadata, partition directory
     * entries) — those stay durable under {@code commitMode != NOSYNC} regardless — nor inside
     * {@code fsyncMaterializedState()} (the epoch must force the flush).
     *
     * <p>Non-WAL tables have no durable WAL to replay, so ADAPTIVE on a non-WAL table degrades to
     * NOSYNC-grade apply durability; use SYNC if you need per-commit apply durability there.
     */
    public static boolean appliesColumnSync(int commitMode) {
        return commitMode == SYNC || commitMode == ASYNC;
    }
}
