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
    /**
     * UNSET sentinel for the PER-TABLE commit-mode override stored in {@code _meta}
     * ({@link io.questdb.cairo.TableUtils#META_OFFSET_COMMIT_MODE}). A table whose {@code _meta} stores
     * UNSET (every table created before this field existed, and every table that never set an explicit
     * mode) defers to the global {@code cairo.commit.mode}. Resolve a table's effective mode with
     * {@link #effectiveCommitMode(int, int)}. UNSET must never reach a durability decision point — it is
     * resolved against the global mode first.
     */
    public static final int UNSET = -1;
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

    /**
     * Resolves a table's EFFECTIVE commit mode: the per-table override stored in {@code _meta} when it is
     * set, otherwise the global {@code cairo.commit.mode}. This is the single rule every per-table
     * adaptive decision point must apply (WAL-commit durability, the apply lazy gate, the durable-epoch
     * trigger, the WAL-purge floor, recovery) so that a {@code WITH commit_mode='adaptive'} table behaves
     * adaptively even when the instance default is {@code nosync}, while its siblings keep the global mode.
     *
     * @param tableMode  the mode stored in the table's {@code _meta} ({@link #UNSET} if none)
     * @param globalMode the instance-wide {@code cairo.commit.mode}
     * @return {@code tableMode} when it is not {@link #UNSET}, else {@code globalMode}
     */
    public static int effectiveCommitMode(int tableMode, int globalMode) {
        return tableMode != UNSET ? tableMode : globalMode;
    }

    /**
     * Parses a {@code commit_mode} token from DDL ({@code WITH commit_mode='...'} /
     * {@code SET PARAM commit_mode='...'}) into a {@link CommitMode} constant. Case-insensitive. Returns
     * {@link #UNSET} for an unrecognized value so the caller can raise a precise SQL error; {@code "unset"}
     * is accepted explicitly to allow reverting a table to the global default.
     */
    public static int fromString(CharSequence mode) {
        if (mode == null) {
            return UNSET;
        }
        if (io.questdb.std.Chars.equalsIgnoreCase(mode, "nosync")) {
            return NOSYNC;
        }
        if (io.questdb.std.Chars.equalsIgnoreCase(mode, "sync")) {
            return SYNC;
        }
        if (io.questdb.std.Chars.equalsIgnoreCase(mode, "async")) {
            return ASYNC;
        }
        if (io.questdb.std.Chars.equalsIgnoreCase(mode, "adaptive")) {
            return ADAPTIVE;
        }
        if (io.questdb.std.Chars.equalsIgnoreCase(mode, "unset")) {
            return UNSET;
        }
        // Unknown token: signal via a distinct out-of-range value so callers don't confuse it with the
        // legitimate UNSET default. -2 is never a valid mode.
        return UNKNOWN;
    }

    /**
     * Returns the lower-case canonical name of a commit mode, or {@code "unset"} for {@link #UNSET}. Used
     * by {@code wal_tables().commitMode} and {@code SHOW CREATE TABLE}-style output.
     */
    public static String toString(int commitMode) {
        switch (commitMode) {
            case SYNC:
                return "sync";
            case ASYNC:
                return "async";
            case NOSYNC:
                return "nosync";
            case ADAPTIVE:
                return "adaptive";
            case UNSET:
                return "unset";
            default:
                return "unknown";
        }
    }

    /**
     * Distinct from {@link #UNSET}: returned by {@link #fromString(CharSequence)} for a token that is not a
     * recognized mode name, so DDL can reject it with a precise error rather than silently storing UNSET.
     */
    public static final int UNKNOWN = -2;
}
