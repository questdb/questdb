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
     * "No enrolment recorded" sentinel for the ENROLMENT record in {@code _meta}
     * ({@link io.questdb.cairo.TableUtils#META_OFFSET_ENROLLED_COMMIT_MODE}), which is the only place a
     * commit mode is stored per table. It is not a mode a table can run under: durability is an
     * instance-wide property set by {@code cairo.commit.mode}, so every durability decision point reads
     * {@code configuration.getCommitMode()}.
     * <p>
     * A {@code _meta} written before the enrolment field existed reads back UNSET, which answers the only
     * question the field asks -- "may this table's materialized state be lazily ahead of its durable
     * epoch?" -- with "no".
     * <p>
     * The same sentinel marks "no mode threaded in yet" on the writer-scoped {@code setCommitMode} seams
     * ({@code MemoryMA}), whose holders then read the instance-global mode.
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
     * The commit mode that applies when nothing is configured, for both the server and the embedded API.
     * <p>
     * NOSYNC until an ingest benchmark justifies moving it: adaptive buys local durability at a throughput
     * cost, and that trade has to be measured rather than assumed. Flipping this constant is the whole
     * change -- {@code PropServerConfiguration} and {@code DefaultCairoConfiguration} both defer to it, so
     * they cannot drift apart and hand a server and an embedded process different durability.
     * <p>
     * The test suite runs ADAPTIVE regardless, via {@code questdb.test.commit.mode}; see
     * {@code Overrides}. Changing this constant does not change what the suite exercises.
     */
    public static final int DEFAULT = NOSYNC;

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
     * <p><b>Apply-path only.</b> This gate must be used ONLY at sites whose content is re-derivable by
     * replaying the WAL from the epoch. Besides the column data itself that means the commit POINTERS and
     * the derived indexes, which take the same gate for the same reason:
     * <ul>
     *   <li>{@code TxWriter.commit} ({@code _txn}) and {@code ColumnVersionWriter.commit} ({@code _cv}) —
     *       {@code RecoveryCoordinator} restores both from the epoch's immutable {@code .epoch} copies and
     *       replays {@code (epoch.seqTxn, frontier]} on top. Keeping them eager while the columns are lazy
     *       would also produce a strictly WORSE post-crash state: a durable pointer exposing rows whose data
     *       never reached the device, instead of both rolling back together.</li>
     *   <li>{@code BitmapIndexWriter.commit} / {@code PostingIndexWriter.commit} ({@code .k}/{@code .v},
     *       {@code .pk}/{@code .pv}) — re-derived from the column they index. Note their {@code commit()}
     *       still PUBLISHES buffered postings unconditionally; only the device flush is gated.</li>
     * </ul>
     * It must NOT be used for structural/DDL sync sites ({@code _meta}, {@code _todo}, parquet {@code _pm}
     * metadata, partition directory entries) — those stay durable under {@code commitMode != NOSYNC}
     * regardless — nor for one-shot writers that run outside a table writer and outside the epoch's coverage
     * (table conversion, WAL staging creation, checkpoint restore), which take
     * {@link #structuralCommitMode(int)} — nor inside {@code fsyncMaterializedState()} (the epoch must force
     * the flush, including an explicit {@code IndexWriter.sync(false)} per indexer).
     *
     * <p>Non-WAL tables have no durable WAL to replay, so ADAPTIVE on a non-WAL table degrades to
     * NOSYNC-grade apply durability; use SYNC if you need per-commit apply durability there.
     */
    public static boolean appliesColumnSync(int commitMode) {
        return commitMode == SYNC || commitMode == ASYNC;
    }

    /**
     * Maps a commit mode onto the grade a STRUCTURAL / one-shot durability site should use.
     *
     * <p>{@link #appliesColumnSync} (and the {@code _txn}/{@code _cv}/index commit gates that follow it) treat
     * ADAPTIVE as lazy, because on the APPLY path the materialized state is a rebuildable cache of the durable
     * WAL and the durable epoch is what makes it crash-safe. That reasoning does NOT extend to one-shot
     * structural work performed OUTSIDE a table writer and outside the epoch's coverage — table
     * WAL&harr;non-WAL conversion, WAL staging-directory creation, checkpoint/snapshot restore. Those writes
     * have no epoch to fall back on and no WAL to replay them from, so under ADAPTIVE they must take the
     * SYNC grade, exactly as they did when every such site read {@code commitMode != NOSYNC}.
     *
     * <p>NOSYNC / SYNC / ASYNC are returned unchanged, so this is behaviour-preserving for them.
     */
    public static int structuralCommitMode(int commitMode) {
        return commitMode == ADAPTIVE ? SYNC : commitMode;
    }

    /**
     * Parses a {@code cairo.commit.mode} token into a {@link CommitMode} constant. Case-insensitive.
     * <p>
     * Returns {@link #UNKNOWN} for an unrecognized token so the caller can reject it, rather than silently
     * selecting a mode the operator did not ask for. The database-wide mode is a durability contract; a
     * typo such as {@code syncc} must fail loudly.
     * <p>
     * Callers MUST therefore test for {@link #UNKNOWN} before using the result.
     */
    public static int fromString(CharSequence mode) {
        if (mode == null) {
            return UNKNOWN;
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
        return UNKNOWN;
    }

    /**
     * Returns the lower-case canonical name of a commit mode, or {@code "unset"} for {@link #UNSET}. Used
     * by the startup durability log lines and the configuration error messages.
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
     * Returned by {@link #fromString(CharSequence)} for a token that is not a recognized mode name, so the
     * caller can reject it with a precise error. Never a valid mode.
     */
    public static final int UNKNOWN = -2;
}
