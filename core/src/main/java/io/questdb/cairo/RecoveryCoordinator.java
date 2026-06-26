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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.str.Path;

/**
 * Adaptive durable-EPOCH recovery roll-forward (Plan 3 Task C).
 * <p>
 * Runs ONCE at engine startup, AFTER the table-name registry is loaded but BEFORE normal WAL apply
 * ({@code CheckWalTransactionsJob} -> {@code ApplyWal2TableJob}) runs for any table. For each
 * ADAPTIVE WAL table that recorded a durable epoch ({@code _snapshot} marker + immutable
 * {@code _txn.epoch}/{@code _cv.epoch} copies written by {@link TableWriter#fsyncMaterializedState()}),
 * it RESTORES the durable cut: copies {@code _txn.epoch}->{@code _txn} and
 * {@code _cv.epoch}->{@code _cv}, fsyncs them (and the table dir) BEFORE proceeding, so the table
 * opens at exactly {@code epoch.{seqTxn,txn}}. The existing boot path then idempotently re-applies
 * {@code (epoch.seqTxn, frontier]} from the durable WAL.
 * <p>
 * This is the keystone that makes ADAPTIVE crash-safe end to end: under ADAPTIVE the table apply is
 * LAZY (partition columns are non-durable between epochs — see {@link CommitMode#appliesColumnSync}),
 * so after a power cut the materialized state can be torn ahead of the last durable epoch. Rewinding
 * {@code _txn}/{@code _cv} to the durable cut and re-deriving the rest from the durable WAL rebuilds
 * exactly the lost rows.
 *
 * <h3>Why this is safe and idempotent</h3>
 * <ul>
 *   <li><b>Restore-before-rely (audit #5):</b> the copied {@code _txn}/{@code _cv} are fsync'd, and
 *       the table dir is fsync'd, BEFORE recovery returns — so a crash immediately after recovery
 *       sees the restored cut, not a half-copied file.</li>
 *   <li><b>Idempotent:</b> the {@code .epoch} copies are IMMUTABLE until the next epoch overwrites
 *       them ({@link TableWriter#fsyncMaterializedState()} rewrites them only when a new, later cut
 *       is taken). Re-running recovery (e.g. a crash mid-recovery) re-copies the same bytes and lands
 *       on the same cut. Apply itself is idempotent through {@code _txn} (the contiguity assert
 *       {@code seqTxn == appliedSeqTxn + 1} is satisfied because {@code _txn} now says
 *       {@code epoch.seqTxn}).</li>
 *   <li><b>Conservative fallback:</b> a table with NO {@code _snapshot} marker (or whose
 *       {@code .epoch} copies are absent) is left UNTOUCHED -> today's behaviour (full WAL replay /
 *       normal open). Non-adaptive tables and non-WAL tables are never touched.</li>
 * </ul>
 */
public class RecoveryCoordinator {
    private static final Log LOG = LogFactory.getLog(RecoveryCoordinator.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;

    public RecoveryCoordinator(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
    }

    /**
     * Restore the durable epoch cut for every adaptive WAL table that has one. Must be called once,
     * after the table registry is loaded and before any WAL apply.
     */
    public void recover() {
        // Operator kill-switch / negative-control hook: when disabled, skip the roll-forward entirely
        // (under ADAPTIVE this leaves a post-crash table torn ahead of the last epoch — by design).
        if (!configuration.isAdaptiveRecoveryRollForwardEnabled()) {
            return;
        }
        // Adaptive-only: the durable epoch + lazy apply only exist under CommitMode.ADAPTIVE. Under
        // any other mode the apply path is already self-durable (or NOSYNC by design), and no epoch
        // copies are ever written, so there is nothing to roll forward.
        if (configuration.getCommitMode() != CommitMode.ADAPTIVE) {
            return;
        }

        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        engine.getTableTokens(tokens, false);

        try (Path src = new Path(); Path dst = new Path(); Path dir = new Path()) {
            for (int i = 0, n = tokens.size(); i < n; i++) {
                final TableToken token = tokens.get(i);
                if (!token.isWal()) {
                    continue;
                }
                recoverTable(token, src, dst, dir);
            }
        }
    }

    private void recoverTable(TableToken token, Path src, Path dst, Path dir) {
        // Does this table have a durable epoch? Load the _snapshot marker; absent / both slots torn =>
        // no recovery anchor => leave the table untouched (full WAL replay / normal open).
        final long epochSeqTxn;
        try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
            tablePath(dir, token).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(dir.$());
            if (!marker.tryLoad()) {
                return;
            }
            epochSeqTxn = marker.getEpochSeqTxn();
        }

        // The immutable durable cut copies. advance() writes BOTH copies (fsync'd) BEFORE the marker,
        // so a loadable marker implies both copies exist; still, if either is missing the cut is
        // incomplete -> conservatively leave the live files untouched (normal open / full replay).
        epochCopyPath(src, token, TableUtils.TXN_FILE_NAME);
        final boolean txnEpochExists = ff.exists(src.$());
        epochCopyPath(src, token, TableUtils.COLUMN_VERSION_FILE_NAME);
        final boolean cvEpochExists = ff.exists(src.$());
        if (!txnEpochExists || !cvEpochExists) {
            LOG.error().$("adaptive epoch marker present but a durable copy is absent, skipping roll-forward [table=")
                    .$(token).$(", epochSeqTxn=").$(epochSeqTxn)
                    .$(", txnEpoch=").$(txnEpochExists).$(", cvEpoch=").$(cvEpochExists).I$();
            return;
        }

        // Restore the durable cut: _txn.epoch -> _txn, _cv.epoch -> _cv. ff.copy() (creat O_TRUNC)
        // fully replaces the live, lazily-advanced files with the epoch's canonical A/B record. The
        // .epoch copies are immutable until the next epoch, so re-running this (a crash mid-recovery)
        // re-copies identical bytes and lands on the same cut -> idempotent.
        //
        // Restore _txn (the pointer) BEFORE _cv (the data) here. We are overwriting live files that, post-
        // crash, sit at the FRONTIER. A crash BETWEEN these two restores leaves _txn at the (older) epoch
        // while _cv is still at the (newer) frontier -> _txn behind _cv, the SAFE skew (the older _txn
        // never references column versions beyond it). The reverse would briefly leave _txn at the frontier
        // over an epoch _cv (a dangling reference). Recovery re-runs on the next boot and completes the pair.
        restoreFile(token, src, dst, TableUtils.TXN_FILE_NAME);
        restoreFile(token, src, dst, TableUtils.COLUMN_VERSION_FILE_NAME);

        // Restore-BEFORE-rely (audit #5): the copied _txn/_cv must be durable, and their directory
        // entries (sizes/names) journaled, BEFORE the boot path opens the table and re-applies the WAL
        // on top of this cut. fsync each restored file's fd, then the table dir.
        fsyncFile(token, dst, TableUtils.TXN_FILE_NAME);
        fsyncFile(token, dst, TableUtils.COLUMN_VERSION_FILE_NAME);
        fsyncDir(token, dir);

        LOG.info().$("adaptive epoch roll-forward restored durable cut [table=").$(token)
                .$(", epochSeqTxn=").$(epochSeqTxn).I$();
    }

    /** Copy {@code <fileName>.epoch} over {@code <fileName>} in the table dir (O_TRUNC replace). */
    private void restoreFile(TableToken token, Path src, Path dst, CharSequence fileName) {
        epochCopyPath(src, token, fileName);
        tablePath(dst, token).concat(fileName);
        if (ff.copy(src.$(), dst.$()) < 0) {
            throw CairoException.critical(ff.errno())
                    .put("adaptive epoch roll-forward failed to restore [table=").put(token.getTableName())
                    .put(", src=").put(src).put(", dst=").put(dst).put(']');
        }
    }

    /** fsync a single table-dir file by path (open RW, fsync, close). */
    private void fsyncFile(TableToken token, Path dst, CharSequence fileName) {
        tablePath(dst, token).concat(fileName);
        final long fd = TableUtils.openRW(ff, dst.$(), LOG, configuration.getWriterFileOpenOpts());
        if (fd == -1) {
            throw CairoException.critical(ff.errno())
                    .put("adaptive epoch roll-forward could not open restored file to fsync [table=")
                    .put(token.getTableName()).put(", file=").put(dst).put(']');
        }
        try {
            ff.fsync(fd);
        } finally {
            ff.close(fd);
        }
    }

    /** fsync the table directory entry so the restored file sizes/names are journaled. */
    private void fsyncDir(TableToken token, Path dir) {
        if (Os.isWindows()) {
            return; // no directory fsync on Windows (mirrors TableWriter's dir-sync guards)
        }
        tablePath(dir, token).slash$();
        final long dirFd = TableUtils.openRONoCache(ff, dir.$(), LOG);
        if (dirFd != -1) {
            ff.fsyncAndClose(dirFd);
        }
    }

    private Path tablePath(Path p, TableToken token) {
        return p.of(configuration.getDbRoot()).concat(token);
    }

    private Path epochCopyPath(Path p, TableToken token, CharSequence baseFileName) {
        tablePath(p, token).concat(baseFileName).put(TableUtils.EPOCH_COPY_SUFFIX);
        return p;
    }
}
