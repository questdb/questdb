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

import io.questdb.cairo.idx.ParquetIndexSeal;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.PostingSealPurgeTask;

import java.io.Closeable;

/**
 * Turns a single {@link PostingSealPurgeTask} into actual file deletions
 * when the table's {@link TxnScoreboard} confirms no reader is still in the
 * visibility window of the superseded sealed version.
 * <p>
 * Stateless aside from the reused {@link Path} buffer;
 * {@link PostingSealPurgeJob} owns one instance and feeds it tasks
 * sequentially.
 */
public class PostingSealPurgeOperator implements Closeable, PostingIndexUtils.SealedFileVisitor {

    private static final Log LOG = LogFactory.getLog(PostingSealPurgeOperator.class);
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final Path path;
    private final int pathRootLen;
    private boolean scanAllCoversRemoved;
    private boolean scanAnyCoverRemoved;
    private CharSequence scanColumnName;
    private int scanPartitionPathLen;
    private long scanRemovedCoverColumnNameTxn;
    private int scanRemovedCoverIncludeIdx;
    private long scanTargetPostingTxn;
    private long scanTargetSealTxn;
    private TxnScoreboard txnScoreboard;

    public PostingSealPurgeOperator(CairoEngine engine) {
        try {
            this.engine = engine;
            CairoConfiguration configuration = engine.getConfiguration();
            this.ff = configuration.getFilesFacade();
            this.path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            this.path.of(configuration.getDbRoot());
            this.pathRootLen = path.size();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(path);
        txnScoreboard = Misc.free(txnScoreboard);
    }

    @Override
    public void onCoverDataFile(int includeIdx, long postingColumnNameTxn, long coveredColumnNameTxn, long sealTxn) {
        if (postingColumnNameTxn != scanTargetPostingTxn || sealTxn != scanTargetSealTxn) {
            return;
        }
        LPSZ pc = PostingIndexUtils.coverDataFileName(path.trimTo(scanPartitionPathLen),
                scanColumnName, includeIdx, postingColumnNameTxn, coveredColumnNameTxn, sealTxn);
        // removeQuiet returns true on ENOENT too, but scanSealedFiles only
        // invokes this callback for .pc files actually present in the directory
        // listing, so a true return here means a real file was unlinked. Track
        // it so purge() can run the same post-unlink reuse-race head re-read for
        // covers that it already runs for the .pv (a reused-then-republished
        // sealTxn can make a .pc.{sealTxn} live again between purge()'s pre-unlink
        // guard and this scan; on Windows removeQuiet fails on the open mapped
        // file, so the live .pc survives and this flag stays false).
        if (ff.removeQuiet(pc)) {
            scanAnyCoverRemoved = true;
            // Remember a removed cover's identity so purge() can re-stat it after the
            // scan (the .pv pvStillGone analogue): a reused-then-republished sealTxn
            // recreates the whole .pc set, so a removed cover that is present again is
            // the benign orphan-then-reuse case, not a live unlink.
            scanRemovedCoverIncludeIdx = includeIdx;
            scanRemovedCoverColumnNameTxn = coveredColumnNameTxn;
        } else {
            scanAllCoversRemoved = false;
        }
        path.trimTo(scanPartitionPathLen);
    }

    @Override
    public void onValueFile(long postingColumnNameTxn, long sealTxn) {
    }

    public boolean purge(PostingSealPurgeTask task) {
        if (task.isEmpty()) {
            return true;
        }
        // A task's scoreboard window is derived from the supersession point of
        // ITS OWN namespace, so it may only be applied to artifacts of that
        // namespace. Attributing the number is not something the operator can
        // do from evidence on disk -- one directory can hold sym.pv.1 and
        // sym.pidx.1 at once -- so the producer records it and a task that
        // carries no attribution unlinks nothing.
        if (!PostingSealPurgeTask.isValidArtifactForm(task.getArtifactForm())) {
            // Every field below comes from the same decode that produced the
            // artifact form this branch rejected, so they are best-effort: a
            // decode wrong about one field can be wrong about the rest. Printed
            // anyway because they are the only lead a human gets, and said to be
            // best-effort so nobody treats a bad path as evidence of a bug
            // elsewhere.
            LOG.critical().$("posting seal purge: task carries no artifact form (purge log or spill file written by an older build), dropping without unlinking; fields below are best-effort, decoded from the same record [table=")
                    .$(task.getTableToken() == null ? null : task.getTableToken().getTableName())
                    .$(", column=").$(task.getIndexColumnName())
                    .$(", postingColumnNameTxn=").$(task.getPostingColumnNameTxn())
                    .$(", sealTxn=").$(task.getSealTxn())
                    // The partition, so the sidecars this task will not unlink
                    // can still be found by hand. recoverOpenTasks truncates the
                    // log once it drains, so this line is the only record that
                    // survives, and without the partition it names no path.
                    .$(", partitionTimestamp=").$(task.getPartitionTimestamp())
                    .$(", partitionNameTxn=").$(task.getPartitionNameTxn())
                    .$(", form=").$(task.getArtifactForm())
                    .I$();
            return true;
        }
        // Validate any cached scoreboard before doing anything else.
        // The cache is held across calls as an optimization for repeated
        // tasks on the same table; if the cached table has been dropped
        // (or recreated with a new tableId) we must release it now,
        // otherwise it stays pinned until the operator close() — and
        // the early-return paths below would skip the late
        // cache-invalidation check that used to live here.
        if (txnScoreboard != null) {
            TableToken cachedToken = txnScoreboard.getTableToken();
            TableToken liveCached = engine.getTableTokenIfExists(cachedToken.getTableName());
            if (liveCached == null || liveCached.getTableId() != cachedToken.getTableId()) {
                txnScoreboard = Misc.free(txnScoreboard);
            }
        }
        TableToken originalToken = task.getTableToken();
        TableToken liveToken = engine.getTableTokenIfExists(originalToken.getTableName());
        if (liveToken == null || liveToken.getTableId() != originalToken.getTableId()) {
            LOG.info().$("posting seal purge: table no longer exists, skipping [table=")
                    .$(originalToken.getTableName())
                    .I$();
            return true;
        }
        if (txnScoreboard != null && !txnScoreboard.getTableToken().equals(liveToken)) {
            txnScoreboard = Misc.free(txnScoreboard);
        }
        if (txnScoreboard == null) {
            try {
                txnScoreboard = engine.getTxnScoreboard(liveToken);
            } catch (Throwable th) {
                LOG.error().$("posting seal purge: cannot acquire scoreboard, retrying [table=")
                        .$(liveToken.getTableName())
                        .$(", err=").$(th)
                        .I$();
                return false;
            }
        }

        boolean safe;
        try {
            safe = txnScoreboard.isRangeAvailable(task.getFromTableTxn(), task.getToTableTxn());
        } catch (CairoException ex) {
            LOG.error().$("posting seal purge: scoreboard query failed, retrying [table=")
                    .$(liveToken.getTableName())
                    .$(", column=").$(task.getIndexColumnName())
                    .$(", from=").$(task.getFromTableTxn())
                    .$(", to=").$(task.getToTableTxn())
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .I$();
            return false;
        }
        if (!safe) {
            return false;
        }

        path.trimTo(pathRootLen).concat(liveToken.getDirName());
        int pathTableLen = path.size();
        TableUtils.setPathForNativePartition(
                path,
                task.getTimestampType(),
                task.getPartitionBy(),
                task.getPartitionTimestamp(),
                task.getPartitionNameTxn()
        );
        int pathPartitionLen = path.size();

        if (task.getArtifactForm() == PostingSealPurgeTask.ARTIFACT_FORM_PARQUET) {
            // The parquet form of the retired version: <col>.pidx.<indexTxn>.parquet
            // and its ._im. No reuse guard is needed here and none would be
            // meaningful: the index txn is a table txn, which is monotonic and
            // never handed out twice, so a superseded pidx pair can never become
            // live again. The scoreboard window this task carries -- [0, the txn
            // the replacing footer became visible at) -- is the whole proof.
            //
            // The _im goes first. It is the parquet form's commit signal -- the
            // token in the _pm names an _im file size, and the reader resolves the
            // parquet only through it -- so a crash between the two unlinks leaves
            // a parquet with no _im, which the writer's orphan sweep reclaims. The
            // other order would leave an _im naming a parquet that is gone.
            boolean removed = true;
            path.trimTo(pathPartitionLen);
            if (!ff.removeQuiet(ParquetIndexSeal.indexMetaFileName(path, task.getIndexColumnName(), task.getSealTxn()))) {
                removed = false;
            }
            path.trimTo(pathPartitionLen);
            if (!ff.removeQuiet(ParquetIndexSeal.indexParquetFileName(path, task.getIndexColumnName(), task.getSealTxn()))) {
                removed = false;
            }
            if (removed) {
                LOG.info().$("purged parquet-form posting sealed version [table=").$(liveToken.getTableName())
                        .$(", column=").$(task.getIndexColumnName())
                        .$(", indexTxn=").$(task.getSealTxn())
                        .$(", partitionTs=").$ts(task.getPartitionTimestamp())
                        .$(", partitionNameTxn=").$(task.getPartitionNameTxn())
                        .I$();
            }
            path.trimTo(pathTableLen);
            return removed;
        }

        // Native form from here down: <col>.pv.<postingColumnNameTxn>.<sealTxn>
        // and its <col>.pc*.<sealTxn> covers. Nothing below may touch a pidx
        // artifact -- this task's window is drawn from the native chain's
        // supersession point and says nothing about which readers can still
        // resolve a covering index txn that happens to carry the same number.

        // Liveness guard against sealTxn reuse -- the C1 endgame the publish-time
        // head guard cannot cover. A staged-but-never-published sealTxn whose
        // [0, MAX) orphan purge was queued can be REUSED after the failing writer
        // is freed and reopened: peekNextSealTxn() returns the same value because
        // genCounter only advances on a successful publish, so a later seal then
        // publishes a live chain head at that sealTxn. The publish-time guard
        // could not catch it because the chain head was still the OLD sealTxn when
        // this orphan entry drained. Re-read the on-disk .pk head sealTxn now, at
        // delete time: if it equals this task's sealTxn the .pv is live again, so
        // abandon the orphan purge instead of deleting a chain-referenced file
        // (recovery would otherwise need a REINDEX). The reused file's own later
        // supersession queues a fresh purge with a correct reader window, so
        // abandoning loses nothing. readSealTxnFromKeyFile returns -1 when the .pk
        // is missing or unreadable -- treat that as "cannot prove live" and fall
        // through to the scoreboard-gated delete so a genuine orphan (no live .pk)
        // stays reclaimable.
        path.trimTo(pathPartitionLen);

        long liveHeadSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                ff, PostingIndexUtils.keyFileName(path, task.getIndexColumnName(), task.getPostingColumnNameTxn()));
        path.trimTo(pathPartitionLen);
        if (liveHeadSealTxn == task.getSealTxn()) {
            LOG.critical().$("posting seal purge: target sealTxn is the live chain head (reused), abandoning orphan purge to avoid deleting a live file [table=")
                    .$(liveToken.getTableName())
                    .$(", column=").$(task.getIndexColumnName())
                    .$(", postingColumnNameTxn=").$(task.getPostingColumnNameTxn())
                    .$(", sealTxn=").$(task.getSealTxn())
                    .I$();
            return true;
        }

        boolean allRemoved = true;
        path.trimTo(pathPartitionLen);
        LPSZ pv = PostingIndexUtils.valueFileName(path, task.getIndexColumnName(),
                task.getPostingColumnNameTxn(), task.getSealTxn());
        // removeQuiet also returns true when the .pv never existed (ENOENT), so
        // pvRemoved means "absent afterward", not "a live file was unlinked"; the
        // exists() + head re-read below is what isolates the genuine live-unlink case.
        boolean pvRemoved = ff.removeQuiet(pv);
        if (!pvRemoved) {
            allRemoved = false;
        }

        // Post-unlink reuse-race detection. The pre-unlink head read and the
        // removeQuiet above are not atomic against a concurrent seal, so a writer
        // that REUSED this sealTxn (peekNextSealTxn does not advance without a
        // publish) could have created and published .pv.{sealTxn} as the live chain
        // head inside that window -- in which case removeQuiet just dropped a
        // chain-referenced file (on Linux it unlinks the name even while the writer
        // holds the fd open). Re-read the head AND re-stat the .pv to separate that
        // from the benign case (we deleted a genuine orphan and a later reuse
        // re-created a fresh, still-present .pv.{sealTxn}): only "head == sealTxn
        // AND the .pv we removed no longer exists" means we unlinked a live,
        // non-recreated file. The Windows path is self-protecting -- removeQuiet
        // fails on an open mapped file, so pvRemoved is false and the .pv survives.
        // The unlink cannot be undone; this is a loud, no-false-positive REINDEX
        // diagnostic so a silent deletion cannot surface later as an opaque hard
        // reader failure. Fully closing the window would require serializing the
        // unlink against the writer's seal, which this background job does not hold.
        if (pvRemoved) {
            path.trimTo(pathPartitionLen);
            boolean pvStillGone = !ff.exists(PostingIndexUtils.valueFileName(path, task.getIndexColumnName(),
                    task.getPostingColumnNameTxn(), task.getSealTxn()));
            path.trimTo(pathPartitionLen);
            if (pvStillGone) {
                long postUnlinkHeadSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                        ff, PostingIndexUtils.keyFileName(path, task.getIndexColumnName(), task.getPostingColumnNameTxn()));
                path.trimTo(pathPartitionLen);
                if (postUnlinkHeadSealTxn == task.getSealTxn()) {
                    LOG.critical().$("posting seal purge: sealTxn became the live chain head during the orphan unlink (reuse race) and its .pv was deleted while live; REINDEX this column/partition [table=")
                            .$(liveToken.getTableName())
                            .$(", column=").$(task.getIndexColumnName())
                            .$(", postingColumnNameTxn=").$(task.getPostingColumnNameTxn())
                            .$(", sealTxn=").$(task.getSealTxn())
                            .I$();
                }
            }
        }

        scanAllCoversRemoved = true;
        scanAnyCoverRemoved = false;
        scanTargetSealTxn = task.getSealTxn();
        scanTargetPostingTxn = task.getPostingColumnNameTxn();
        scanColumnName = task.getIndexColumnName();
        scanPartitionPathLen = pathPartitionLen;
        path.trimTo(pathPartitionLen);
        PostingIndexUtils.scanSealedFiles(ff, path, pathPartitionLen, scanColumnName, this);
        path.trimTo(pathPartitionLen);

        // Post-unlink reuse-race detection for the cover (.pc) files, the analogue of
        // the .pv check above. The pre-unlink head guard abandons the whole task when
        // the head already equals this sealTxn, but it is not atomic against a writer
        // that REUSES this sealTxn and republishes .pc.{sealTxn} live in the window
        // between that guard and the scan's directory snapshot -- onCoverDataFile would
        // then unlink a live cover file. Re-stat a removed cover AND re-read the head
        // after the scan to separate that from the benign case (we deleted a genuine
        // orphan and a later reuse re-created a fresh, still-present .pc): a republished
        // sealTxn recreates the whole .pc set via openSidecarFiles, so only "the removed
        // .pc is still gone AND head == sealTxn" means a live, non-recreated cover was
        // unlinked. Losing a .pc is less severe than losing the .pv -- the reader falls
        // back to the base column rather than hard-failing -- but it is still a silent
        // live-unlink, so surface the same REINDEX diagnostic. readSealTxnFromKeyFile
        // returns -1 on a missing/unreadable .pk, which never equals a valid sealTxn, so
        // a genuine orphan (no live .pk) does not trip this.
        if (scanAnyCoverRemoved) {
            path.trimTo(pathPartitionLen);
            boolean coverStillGone = !ff.exists(PostingIndexUtils.coverDataFileName(
                    path, task.getIndexColumnName(), scanRemovedCoverIncludeIdx,
                    task.getPostingColumnNameTxn(), scanRemovedCoverColumnNameTxn, task.getSealTxn()));
            path.trimTo(pathPartitionLen);
            if (coverStillGone) {
                long postScanHeadSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                        ff, PostingIndexUtils.keyFileName(path, task.getIndexColumnName(), task.getPostingColumnNameTxn()));
                path.trimTo(pathPartitionLen);
                if (postScanHeadSealTxn == task.getSealTxn()) {
                    LOG.critical().$("posting seal purge: sealTxn became the live chain head during the orphan cover-file unlink (reuse race) and its .pc was deleted while live; REINDEX this column/partition [table=")
                            .$(liveToken.getTableName())
                            .$(", column=").$(task.getIndexColumnName())
                            .$(", postingColumnNameTxn=").$(task.getPostingColumnNameTxn())
                            .$(", sealTxn=").$(task.getSealTxn())
                            .I$();
                }
            }
        }

        boolean done = allRemoved && scanAllCoversRemoved;
        if (done) {
            LOG.info().$("purged posting sealed version [table=").$(liveToken.getTableName())
                    .$(", column=").$(scanColumnName)
                    .$(", postingColumnNameTxn=").$(task.getPostingColumnNameTxn())
                    .$(", sealTxn=").$(task.getSealTxn())
                    .$(", partitionTs=").$ts(task.getPartitionTimestamp())
                    .$(", partitionNameTxn=").$(task.getPartitionNameTxn())
                    .I$();
        }
        scanColumnName = null;
        path.trimTo(pathTableLen);
        return done;
    }

}
