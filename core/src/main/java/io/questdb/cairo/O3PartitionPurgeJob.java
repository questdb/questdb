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

import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Job;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Vect;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.O3PartitionPurgeTask;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

public class O3PartitionPurgeJob extends AbstractQueueConsumerJob<O3PartitionPurgeTask> implements Closeable {

    private final static Log LOG = LogFactory.getLog(O3PartitionPurgeJob.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final Utf8StringSink fileNameSink;
    private final AtomicBoolean halted = new AtomicBoolean(false);
    private final DirectLongList partitionList;
    private final TxReader txnReader;

    public O3PartitionPurgeJob(CairoEngine engine) {
        super(engine.getMessageBus().getO3PurgeDiscoveryQueue(), engine.getMessageBus().getO3PurgeDiscoverySubSeq());
        try {
            this.engine = engine;
            this.configuration = engine.getMessageBus().getConfiguration();
            // Single-instance per-iteration scratch. Under continuation rotation
            // the framework mints a fresh instance per snapshot via
            // cloneInstance(); concurrent access to this instance's scratch
            // is therefore impossible.
            this.fileNameSink = new Utf8StringSink();
            this.partitionList = new DirectLongList(
                    configuration.getPartitionPurgeListCapacity() * 2L,
                    MemoryTag.NATIVE_O3
            );
            this.txnReader = new TxReader(configuration.getFilesFacade());
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Legacy constructor kept for callers that still pass a workerCount
     * (pool-sizing hint). The hint is ignored; per-iteration scratch is
     * single-instance now.
     */
    public O3PartitionPurgeJob(CairoEngine engine, int workerCount) {
        this(engine);
    }

    @Override
    public Job cloneInstance() {
        return new O3PartitionPurgeJob(engine);
    }

    @Override
    public void close() {
        if (halted.compareAndSet(false, true)) {
            Misc.free(partitionList);
            Misc.free(txnReader);
        }
    }

    @Override
    public void closeInstance() {
        // cloneInstance() mints a fresh job per generation, so the pool frees
        // each instance's native scratch through this hook at halt. The halted
        // CAS in close() keeps the call idempotent.
        close();
    }

    private static void parsePartitionDateVersion(
            Utf8StringSink fileNameSink,
            DirectLongList partitionList,
            TableToken tableToken,
            DateFormat partitionByFormat
    ) {
        int index = Utf8s.lastIndexOfAscii(fileNameSink, '.');

        int len = fileNameSink.size();
        if (index < 0) {
            index = len;
        }
        try {
            if (index < len) {
                long partitionVersion = Numbers.parseLong(fileNameSink, index + 1, len);
                // When reader locks transaction 100 it opens a partition version .99 or lower.
                // Also, when there is no transaction version in the name, it is counted as -1.
                // By adding +1 here we kill 2 birds in with one stone, partition versions are aligned with
                // txn scoreboard reader locks and no need to add -1 that allows us to use 128bit
                // sort to sort 2 x 64bit unsigned integers
                partitionList.add(partitionVersion + 1);
            } else {
                // This should be -1, but it is only possible to correctly sort 2 unsigned longs
                // as 128bit integer sort
                // Set 0 instead of -1 and revert it later on. There should be not possible to have .0 in the partition name
                partitionList.add(0);
            }

            try {
                long partitionTs = partitionByFormat.parse(fileNameSink.asAsciiCharSequence(), 0, index, EN_LOCALE);
                partitionList.add(partitionTs);
            } catch (NumericException e) {
                // A live view's table directory holds _checkpoints alongside its
                // partitions, so without it here every discovery pass logs one
                // "unknown directory" line per live view.
                if (!Utf8s.startsWithAscii(fileNameSink, WalUtils.WAL_NAME_BASE) && !Utf8s.equalsAscii(WalUtils.SEQ_DIR, fileNameSink)
                        && !Utf8s.equalsAscii("seq", fileNameSink)
                        && !Utf8s.equalsAscii(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME, fileNameSink)) {
                    LOG.info().$("unknown directory [table=").$(tableToken).$(", dir=").$(fileNameSink).I$();
                }
                partitionList.setPos(partitionList.size() - 1); // remove partition version record
            }
        } catch (NumericException e) {
            LOG.error().$("unknown directory [table=").$(tableToken).$(", dir=").$(fileNameSink).I$();
        }
    }

    private void discoverPartitions(
            FilesFacade ff,
            Utf8StringSink fileNameSink,
            DirectLongList partitionList,
            CharSequence root,
            TableToken tableToken,
            TxReader txReader,
            int timestampType,
            int partitionBy
    ) {
        LOG.info().$("processing [table=").$(tableToken).I$();
        Path path = Path.getThreadLocal(root).concat(tableToken);
        int plimit = path.size();
        partitionList.clear();
        DateFormat partitionByFormat = PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy);
        long p = ff.findFirst(path.$());
        if (p > 0) {
            try {
                do {
                    if (ff.isDirOrSoftLinkDirNoDots(path, plimit, ff.findName(p), ff.findType(p), fileNameSink)) {
                        parsePartitionDateVersion(fileNameSink, partitionList, tableToken, partitionByFormat);
                        path.trimTo(plimit).$();
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }

        // find duplicate partitions
        assert partitionList.size() % 2 == 0;
        Vect.sort128BitAscInPlace(partitionList.getAddress(), partitionList.size() / 2);

        long partitionTimestamp = Numbers.LONG_NULL;
        int lo = 0;
        int n = (int) partitionList.size();

        path.of(root).concat(tableToken);

        int tableRootLen = path.size();
        TxnScoreboard txnScoreboard = null;
        try {
            txnScoreboard = engine.getTxnScoreboard(tableToken);
            txReader.ofRO(path.trimTo(tableRootLen).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

            // Composite (ts, cellKey) gate: this whole method enumerates the table root's directories
            // BY DAY ONLY -- one raw index probed per distinct day timestamp, always at cellKey 0
            // (findAttachedPartitionRawIndexByLoTimestamp(day) == ...By(day, 0), see that method's own
            // docs). For a REAL composite table -- one whose cells were actually routed by Plan 4a's
            // write path -- a day whose cells DON'T include cellKey 0 makes that probe return <0 (not
            // found) even though the day IS attached under a different cellKey, so this misclassifies a
            // perfectly live day directory as DETACHED (processDetachedPartition) and recursively
            // deletes it (purgePartition -> ff.unlinkOrRemove) while _txn still references every row in
            // it: silent data loss. txReader.getLongsPerAttachedPartition() is this exact table's own
            // self-describing _txn stride marker (Plan 3b Tasks 1+3 -- authoritative from CREATE,
            // symmetric on every load, just read fresh above by safeReadTxn/unsafeLoadAll), so it is 8
            // (COMPOSITE) iff this table was declared composite (dimCount>0), regardless of whether it
            // has ever actually used more than one cell -- skipping a dormant composite table too is
            // conservative, not a correctness requirement (a cell-aware purge is deferred to Plan 4b).
            // Plain tables always read 4 here and are completely unaffected -- same idiom as
            // TableWriter#repairDataGaps's own composite gate.
            if (txReader.getLongsPerAttachedPartition() > TableUtils.LONGS_PER_TX_ATTACHED_PARTITION) {
                // The day-blind walk above is unusable here, but doing NOTHING is not free either:
                // the deferred arm of TableWriter#processPartitionRemoveCandidates0 hands its work to
                // this job, so for a composite table that arm had no consumer at all and a superseded
                // cell version that missed its inline removal was never reclaimed. On Windows that is
                // routine rather than rare -- a directory holding a mapped file cannot be deleted, so
                // the inline removal fails with errno=5 whenever any reader is on the cell. CI caught
                // it as a day settling at three directories, `exch=E0.8, exch=E1.4, exch=E1.8`, where
                // E1.4 is a superseded version of the cell E1.8 replaced.
                purgeCompositeSupersededCellVersions(
                        ff, fileNameSink, root, tableToken, txReader, txnScoreboard,
                        timestampType, partitionBy, path, tableRootLen
                );
                return;
            }

            for (int i = 0; i < n; i += 2) {
                long currentPartitionTs = partitionList.get(i + 1);
                if (currentPartitionTs != partitionTimestamp) {
                    if (i > lo + 2 ||
                            (i > 0 && txReader.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp) < 0)) {
                        processPartition(
                                tableToken,
                                ff,
                                path,
                                tableRootLen,
                                txReader,
                                txnScoreboard,
                                partitionTimestamp,
                                timestampType,
                                partitionBy,
                                partitionList,
                                lo,
                                i
                        );
                    }
                    lo = i;
                    partitionTimestamp = currentPartitionTs;
                }
            }
            // Tail
            if (n > lo + 2 || txReader.getPartitionRowCountByTimestamp(partitionTimestamp) < 0) {
                processPartition(
                        tableToken,
                        ff,
                        path,
                        tableRootLen,
                        txReader,
                        txnScoreboard,
                        partitionTimestamp,
                        timestampType,
                        partitionBy,
                        partitionList,
                        lo,
                        n
                );
            }
        } catch (TableReferenceOutOfDateException e) {
            // the table is dropped and recreated since we started processing it.
            // abort the table processing
            LOG.info().$("table reference out of date, aborting [table=").$(tableToken).I$();
        } catch (CairoException ex) {
            // It is possible that the table is dropped while this async job was in the queue.
            // so it can be not too bad. Log error and continue work on the queue
            LOG.error()
                    .$("could not purge partition open [table=").$(tableToken)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            LOG.error().$safe(ex.getFlyweightMessage()).$();
        } finally {
            txReader.clear();
            Misc.free(txnScoreboard);
        }
        LOG.info().$("processed [table=").$(tableToken).I$();
    }

    private void processDetachedPartition(
            TableToken tableToken,
            FilesFacade ff,
            Path path,
            int tableRootLen,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            long partitionTimestamp,
            int timestampType,
            int partitionBy,
            DirectLongList partitionList,
            int lo,
            int hi
    ) {
        // Partition is dropped or not fully committed.
        // It is only possible to delete when there are no readers
        boolean checkpointInProgress = engine.getCheckpointStatus().isInProgress();
        long lastTxn = txReader.getTxn();
        for (int i = hi - 2, n = lo - 1; i > n; i -= 2) {
            long nameTxn = partitionList.get(i);

            // If the last committed transaction number is 4, TableWriter can write partition with ending .4 and .3
            // If the version on disk is .2 (nameTxn == 3) can remove it if the lastTxn > 3, e.g., when nameTxn < lastTxn
            // When a backup checkpoint is in progress, skip deletion — the checkpoint may reference
            // these partitions via snapshotted metadata even if the scoreboard is not pinned yet.
            boolean rangeUnlocked = !checkpointInProgress
                    && nameTxn < lastTxn && txnScoreboard.isRangeAvailable(nameTxn, lastTxn);

            path.trimTo(tableRootLen);
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn - 1);
            path.$();

            if (rangeUnlocked) {
                // nameTxn can be deleted
                // -1 here being to compensate +1 added when a partition version parsed from folder name
                // See comments of why +1 added there in parsePartitionDateVersion()
                purgePartition(tableToken, ff, path, tableRootLen - tableToken.getDirNameUtf8().size() - 1, "purging dropped partition directory [path=");
                lastTxn = nameTxn;
            } else {
                LOG.debug().$("cannot purge partition directory, locked for reading [path=")
                        .$substr(tableRootLen - tableToken.getDirNameUtf8().size() - 1, path)
                        .I$();
                break;
            }
        }
    }

    private void processPartition(
            TableToken tableToken,
            FilesFacade ff,
            Path path,
            int tableRootLen,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            long partitionTimestamp,
            int timestampType,
            int partitionBy,
            DirectLongList partitionList,
            int lo,
            int hi
    ) {
        boolean partitionInTxnFile = txReader.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp) >= 0;
        if (partitionInTxnFile) {
            processPartition0(
                    tableToken,
                    ff,
                    path,
                    tableRootLen,
                    txReader,
                    txnScoreboard,
                    partitionTimestamp,
                    timestampType,
                    partitionBy,
                    partitionList,
                    lo,
                    hi
            );
        } else {
            processDetachedPartition(
                    tableToken,
                    ff,
                    path,
                    tableRootLen,
                    txReader,
                    txnScoreboard,
                    partitionTimestamp,
                    timestampType,
                    partitionBy,
                    partitionList,
                    lo,
                    hi
            );
        }
    }

    private void processPartition0(
            TableToken tableToken,
            FilesFacade ff,
            Path path,
            int tableRootLen,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            long partitionTimestamp,
            int timestampType,
            int partitionBy,
            DirectLongList partitionList,
            int lo,
            int hi
    ) {
        long lastCommittedPartitionName = txReader.getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
        if (lastCommittedPartitionName > -1) {
            assert hi <= partitionList.size();
            // When a backup checkpoint is in progress, skip deletion — the checkpoint may reference
            // these partitions via snapshotted metadata even if the scoreboard is not pinned yet.
            boolean checkpointInProgress = engine.getCheckpointStatus().isInProgress();
            // lo points to the beginning element in partitionList, hi next after last
            // each partition folder represented by a pair in the partitionList (partition version, partition timestamp)
            // Skip first pair, start from second and check if it can be deleted.
            for (int i = lo + 2; i < hi; i += 2) {
                long nextNameVersion = Math.min(lastCommittedPartitionName + 1, partitionList.get(i));
                long previousNameVersion = partitionList.get(i - 2);

                boolean rangeUnlocked = !checkpointInProgress
                        && previousNameVersion < nextNameVersion
                        && txnScoreboard.isRangeAvailable(previousNameVersion, nextNameVersion);

                // Sometimes TableWriter can create a partition folder before committing the transaction
                // and then clean it before committing because it was not necessary to do a copy on write.
                // We read partition directories before reading the txn file, so it is possible to see such partitions
                // that don't exist when the txn file was committed.
                // Check that the partition version we think we rely on indeed still exists.
                if (rangeUnlocked) {
                    path.trimTo(tableRootLen);
                    TableUtils.setPathForNativePartition(
                            path,
                            timestampType,
                            partitionBy,
                            partitionTimestamp,
                            nextNameVersion - 1
                    );
                    if (!ff.exists(path.$())) {
                        // We see some phantom partitions, the best way is to abort processing this partition
                        LOG.info().$("partition dir removed after scanning the directories, aborting processing the partition [partition=")
                                .$substr(tableRootLen - tableToken.getDirNameUtf8().size() - 1, path)
                                .I$();
                        return;
                    }
                }

                path.trimTo(tableRootLen);
                TableUtils.setPathForNativePartition(
                        path,
                        timestampType,
                        partitionBy,
                        partitionTimestamp,
                        previousNameVersion - 1
                );
                path.$();

                if (rangeUnlocked) {
                    // previousNameVersion can be deleted
                    // -1 here is to compensate +1 added when a partition version parsed from folder name
                    // See comments of why +1 added there in parsePartitionDateVersion()
                    engine.getPartitionOverwriteControl().notifyPartitionMutates(
                            tableToken,
                            timestampType,
                            partitionTimestamp,
                            previousNameVersion - 1,
                            0
                    );
                    purgePartition(
                            tableToken,
                            ff,
                            path,
                            tableRootLen - tableToken.getDirNameUtf8().size() - 1,
                            "purging overwritten partition directory [path="
                    );
                } else {
                    LOG.info().$("cannot purge overwritten partition directory, locked for reading path=")
                            .$substr(tableRootLen - tableToken.getDirNameUtf8().size() - 1, path).I$();
                }
            }
        }
    }

    /**
     * The composite counterpart of {@link #processPartition0}: reclaims a cell's SUPERSEDED version
     * directories, and nothing else.
     * <p>
     * The plain walk cannot be reused. It enumerates the table root and reads every directory as
     * {@code <day>.<nameTxn>}, but on a composite table the table root holds bare day CONTAINERS and
     * the versioned directories are one level down, {@code <day>/<segment>.<nameTxn>}. Worse, its
     * "is this day attached?" probe is {@code findAttachedPartitionRawIndexByLoTimestamp}, which is
     * hardcoded to cellKey 0, so a live day whose cells do not include cellKey 0 reads as DETACHED and
     * the whole day gets deleted while {@code _txn} still references every row in it.
     * <p>
     * <b>What this deliberately does NOT do.</b> It never removes a cell directory, nor a day
     * container, nor anything for a dropped or detached partition. A directory is a candidate only
     * when a STRICTLY NEWER version of the SAME CELL exists on disk AND {@code _txn} references that
     * newer version for that day AND {@code _txn} references the candidate's own nameTxn NOWHERE in
     * that day (see the guard's own comment for why the third clause is load-bearing rather than
     * belt-and-braces). That is the same invariant {@code processPartition0} relies on --
     * the directory being deleted is not what {@code _txn} points at, and the scoreboard says no
     * reader can still be looking at it -- applied per cell instead of per day. Everything else is
     * left alone: a cell with no live version on disk is skipped entirely rather than treated as
     * detached, because "no live version" is also what a half-finished install looks like, and this
     * job runs asynchronously against a table other threads are writing.
     * <p>
     * That conservatism also covers the one place the {@code <segment>.<nameTxn>} split is ambiguous.
     * A dimension value may itself contain a dot, so splitting at the LAST dot can put the boundary
     * in the wrong place. It cannot cause a wrong deletion: a mis-split yields a segment string that
     * matches no sibling's, the group has no live version, and the cell is skipped. The failure mode
     * of getting this wrong is leaving a directory behind, never removing a live one.
     */
    private void purgeCompositeSupersededCellVersions(
            FilesFacade ff,
            Utf8StringSink fileNameSink,
            CharSequence root,
            TableToken tableToken,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            int timestampType,
            int partitionBy,
            Path path,
            int tableRootLen
    ) {
        // Same guard the plain arms use: a checkpoint may reference these directories through
        // snapshotted metadata even when the scoreboard is not pinned.
        if (engine.getCheckpointStatus().isInProgress()) {
            return;
        }

        final DateFormat partitionByFormat = PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy);
        final ObjList<String> dayNames = new ObjList<>();
        final LongList dayTimestamps = new LongList();

        // Pass 1: the day CONTAINERS. Collected up front rather than walked in place because the
        // per-day pass below needs its own findFirst/findNext cursor.
        //
        // A PRIVATE Path, not Path.getThreadLocal: the caller's `path` -- the one this method purges
        // through -- IS the thread-local, so sharing it would clobber tableRootLen mid-walk. One
        // allocation per invocation, on a job that runs rarely.
        final Path dirPath = new Path();
        try {
            dirPath.of(root).concat(tableToken);
            final int plimit = dirPath.size();
            long p = ff.findFirst(dirPath.$());
            if (p > 0) {
                try {
                    do {
                        if (!ff.isDirOrSoftLinkDirNoDots(dirPath, plimit, ff.findName(p), ff.findType(p), fileNameSink)) {
                            continue;
                        }
                        final int len = fileNameSink.size();
                        // A day CONTAINER is unversioned. Rejecting any dot explicitly, rather than
                        // trusting the date parser to refuse the trailing ".<txn>", is the guard that
                        // keeps a stray `<day>.<txn>` at the table root out of this walk -- descending
                        // into one and treating its children as cell versions is precisely how a
                        // cell-aware purge would delete live cells.
                        if (Utf8s.lastIndexOfAscii(fileNameSink, '.') > -1) {
                            dirPath.trimTo(plimit).$();
                            continue;
                        }
                        try {
                            final long ts = partitionByFormat.parse(fileNameSink.asAsciiCharSequence(), 0, len, EN_LOCALE);
                            dayNames.add(Utf8s.toString(fileNameSink));
                            dayTimestamps.add(ts);
                        } catch (NumericException ignore) {
                            // not a day container
                        }
                        dirPath.trimTo(plimit).$();
                    } while (ff.findNext(p) > 0);
                } finally {
                    ff.findClose(p);
                }
            }

            final ObjList<String> segNames = new ObjList<>();
            final LongList segTxns = new LongList();
            final LongList liveNameTxns = new LongList();

            for (int d = 0, nd = dayNames.size(); d < nd; d++) {
                final long dayTs = dayTimestamps.getQuick(d);

                // Every nameTxn _txn holds for this day, across all of its cells. Read from the
                // per-record getters, so it is cellKey-agnostic by construction -- the trap the day-only
                // probe above falls into.
                liveNameTxns.clear();
                for (int i = 0, n2 = txReader.getPartitionCount(); i < n2; i++) {
                    if (txReader.getPartitionTimestampByIndex(i) == dayTs) {
                        liveNameTxns.add(txReader.getPartitionNameTxn(i));
                    }
                }
                if (liveNameTxns.size() == 0) {
                    // The whole day is gone from _txn. Dropped-partition cleanup is NOT this method's
                    // job (see the class of thing it refuses, above), so leave it to the writer.
                    continue;
                }

                segNames.clear();
                segTxns.clear();
                dirPath.of(root).concat(tableToken).concat(dayNames.getQuick(d));
                final int dlimit = dirPath.size();
                final long dp = ff.findFirst(dirPath.$());
                if (dp < 1) {
                    continue;
                }
                try {
                    do {
                        if (!ff.isDirOrSoftLinkDirNoDots(dirPath, dlimit, ff.findName(dp), ff.findType(dp), fileNameSink)) {
                            continue;
                        }
                        // Split on the LAST dot. Decoded to a String first: a dimension value may hold
                        // any UTF-8, so slicing the raw byte sink would need a codepoint-aware substring
                        // for no gain here -- this job runs rarely and a day has few cells.
                        final String dirName = Utf8s.toString(fileNameSink);
                        final int dot = dirName.lastIndexOf('.');
                        long nameTxn = -1L;
                        String seg = dirName;
                        if (dot > 0) {
                            try {
                                nameTxn = Numbers.parseLong(dirName, dot + 1, dirName.length());
                                seg = dirName.substring(0, dot);
                            } catch (NumericException ignore) {
                                // a value containing a dot, with no version suffix
                                nameTxn = -1L;
                                seg = dirName;
                            }
                        }
                        segNames.add(seg);
                        segTxns.add(nameTxn);
                        dirPath.trimTo(dlimit).$();
                    } while (ff.findNext(dp) > 0);
                } finally {
                    ff.findClose(dp);
                }

                for (int a = 0, na = segNames.size(); a < na; a++) {
                    final String seg = segNames.getQuick(a);
                    final long candidate = segTxns.getQuick(a);

                    // The live version OF THIS CELL: the newest version present on disk for this segment
                    // that _txn also references for this day. Matching on the segment string is what keeps
                    // cells apart -- nameTxn alone cannot, because CONVERT stamps one nameTxn across every
                    // cell of a day.
                    long live = Long.MIN_VALUE;
                    for (int b = 0; b < na; b++) {
                        if (!seg.equals(segNames.getQuick(b))) {
                            continue;
                        }
                        final long t = segTxns.getQuick(b);
                        if (t > live && liveNameTxns.indexOf(t) > -1) {
                            live = t;
                        }
                    }
                    if (live == Long.MIN_VALUE || candidate >= live) {
                        // No live version for this cell, or this IS the live one (or newer than it -- an
                        // install that has not committed yet, which must survive).
                        continue;
                    }

                    // AND the candidate's own nameTxn must be referenced by NO record of this day.
                    //
                    // Without this the rule above is unsound, because liveNameTxns is day-wide while
                    // `seg` is per-cell: a nameTxn that is live for a DIFFERENT cell can validate a
                    // stale sibling of this one. Concretely -- E0 live at 8, E1 live at 4, and a
                    // stale `exch=E1.8` left by an aborted install. `live` for E1 resolves to 8 (8 is
                    // in the day's set, via E0), candidate 4 < 8, and the LIVE `exch=E1.4` gets
                    // deleted. Requiring the candidate to appear nowhere in the day's set makes that
                    // impossible: a directory no record names cannot be any cell's live directory.
                    //
                    // Getting a per-cell answer instead would mean resolving cellKey -> segment
                    // through the cell registry (CellSegmentResolver), i.e. opening a symbol reader
                    // per dimension inside an async purge job. This is strictly weaker -- a stale
                    // version whose nameTxn happens to be live for another cell of the same day is
                    // left on disk -- and strictly safe, which is the right trade here.
                    if (liveNameTxns.indexOf(candidate) > -1) {
                        continue;
                    }

                    // +1 on both ends is the scoreboard convention the plain arms use: a reader holding
                    // txn N may have opened version N-1 or lower.
                    if (!txnScoreboard.isRangeAvailable(candidate + 1, live + 1)) {
                        LOG.debug().$("cannot purge superseded cell version, locked for reading [table=").$(tableToken)
                                .$(", day=").$(dayNames.getQuick(d)).$(", cell=").$safe(seg).$(", nameTxn=").$(candidate)
                                .I$();
                        continue;
                    }

                    path.trimTo(tableRootLen);
                    TableUtils.setPathForNativePartition(path, timestampType, partitionBy, dayTs, candidate, seg);
                    path.$();
                    if (!ff.exists(path.$())) {
                        continue;
                    }
                    engine.getPartitionOverwriteControl().notifyPartitionMutates(
                            tableToken, timestampType, dayTs, candidate, 0
                    );
                    purgePartition(
                            tableToken, ff, path,
                            tableRootLen - tableToken.getDirNameUtf8().size() - 1,
                            "purging superseded composite cell version [path="
                    );
                }
            }
        } finally {
            dirPath.close();
        }
    }

    private void purgePartition(TableToken tableToken, FilesFacade ff, Path path, int pathFrom, String message) {
        if (engine.lockTableCreate(tableToken)) {
            try {
                TableToken lastToken = engine.getUpdatedTableToken(tableToken);
                if (lastToken == tableToken) {
                    LOG.info().$(message).$substr(pathFrom, path).I$();
                    ff.unlinkOrRemove(path, LOG);
                } else {
                    // the table is dropped and recreated since we started processing it.
                    // abort the table processing
                    throw new TableReferenceOutOfDateException();
                }
            } finally {
                engine.unlockTableCreate(tableToken);
            }
        } else {
            // the table is dropped and recreated since we started processing it.
            // abort the table processing
            throw new TableReferenceOutOfDateException();
        }
    }

    @Override
    protected boolean doRun(long cursor, WorkerContext workerContext) {
        final O3PartitionPurgeTask task = queue.get(cursor);
        discoverPartitions(
                configuration.getFilesFacade(),
                fileNameSink,
                partitionList,
                configuration.getDbRoot(),
                task.getTableToken(),
                txnReader,
                task.getTimestampType(),
                task.getPartitionBy()
        );
        subSeq.done(cursor);
        return true;
    }
}
