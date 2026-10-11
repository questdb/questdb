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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.wal.seq.TableSequencer.NO_TXN;

/**
 * WAL writer specialized for views.
 * <p>
 * Views do not contain data and their schema is not persisted.
 * This writer only manages WAL event files without creating column files or the _meta file.
 * <p>
 * This class extends {@link WalWriterBase} to reuse common WAL infrastructure (segment
 * management, locking, sequencer integration) while providing view-specific operations
 * through {@link #replaceViewDefinition(String, LowerCaseCharSequenceObjHashMap)}.
 */
public class ViewWalWriter extends WalWriterBase {
    private static final Log LOG = LogFactory.getLog(ViewWalWriter.class);

    public ViewWalWriter(
            CairoConfiguration configuration,
            TableToken tableToken,
            TableSequencerAPI tableSequencerAPI,
            WalDirectoryPolicy walDirectoryPolicy,
            WalLocker walLocker
    ) {
        super(configuration, tableToken, tableSequencerAPI, walDirectoryPolicy, walLocker);

        LOG.info().$("open [table=").$(tableToken).I$();

        try {
            lockWal();
            mkWalDir();

            events.of(null, null, null, null);

            openNewSegment();
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
    }

    @Override
    public void close() {
        if (open) {
            doClose(walDirectoryPolicy.truncateFilesOnClose());
        }
    }

    public long replaceViewDefinition(
            @NotNull String viewSql,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies
    ) {
        try {
            if (rollSegmentOnNextRow) {
                rollSegment();
                rollSegmentOnNextRow = false;
            }
            lastSegmentTxn = events.appendViewDefinition(viewSql, dependencies);
            syncIfRequired();

            final long seqTxn = getSequencerTxn();
            LOG.info().$("replaced view definition [wal=").$substr(pathRootSize, path).$(Files.SEPARATOR).$(segmentId)
                    .$(", segTxn=").$(lastSegmentTxn)
                    .$(", seqTxn=").$(lastSeqTxn)
                    .I$();
            mayRollSegmentOnNextRow();
            return seqTxn;
        } catch (Throwable th) {
            distressed = true;
            throw th;
        }
    }

    public void rollSegment() {
        try {
            openNewSegment();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        }
    }

    @Override
    public String toString() {
        return "ViewWalWriter{" +
                "name=" + walName +
                ", view=" + tableToken.getTableName() +
                '}';
    }

    @Override
    void syncSegmentFilesBeforeSequencing(int commitMode) {
        // A view segment holds the event files only.
    }

    private boolean breachedRolloverSizeThreshold() {
        final long threshold = configuration.getWalSegmentRolloverSize();
        if (threshold == 0) {
            return false;
        }

        return events.size() > threshold;
    }

    private void doClose(boolean truncate) {
        if (open) {
            open = false;
            if (events != null) {
                events.close(truncate, Vm.TRUNCATE_TO_POINTER);
            }

            if (minSegmentLocked > -1) {
                notifySegmentClosure(lastSegmentTxn, minSegmentLocked);
                minSegmentLocked = -1;
            }

            try {
                releaseWalLock();
            } finally {
                Misc.free(path);
                LOG.info().$("closed [view=").$(tableToken).I$();
            }
            // must happen after the WAL lock is released
            notifyWalClosure();
        }
    }

    private long getSequencerTxn() {
        try {
            long seqTxn = nextSeqTxn(0L, Long.MAX_VALUE, -1L, 0L);
            assert seqTxn != NO_TXN;
            // Group commit leaves the log's flush to a batch that only table WAL writers run.
            if (configuration.getCommitMode() == CommitMode.ADAPTIVE && configuration.getAdaptiveCommitGroupWindowUs() > 0) {
                final SeqTxnTracker tracker = sequencer.getTxnTracker(tableToken);
                final long orphanSweepMark = tracker.snapshotOrphanSweepMark();
                tracker.markWriterDurable(walId, orphanSweepMark, sequencer.fdatasyncTxnLog(tableToken));
            }
            return lastSeqTxn = seqTxn;
        } catch (CairoException e) {
            // The log is flushed in the append or right after it. A failed flush stops the engine, as on the commit path.
            sequencer.handleDataSyncFailure(e);
            throw e;
        }
    }

    private void mayRollSegmentOnNextRow() {
        if (rollSegmentOnNextRow) {
            return;
        }
        rollSegmentOnNextRow = breachedRolloverSizeThreshold() || (lastSegmentTxn > Integer.MAX_VALUE - 2);
    }

    private void openNewSegment() {
        final int newSegmentId = segmentId + 1;
        final long oldLastSegmentTxn = lastSegmentTxn;
        long dirFd = -1;
        try {
            final int segmentPathLen = createSegmentDir(newSegmentId);
            segmentId = newSegmentId;
            final int commitMode = configuration.getCommitMode();
            if (!Os.isWindows() && commitMode != CommitMode.NOSYNC) {
                dirFd = TableUtils.openRONoCache(ff, path.$(), LOG);
            }

            events.openEventFile(path, segmentPathLen, walDirectoryPolicy.truncateFilesOnClose(), tableToken.isSystem());
            syncIfRequired();

            if (dirFd != -1) {
                final long fd = dirFd;
                // fsyncAndClose closes the fd even when the flush fails
                dirFd = -1;
                ff.fsyncAndClose(fd);
                fsyncSegmentDirEntries(commitMode, newSegmentId);
            }
            lastSegmentTxn = -1;
            path.trimTo(pathSize).slash().put(newSegmentId);
            LOG.info().$("opened WAL segment [path=").$substr(pathRootSize, path).I$();
        } finally {
            if (dirFd != -1) {
                // An event file open or flush failed before the segment directory was flushed: release its fd.
                ff.close(dirFd);
            }
            int oldMinSegmentLocked = minSegmentLocked;
            if (moveMinSegmentLock(newSegmentId)) {
                notifySegmentClosure(oldLastSegmentTxn, oldMinSegmentLocked);
            }
            path.trimTo(pathSize);
        }
    }

    private void syncIfRequired() {
        final int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            events.sync();
        }
    }
}
