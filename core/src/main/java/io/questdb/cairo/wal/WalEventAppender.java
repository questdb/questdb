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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;

/** Event-only WAL segments with the same identity, locking and purge lifecycle as row WALs. */
public final class WalEventAppender extends WalWriterBase implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(WalEventAppender.class);
    private boolean hasUnsyncedDirectory;
    private int pendingSegmentTxn = -1;
    private long segmentRowCount;

    public WalEventAppender(CairoEngine engine, TableToken tableToken) {
        super(engine.getConfiguration(), tableToken, engine.getTableSequencerAPI(), engine.getWalDirectoryPolicy(), engine.getWalLocker(),
                engine.getTableSequencerAPI().getNextEventWalId(tableToken));
        try {
            lockWal();
            mkWalDir();
            openSegment();
        } catch (Throwable th) {
            distressed = true;
            Misc.free(this, th);
            throw th;
        }
    }

    public int appendDeltaRun(long minTimestamp, long maxTimestamp, long rowCount, WalEventPayloadWriter symbols) {
        if (distressed || pendingSegmentTxn >= 0) {
            throw CairoException.critical(0).put("Delta event writer is not ready [table=").put(tableToken).put(']');
        }
        try {
            if (rollSegmentOnNextRow) {
                openSegment();
            }
            pendingSegmentTxn = events.appendDeltaRun(minTimestamp, maxTimestamp, rowCount, symbols);
            events.sync();
            if (hasUnsyncedDirectory && configuration.getCommitMode() == CommitMode.SYNC && !Os.isWindows()) {
                // The event files and each newly created parent must survive sequencer acceptance.
                final Path syncPath = Path.getThreadLocal(configuration.getDbRoot()).concat(tableToken).concat(walName).slash().put(segmentId);
                ff.fsyncAndClose(TableUtils.openRONoCache(ff, syncPath.$(), LOG));
                ff.fsyncAndClose(TableUtils.openRONoCache(ff, syncPath.parent().$(), LOG));
                ff.fsyncAndClose(TableUtils.openRONoCache(ff, syncPath.parent().$(), LOG));
                hasUnsyncedDirectory = false;
            }
            segmentRowCount += rowCount;
            final long sizeLimit = configuration.getWalSegmentRolloverSize();
            rollSegmentOnNextRow = segmentRowCount >= configuration.getWalSegmentRolloverRowCount()
                    || sizeLimit > 0 && events.size() >= sizeLimit || pendingSegmentTxn == Integer.MAX_VALUE - 1;
            return pendingSegmentTxn;
        } catch (Throwable th) {
            distressed = true;
            throw th;
        }
    }

    @Override
    public void close() {
        if (open) {
            open = false;
            try {
                // An unknown acceptance outcome must never truncate an event another process may recover.
                events.close(false, Vm.TRUNCATE_TO_POINTER);
                if (minSegmentLocked >= 0 && segmentId >= 0) {
                    notifySegmentClosure(lastSegmentTxn, segmentId);
                }
            } finally {
                try {
                    if (minSegmentLocked >= 0) {
                        releaseWalLock();
                        minSegmentLocked = -1;
                        notifyWalClosure();
                    }
                } finally {
                    Misc.free(path);
                }
            }
        }
    }

    public void markAccepted(long seqTxn) {
        lastSeqTxn = seqTxn;
        lastSegmentTxn = pendingSegmentTxn;
        pendingSegmentTxn = -1;
    }

    public void updateTableToken(TableToken token) {
        tableToken = token;
    }

    @Override
    void syncSegmentFilesBeforeSequencing(int commitMode) {
        // A Delta event segment holds the event files only.
    }

    private void openSegment() {
        final int oldSegment = segmentId;
        final int oldLastTxn = lastSegmentTxn;
        final int newSegment = oldSegment + 1;
        final int length = createSegmentDir(newSegment);
        events.openEventFile(path, length, false, false);
        segmentId = newSegment;
        lastSegmentTxn = -1;
        segmentRowCount = 0;
        rollSegmentOnNextRow = false;
        hasUnsyncedDirectory = true;
        if (oldSegment >= 0) {
            moveMinSegmentLock(newSegment);
            notifySegmentClosure(oldLastTxn, oldSegment);
        }
    }
}
