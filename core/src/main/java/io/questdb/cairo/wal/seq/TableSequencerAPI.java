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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ObjHashSet;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.function.Function;

import static io.questdb.cairo.wal.ApplyWal2TableJob.WAL_2_TABLE_RESUME_REASON;

/**
 * Facade for WAL table sequencer operations. Delegates remote-capable operations
 * to a pluggable {@link SequencerService} and manages local-only concerns such as
 * suspend/resume state, transaction tracking, and WAL listener notifications.
 */
public class TableSequencerAPI implements QuietCloseable, SequencerServiceListener {
    private final Function<CharSequence, SeqTxnTracker> createTxnTracker;
    private final CairoEngine engine;
    private final ConcurrentHashMap<SeqTxnTracker> seqTxnTrackers = new ConcurrentHashMap<>(false);
    private final SequencerService sequencerService;

    public TableSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
        this.engine = engine;
        this.createTxnTracker = dir -> new SeqTxnTracker(configuration);
        SequencerServiceFactory factory = configuration.getSequencerServiceFactory();
        if (factory != null) {
            this.sequencerService = factory.create(engine, configuration);
        } else {
            this.sequencerService = new LocalSequencerService(engine, configuration, seqTxnTrackers, createTxnTracker);
        }
        this.sequencerService.setListener(this);
    }

    public void applyRename(TableToken tableToken) {
        sequencerService.applyRename(tableToken);
    }

    @Override
    public void close() {
        seqTxnTrackers.clear();
        sequencerService.close();
    }

    @TestOnly
    public void closeSequencer(TableToken tableToken) {
        if (sequencerService instanceof LocalSequencerService) {
            ((LocalSequencerService) sequencerService).closeSequencer(tableToken);
        }
    }

    // ---- SequencerServiceListener callbacks ----
    // These handle local state (SeqTxnTracker) setup in response to sequencer events.
    // In multi-primary mode, these fire for both locally-initiated and remote events.

    public void forAllWalTables(ObjHashSet<TableToken> tableTokenBucket, boolean includeDropped, SequencerService.TableSequencerCallback callback) {
        sequencerService.forAllWalTables(tableTokenBucket, includeDropped, callback);
    }

    public int getCurrentWalId(final TableToken tableToken) {
        if (sequencerService instanceof LocalSequencerService) {
            return ((LocalSequencerService) sequencerService).getCurrentWalId(tableToken);
        }
        throw new UnsupportedOperationException("getCurrentWalId is only supported with LocalSequencerService");
    }

    public @NotNull TransactionLogCursor getCursor(final TableToken tableToken, long seqTxn) {
        return sequencerService.getCursor(tableToken, seqTxn);
    }

    public @NotNull TableMetadataChangeLog getMetadataChangeLog(final TableToken tableToken, long structureVersionLo) {
        return sequencerService.getMetadataChangeLog(tableToken, structureVersionLo);
    }

    public TableMetadataChangeLog getMetadataChangeLogSlow(final TableToken tableToken, long structureVersionLo) {
        if (sequencerService instanceof LocalSequencerService) {
            return ((LocalSequencerService) sequencerService).getMetadataChangeLogSlow(tableToken, structureVersionLo);
        }
        // For remote implementations, getMetadataChangeLog is always authoritative
        return sequencerService.getMetadataChangeLog(tableToken, structureVersionLo);
    }

    public int getNextWalId(final TableToken tableToken) {
        return sequencerService.getNextWalId(tableToken);
    }

    public SequencerService getSequencerService() {
        return sequencerService;
    }

    public long getTableMetadata(final TableToken tableToken, final TableRecordMetadataSink sink) {
        return sequencerService.getTableMetadata(tableToken, sink);
    }

    @NotNull
    public SeqTxnTracker getTxnTracker(TableToken tableToken) {
        return getSeqTxnTracker(tableToken);
    }

    public boolean initTxnTracker(TableToken tableToken, long writerTxn, long seqTxn) {
        SeqTxnTracker seqTxnTracker = getSeqTxnTracker(tableToken);
        final boolean isSuspended = isSuspended(tableToken);
        return seqTxnTracker.initTxns(writerTxn, seqTxn, isSuspended);
    }

    public boolean isSuspended(final TableToken tableToken) {
        return getSeqTxnTracker(tableToken).isSuspended();
    }

    public boolean isTxnTrackerInitialised(final TableToken tableToken) {
        return getSeqTxnTracker(tableToken).isInitialised();
    }

    public long lastTxn(final TableToken tableToken) {
        return sequencerService.lastTxn(tableToken);
    }

    public long nextStructureTxn(final TableToken tableToken, long structureVersion, AlterOperation alterOp) {
        return sequencerService.nextStructureTxn(tableToken, structureVersion, alterOp);
    }

    public long nextTxn(final TableToken tableToken, int walId, long expectedSchemaVersion, int segmentId, int segmentTxn, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount) {
        return sequencerService.nextTxn(tableToken, walId, expectedSchemaVersion, segmentId, segmentTxn, txnMinTimestamp, txnMaxTimestamp, txnRowCount);
    }

    public boolean notifyOnCheck(TableToken tableToken, long seqTxn) {
        return getSeqTxnTracker(tableToken).notifyOnCheck(seqTxn);
    }

    public void notifySegmentClosed(TableToken tableToken, long txn, int walId, int segmentId) {
        engine.getWalListener().segmentClosed(tableToken, txn, walId, segmentId);
    }

    public void notifyWalClosed(TableToken tableToken, long txn, int walId) {
        engine.getWalListener().walClosed(tableToken, txn, walId);
    }

    @Override
    public void onTableDropped(TableToken tableToken, long databaseVersion) {
        getSeqTxnTracker(tableToken).notifyOnDrop();
    }

    @Override
    public void onTableRegistered(TableToken tableToken, long databaseVersion) {
        getSeqTxnTracker(tableToken).initTxns(0, 0, false);
    }

    @Override
    public void onTableRenamed(TableToken oldToken, TableToken newToken, long databaseVersion) {
        // SeqTxnTracker is keyed by dirName, which doesn't change on rename
    }

    @Override
    public void onTransactionsAvailable(TableToken tableToken, long seqTxn) {
        // Transaction tracking is handled by TableSequencerImpl.notifyTxnCommitted()
        // in the local case. For remote, this would trigger ApplyWal2TableJob.
    }

    @TestOnly
    public void openSequencer(TableToken tableToken) {
        if (sequencerService instanceof LocalSequencerService) {
            ((LocalSequencerService) sequencerService).openSequencer(tableToken);
        }
    }

    public boolean prepareToConvertToNonWal(final TableToken tableToken) {
        if (sequencerService instanceof LocalSequencerService) {
            return ((LocalSequencerService) sequencerService).prepareToConvertToNonWal(tableToken);
        }
        throw new UnsupportedOperationException("prepareToConvertToNonWal is only supported with LocalSequencerService");
    }

    public void purgeTxnTracker(String dirName) {
        seqTxnTrackers.remove(dirName);
    }

    public boolean releaseAll() {
        seqTxnTrackers.clear();
        return sequencerService.releaseAll();
    }

    public boolean releaseInactive() {
        return sequencerService.releaseInactive();
    }

    public TableToken reload(TableToken tableToken) {
        return sequencerService.reload(tableToken);
    }

    public void reloadMetadataConditionally(
            final TableToken tableToken,
            long expectedStructureVersion,
            TableRecordMetadataSink sink
    ) {
        if (sequencerService instanceof LocalSequencerService) {
            ((LocalSequencerService) sequencerService).reloadMetadataConditionally(tableToken, expectedStructureVersion, sink);
        } else {
            // For remote: always reload
            sequencerService.getTableMetadata(tableToken, sink);
        }
    }

    public void resumeTable(TableToken tableToken, long resumeFromTxn) {
        if (!isSuspended(tableToken)) {
            // Even if the table already unsuspended, send ApplyWal2TableJob notification anyway
            // as a way to resume table which is not moving even if it's marked as not suspended.
            engine.notifyWalTxnCommitted(tableToken);
            getSeqTxnTracker(tableToken).setUnsuspended();
            return;
        }
        final long nextTxn = sequencerService.lastTxn(tableToken) + 1;
        if (resumeFromTxn > nextTxn) {
            throw CairoException.nonCritical().put("resume txn is higher than next available transaction [resumeFromTxn=").put(resumeFromTxn).put(", nextTxn=").put(nextTxn).put(']');
        }
        // resume from the latest on negative value
        if (resumeFromTxn > 0) {
            try (TableWriter tableWriter = engine.getWriter(tableToken, WAL_2_TABLE_RESUME_REASON)) {
                long seqTxn = tableWriter.getAppliedSeqTxn();
                if (resumeFromTxn - 1 > seqTxn) {
                    // including resumeFromTxn
                    tableWriter.commitSeqTxn(resumeFromTxn - 1);
                }
            }
        }
        engine.notifyWalTxnCommitted(tableToken);
        getSeqTxnTracker(tableToken).setUnsuspended();
    }

    @TestOnly
    public void setDistressed(TableToken tableToken) {
        if (sequencerService instanceof LocalSequencerService) {
            ((LocalSequencerService) sequencerService).setDistressed(tableToken);
        }
    }

    public void suspendTable(final TableToken tableToken, ErrorTag errorTag, String errorMessage) {
        getSeqTxnTracker(tableToken).setSuspended(errorTag, errorMessage);
    }

    public boolean updateWriterTxns(final TableToken tableToken, long writerTxn, long dirtyWriterTxn) {
        return getSeqTxnTracker(tableToken).updateWriterTxns(writerTxn, dirtyWriterTxn);
    }

    @NotNull
    private SeqTxnTracker getSeqTxnTracker(TableToken tt) {
        return seqTxnTrackers.computeIfAbsent(tt.getDirName(), createTxnTracker);
    }

    @FunctionalInterface
    public interface TableSequencerCallback extends SequencerService.TableSequencerCallback {
    }
}
