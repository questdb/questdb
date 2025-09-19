/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.BinaryAlterSerializer;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.IDGenerator;
import io.questdb.cairo.IDGeneratorFactory;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.WAL_INDEX_FILE_NAME;

public class TableSequencerImpl implements TableSequencer {
    private static final Log LOG = LogFactory.getLog(TableSequencerImpl.class);
    private final static BinaryAlterSerializer alterCommandWalFormatter = new BinaryAlterSerializer();
    private final CairoEngine engine;
    private final SequencerMetadata metadata;
    private final SequencerMetadataService metadataSvc;
    private final MicrosecondClock microClock;
    private final Path path;
    private final TableSequencerAPI pool;
    private final int rootLen;
    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();
    private final SeqTxnTracker seqTxnTracker;
    private final TableTransactionLog tableTransactionLog;
    private final WalDirectoryPolicy walDirectoryPolicy;
    private final IDGenerator walIdGenerator;
    volatile long releaseTime = Long.MAX_VALUE;
    private volatile boolean closed = false;
    private boolean distressed = false;
    private TableToken tableToken;

    TableSequencerImpl(
            TableSequencerAPI pool,
            CairoEngine engine,
            TableToken tableToken,
            SeqTxnTracker txnTracker,
            int tableId,
            @Nullable TableStructure tableStruct
    ) {
        this.pool = pool;
        this.engine = engine;
        this.tableToken = tableToken;
        this.seqTxnTracker = txnTracker;
        final CairoConfiguration configuration = engine.getConfiguration();
        this.walDirectoryPolicy = engine.getWalDirectoryPolicy();
        final FilesFacade ff = configuration.getFilesFacade();
        try {
            path = new Path();
            path.of(configuration.getDbRoot());
            path.concat(tableToken.getDirName()).concat(SEQ_DIR);
            rootLen = path.size();

            metadata = new SequencerMetadata(ff, configuration.getCommitMode());
            metadataSvc = new SequencerMetadataService(metadata, tableToken);
            walIdGenerator = IDGeneratorFactory.newIDGenerator(
                    configuration,
                    WAL_INDEX_FILE_NAME,
                    configuration.getIdGenerateBatchStep() < 0 ? 512 : configuration.getIdGenerateBatchStep()
            );
            tableTransactionLog = new TableTransactionLog(configuration);
            microClock = configuration.getMicrosecondClock();
            if (tableStruct != null) {
                schemaLock.writeLock().lock();
                try {
                    createSequencerDir(ff, configuration.getMkDirMode());
                    final long timestamp = microClock.getTicks();
                    metadata.create(tableStruct, tableToken, path, rootLen, tableId);
                    tableTransactionLog.create(path, timestamp);
                    engine.getWalListener().tableCreated(tableToken, timestamp);
                } finally {
                    schemaLock.writeLock().unlock();
                }
            }
        } catch (Throwable th) {
            LOG.critical().$("could not create sequencer [name=").$(tableToken)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
        try {
            walIdGenerator.open(path);
            metadata.open(path, rootLen, tableToken);
            tableTransactionLog.open(path);
        } catch (CairoException ex) {
            closeLocked();
            if (ex.isTableDropped()) {
                throw ex;
            }
            if (ex.isFileCannotRead() && engine.isTableDropped(tableToken)) {
                LOG.info().$("could not open sequencer, table is dropped [table=").$(tableToken)
                        .$(", path=").$(path)
                        .$(", error=").$safe(ex.getMessage())
                        .I$();
                throw CairoException.tableDropped(tableToken);
            }
            LOG.critical().$("could not open sequencer [table=").$(tableToken)
                    .$(", path=").$(path)
                    .$(", errno=").$(ex.getErrno())
                    .$(", error=").$safe(ex.getMessage())
                    .I$();
            throw ex;
        } catch (Throwable th) {
            LOG.critical().$("could not open sequencer [table=").$(tableToken)
                    .$(", path=").$(path)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
    }

    public boolean checkClose() {
        if (closed) {
            return false;
        }
        schemaLock.writeLock().lock();
        try {
            return closeLocked();
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        if (pool.closed) {
            checkClose();
        } else if (!isDistressed() && !isDropped()) {
            releaseTime = pool.configuration.getMicrosecondClock().getTicks();
        } else if (checkClose()) {
            LOG.info()
                    .$("closed table sequencer [table=").$(getTableToken())
                    .$(", distressed=").$(isDistressed())
                    .$(", dropped=").$(isDropped())
                    .I$();
            // Remove from registry only if this thread closed the instance.
            pool.seqRegistry.remove(getTableToken().getDirName(), this);
        }
    }

    @Override
    public void dropTable() {
        checkDropped();
        final long timestamp = microClock.getTicks();
        final long txn = tableTransactionLog.addEntry(
                getStructureVersion(), WalUtils.DROP_TABLE_WAL_ID,
                0, 0, timestamp, 0, 0, 0
        );
        metadata.dropTable();
        notifyTxnCommitted(Long.MAX_VALUE);
        engine.getWalListener().tableDropped(tableToken, txn, timestamp);
    }

    @Override
    public TableMetadataChangeLog getMetadataChangeLog(long structureVersionLo) {
        checkDropped();
        if (metadata.getMetadataVersion() == structureVersionLo) {
            // Nothing to do.
            return EmptyOperationCursor.INSTANCE;
        }
        return tableTransactionLog.getTableMetadataChangeLog(structureVersionLo, alterCommandWalFormatter);
    }

    @Override
    public TableMetadataChangeLog getMetadataChangeLogSlow(long structureVersionLo) {
        checkDropped();
        // Do not check cached metadata version.
        return tableTransactionLog.getTableMetadataChangeLog(structureVersionLo, alterCommandWalFormatter);
    }

    @Override
    public int getNextWalId() {
        return (int) walIdGenerator.getNextId();
    }

    @Override
    public long getStructureVersion() {
        return metadata.getMetadataVersion();
    }

    @Override
    public int getTableId() {
        return metadata.getTableId();
    }

    @Override
    public long getTableMetadata(@NotNull TableRecordMetadataSink sink) {
        int columnCount = metadata.getColumnCount();
        int timestampIndex = metadata.getTimestampIndex();
        int compressedTimestampIndex = -1;
        sink.clear();

        int compressedColumnCount = 0;
        boolean reorderNeeded = false;
        int lastOrder = -1;
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            int columnOrder = metadata.getReadColumnOrder().getQuick(i);
            sink.addColumn(
                    metadata.getColumnName(i),
                    columnType,
                    metadata.isColumnIndexed(i),
                    metadata.getIndexValueBlockCapacity(i),
                    metadata.isSymbolTableStatic(i),
                    i,
                    metadata.isDedupKey(i),
                    metadata.getColumnMetadata(i).isSymbolCacheFlag(),
                    metadata.getColumnMetadata(i).getSymbolCapacity()
            );
            if (columnType > -1) {
                reorderNeeded |= lastOrder > columnOrder;
                lastOrder = columnOrder;
                if (i == timestampIndex) {
                    compressedTimestampIndex = compressedColumnCount;
                }
                compressedColumnCount++;
            }
        }

        sink.of(
                tableToken,
                metadata.getTableId(),
                timestampIndex,
                compressedTimestampIndex,
                metadata.isSuspended(),
                metadata.getMetadataVersion(),
                compressedColumnCount,
                reorderNeeded ? metadata.getReadColumnOrder() : null
        );

        return tableTransactionLog.lastTxn();
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public TransactionLogCursor getTransactionLogCursor(long seqTxn) {
        checkDropped();
        return tableTransactionLog.getCursor(seqTxn);
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isDistressed() {
        return distressed;
    }

    public boolean isDropped() {
        return metadata.isDropped();
    }

    @Override
    public long lastTxn() {
        return tableTransactionLog.lastTxn();
    }

    public boolean metadataMatches(long structureVersion) {
        return metadata.getMetadataVersion() == structureVersion;
    }

    @Override
    public long nextStructureTxn(long expectedStructureVersion, TableMetadataChange change) {
        // Writing to TableSequencer can happen from multiple threads, so we need to protect against concurrent writes.
        assert !closed;
        checkDropped();
        long txn;
        try {
            // From sequencer perspective metadata version is the same as column structure version
            if (metadata.getMetadataVersion() == expectedStructureVersion) {
                final long timestamp = microClock.getTicks();
                tableTransactionLog.beginMetadataChangeEntry(
                        expectedStructureVersion + 1, alterCommandWalFormatter, change, timestamp);

                final TableToken oldTableToken = tableToken;

                // Re-read serialised change to ensure it can be read.
                AlterOperation deserializedAlter = tableTransactionLog.readTableMetadataChangeLog(
                        expectedStructureVersion, alterCommandWalFormatter);

                applyToMetadata(deserializedAlter);
                if (metadata.getMetadataVersion() != expectedStructureVersion + 1) {
                    throw CairoException.critical(0)
                            .put("applying structure change to WAL table failed [table=").put(tableToken)
                            .put(", oldVersion: ").put(expectedStructureVersion)
                            .put(", newVersion: ").put(metadata.getMetadataVersion())
                            .put(']');
                }
                metadata.sync();
                // TableToken can become updated as a result of alter.
                tableToken = metadata.getTableToken();
                txn = tableTransactionLog.endMetadataChangeEntry();

                if (!metadata.isSuspended()) {
                    notifyTxnCommitted(txn);
                    if (!tableToken.equals(oldTableToken)) {
                        engine.getWalListener().tableRenamed(tableToken, txn, timestamp, oldTableToken);
                    } else {
                        engine.getWalListener().nonDataTxnCommitted(tableToken, txn, timestamp);
                    }
                }
            } else {
                return NO_TXN;
            }
        } catch (Throwable th) {
            distressed = true;
            LOG.critical().$("could not apply structure change to WAL table sequencer [table=").$(tableToken)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            throw th;
        }
        return txn;
    }

    @Override
    public long nextTxn(
            long expectedStructureVersion,
            int walId,
            int segmentId,
            int segmentTxn,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    ) {
        // Writing to TableSequencer can happen from multiple threads, so we need to protect against concurrent writes.
        assert !closed;
        checkDropped();
        long txn;
        final long timestamp = microClock.getTicks();
        try {
            // From sequencer perspective metadata version is the same as column structure version
            if (metadata.getMetadataVersion() == expectedStructureVersion) {
                txn = nextTxn(walId, segmentId, segmentTxn, timestamp, txnMinTimestamp, txnMaxTimestamp, txnRowCount);
            } else {
                return NO_TXN;
            }
        } catch (Throwable th) {
            distressed = true;
            LOG.critical().$("could not apply transaction to WAL table sequencer [table=").$(tableToken)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            throw th;
        }

        notifyTxnCommitted(txn);
        engine.getWalListener().dataTxnCommitted(tableToken, txn, timestamp, walId, segmentId, segmentTxn);
        return txn;
    }

    public void notifyRename(TableToken tableToken) {
        this.tableToken = tableToken;
        this.metadata.notifyRenameTable(tableToken);
    }

    @Override
    public TableToken reload() {
        tableTransactionLog.reload(path);
        if (tableTransactionLog.isDropped()) {
            return null;
        }

        try (TableMetadataChangeLog metaChangeCursor = tableTransactionLog.getTableMetadataChangeLog(
                metadata.getMetadataVersion(), alterCommandWalFormatter)
        ) {
            boolean updated = false;
            while (metaChangeCursor.hasNext()) {
                TableMetadataChange change = metaChangeCursor.next();
                change.apply(metadataSvc, true);
                updated = true;
            }
            if (updated) {
                metadata.syncToMetaFile();
            }
        }
        long lastTxn = tableTransactionLog.lastTxn();
        LOG.info()
                .$("reloaded table sequencer [table=").$(tableToken)
                .$(", lastTxn=").$(lastTxn)
                .I$();
        seqTxnTracker.notifyOnCommit(lastTxn);
        return tableToken = metadata.getTableToken();
    }

    @Override
    public void resumeTable() {
        metadata.resumeTable();
        notifyTxnCommitted(Long.MAX_VALUE);
        seqTxnTracker.setUnsuspended();
    }

    @TestOnly
    public void setDistressed() {
        this.distressed = true;
    }

    @Override
    public void suspendTable() {
        metadata.suspendTable();
    }

    private void applyToMetadata(TableMetadataChange change) {
        change.apply(metadataSvc, true);
        metadata.syncToMetaFile();
    }

    private void checkDropped() {
        if (metadata.isDropped()) {
            throw CairoException.tableDropped(tableToken);
        }
    }

    private boolean closeLocked() {
        if (closed) {
            return false;
        }
        closed = true;
        Misc.free(metadata);
        Misc.free(tableTransactionLog);
        Misc.free(walIdGenerator);
        Misc.free(path);
        return true;
    }

    private void createSequencerDir(FilesFacade ff, int mkDirMode) {
        if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
            final CairoException e = CairoException.critical(ff.errno()).put("Cannot create sequencer directory: ").put(path);
            closeLocked();
            throw e;
        }
        walDirectoryPolicy.initDirectory(path);
        path.trimTo(rootLen);
    }

    private long nextTxn(
            int walId,
            int segmentId,
            int segmentTxn,
            long timestamp,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    ) {
        return tableTransactionLog.addEntry(
                getStructureVersion(),
                walId,
                segmentId,
                segmentTxn,
                timestamp,
                txnMinTimestamp,
                txnMaxTimestamp,
                txnRowCount
        );
    }

    private void notifyTxnCommitted(long txn) {
        if (txn == Long.MAX_VALUE || seqTxnTracker.notifyOnCommit(txn)) {
            engine.notifyWalTxnCommitted(tableToken);
        }
    }

    void readLock() {
        schemaLock.readLock().lock();
    }

    void unlockRead() {
        schemaLock.readLock().unlock();
    }

    void unlockWrite() {
        schemaLock.writeLock().unlock();
    }

    void writeLock() {
        schemaLock.writeLock().lock();
    }
}
