/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.WAL_INDEX_FILE_NAME;

public class TableSequencerImpl implements TableSequencer {
    private static final Log LOG = LogFactory.getLog(TableSequencerImpl.class);
    private final static BinaryAlterSerializer alterCommandWalFormatter = new BinaryAlterSerializer();
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final SequencerMetadata metadata;
    private final SequencerMetadataService metadataSvc;
    private final MicrosecondClock microClock;
    private final int mkDirMode;
    private final Path path;
    private final int rootLen;
    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();
    private final SeqTxnTracker seqTxnTracker;
    private final TableTransactionLog tableTransactionLog;
    private final WalDirectoryPolicy walDirectoryPolicy;
    private final IDGenerator walIdGenerator;
    private volatile boolean closed = false;
    private boolean distressed;
    private TableToken tableToken;

    TableSequencerImpl(CairoEngine engine, TableToken tableToken, SeqTxnTracker txnTracker) {
        this.engine = engine;
        this.tableToken = tableToken;
        this.seqTxnTracker = txnTracker;

        final CairoConfiguration configuration = engine.getConfiguration();
        this.walDirectoryPolicy = engine.getWalDirectoryPolicy();
        final FilesFacade ff = configuration.getFilesFacade();
        try {
            path = new Path();
            path.of(configuration.getRoot());
            path.concat(tableToken.getDirName()).concat(SEQ_DIR);
            rootLen = path.size();
            this.ff = ff;
            this.mkDirMode = configuration.getMkDirMode();

            metadata = new SequencerMetadata(ff);
            metadataSvc = new SequencerMetadataService(metadata, tableToken);
            walIdGenerator = new IDGenerator(configuration, WAL_INDEX_FILE_NAME);
            tableTransactionLog = new TableTransactionLog(ff);
            microClock = engine.getConfiguration().getMicrosecondClock();
        } catch (Throwable th) {
            LOG.critical().$("could not create sequencer [name=").utf8(tableToken.getDirName())
                    .$(", error=").$(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
    }

    public boolean checkClose() {
        if (!closed) {
            schemaLock.writeLock().lock();
            try {
                return closeLocked();
            } finally {
                schemaLock.writeLock().unlock();
            }
        }
        return false;
    }

    @Override
    public void close() {
        checkClose();
    }

    @Override
    public void dropTable() {
        checkDropped();
        final long timestamp = microClock.getTicks();
        final long txn = tableTransactionLog.addEntry(getStructureVersion(), WalUtils.DROP_TABLE_WALID, 0, 0, timestamp);
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
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            sink.addColumn(
                    metadata.getColumnName(i),
                    columnType,
                    metadata.isColumnIndexed(i),
                    metadata.getIndexValueBlockCapacity(i),
                    metadata.isSymbolTableStatic(i),
                    i,
                    metadata.isDedupKey(i)
            );
            if (columnType > -1) {
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
                compressedColumnCount
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
    public boolean isSuspended() {
        return metadata.isSuspended();
    }

    @Override
    public long lastTxn() {
        return tableTransactionLog.lastTxn();
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
                tableTransactionLog.beginMetadataChangeEntry(expectedStructureVersion + 1, alterCommandWalFormatter, change, timestamp);

                final TableToken oldTableToken = tableToken;

                // Re-read serialised change to ensure it can be read.
                AlterOperation deserializedAlter = tableTransactionLog.readTableMetadataChangeLog(expectedStructureVersion, alterCommandWalFormatter);

                applyToMetadata(deserializedAlter);
                if (metadata.getMetadataVersion() != expectedStructureVersion + 1) {
                    throw CairoException.critical(0)
                            .put("applying structure change to WAL table failed [table=").put(tableToken.getDirName())
                            .put(", oldVersion: ").put(expectedStructureVersion)
                            .put(", newVersion: ").put(metadata.getMetadataVersion())
                            .put(']');
                }
                metadata.syncToDisk();
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
            LOG.critical().$("could not apply structure change to WAL table sequencer [table=").utf8(tableToken.getDirName())
                    .$(", error=").$(th.getMessage())
                    .I$();
            throw th;
        }
        return txn;
    }

    @Override
    public long nextTxn(long expectedStructureVersion, int walId, int segmentId, int segmentTxn) {
        // Writing to TableSequencer can happen from multiple threads, so we need to protect against concurrent writes.
        assert !closed;
        checkDropped();
        long txn;
        final long timestamp = microClock.getTicks();
        try {
            // From sequencer perspective metadata version is the same as column structure version
            if (metadata.getMetadataVersion() == expectedStructureVersion) {
                txn = nextTxn(walId, segmentId, segmentTxn, timestamp);
            } else {
                return NO_TXN;
            }
        } catch (Throwable th) {
            distressed = true;
            LOG.critical().$("could not apply transaction to WAL table sequencer [table=").utf8(tableToken.getDirName())
                    .$(", error=").$(th.getMessage())
                    .I$();
            throw th;
        }

        if (!metadata.isSuspended()) {
            notifyTxnCommitted(txn);
            engine.getWalListener().dataTxnCommitted(tableToken, txn, timestamp, walId, segmentId, segmentTxn);
        }
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

        try (TableMetadataChangeLog metaChangeCursor = tableTransactionLog.getTableMetadataChangeLog(metadata.getMetadataVersion(), alterCommandWalFormatter)) {
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
        LOG.info().$("reloaded table sequencer [name=").utf8(tableToken.getDirName()).$(", lastTxn=").$(lastTxn).I$();
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
        if (!closed) {
            closed = true;
            Misc.free(metadata);
            Misc.free(tableTransactionLog);
            Misc.free(walIdGenerator);
            Misc.free(path);
            return true;
        }
        return false;
    }

    private void createSequencerDir(FilesFacade ff, int mkDirMode) {
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            final CairoException e = CairoException.critical(ff.errno()).put("Cannot create sequencer directory: ").put(path);
            closeLocked();
            throw e;
        }
        walDirectoryPolicy.initDirectory(path);
        path.trimTo(rootLen);
    }

    private long nextTxn(int walId, int segmentId, int segmentTxn, long timestamp) {
        return tableTransactionLog.addEntry(getStructureVersion(), walId, segmentId, segmentTxn, timestamp);
    }

    private void notifyTxnCommitted(long txn) {
        if (txn == Long.MAX_VALUE || seqTxnTracker.notifyOnCommit(txn)) {
            engine.notifyWalTxnCommitted(tableToken);
        }
    }

    void create(int tableId, TableStructure tableStruct) {
        schemaLock.writeLock().lock();
        try {
            createSequencerDir(ff, mkDirMode);
            final long timestamp = microClock.getTicks();
            metadata.create(tableStruct, tableToken, path, rootLen, tableId);
            tableTransactionLog.create(path, timestamp);
            engine.getWalListener().tableCreated(tableToken, timestamp);
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    void open(TableToken tableToken) {
        try {
            walIdGenerator.open(path);
            metadata.open(path, rootLen, tableToken);
            tableTransactionLog.open(path);
        } catch (CairoException ex) {
            closeLocked();
            if (ex.isTableDropped()) {
                throw ex;
            }
            if (ex.errnoReadPathDoesNotExist()) {
                LOG.info().$("could not open sequencer, files deleted, assuming dropped [name=").utf8(tableToken.getDirName())
                        .$(", path=").$(path)
                        .$(", error=").$(ex.getMessage())
                        .I$();
                throw CairoException.tableDropped(tableToken);
            }
            LOG.critical().$("could not open sequencer [name=").utf8(tableToken.getDirName())
                    .$(", path=").$(path)
                    .$(", error=").$(ex.getMessage())
                    .I$();
            throw ex;
        } catch (Throwable th) {
            LOG.critical().$("could not open sequencer [name=").utf8(tableToken.getDirName())
                    .$(", path=").$(path)
                    .$(", error=").$(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
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
