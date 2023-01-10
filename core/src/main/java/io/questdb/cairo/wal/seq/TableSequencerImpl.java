/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import static io.questdb.cairo.wal.WalUtils.WAL_INDEX_FILE_NAME;

public class TableSequencerImpl implements TableSequencer {
    private static final Log LOG = LogFactory.getLog(TableSequencerImpl.class);
    private final static BinaryAlterSerializer alterCommandWalFormatter = new BinaryAlterSerializer();
    private final EmptyOperationCursor emptyOperationCursor = new EmptyOperationCursor();
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final SequencerMetadata metadata;
    private final MicrosecondClock microClock;
    private final int mkDirMode;
    private final Path path;
    private final int rootLen;
    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();
    private final SequencerMetadataUpdater sequencerMetadataUpdater;
    private final TableTransactionLog tableTransactionLog;
    private final IDGenerator walIdGenerator;
    private volatile boolean closed = false;
    private boolean distressed;
    private TableToken tableToken;

    TableSequencerImpl(CairoEngine engine, TableToken tableToken) {
        this.engine = engine;
        this.tableToken = tableToken;

        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try {
            path = new Path();
            path.of(configuration.getRoot()).concat(tableToken.getDirName()).concat(WalUtils.SEQ_DIR);
            rootLen = path.length();
            this.ff = ff;
            this.mkDirMode = configuration.getMkDirMode();

            metadata = new SequencerMetadata(ff);
            sequencerMetadataUpdater = new SequencerMetadataUpdater(metadata, tableToken);
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
        tableTransactionLog.addEntry(getStructureVersion(), WalUtils.DROP_TABLE_WALID, 0, 0, microClock.getTicks());
        metadata.dropTable();
        engine.notifyWalTxnCommitted(tableToken, Long.MAX_VALUE);
    }

    @Override
    public TableMetadataChangeLog getMetadataChangeLogCursor(long structureVersionLo) {
        checkDropped();
        if (metadata.getStructureVersion() == structureVersionLo) {
            // Nothing to do.
            return emptyOperationCursor.of(tableToken);
        }
        return tableTransactionLog.getTableMetadataChangeLog(tableToken, structureVersionLo, alterCommandWalFormatter);
    }

    @Override
    public int getNextWalId() {
        return (int) walIdGenerator.getNextId();
    }

    @Override
    public long getStructureVersion() {
        return metadata.getStructureVersion();
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
                    i
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
                metadata.getStructureVersion(),
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
            if (metadata.getStructureVersion() == expectedStructureVersion) {
                tableTransactionLog.beginMetadataChangeEntry(expectedStructureVersion + 1, alterCommandWalFormatter, change, microClock.getTicks());

                // Re-read serialised change to ensure it can be read.
                AlterOperation deserializedAlter = tableTransactionLog.readTableMetadataChangeLog(expectedStructureVersion, alterCommandWalFormatter);

                applyToMetadata(deserializedAlter);
                if (metadata.getStructureVersion() != expectedStructureVersion + 1) {
                    throw CairoException.critical(0)
                            .put("applying structure change to WAL table failed [table=").put(tableToken.getDirName())
                            .put(", oldVersion: ").put(expectedStructureVersion)
                            .put(", newVersion: ").put(metadata.getStructureVersion())
                            .put(']');
                }
                metadata.syncToDisk();
                txn = tableTransactionLog.endMetadataChangeEntry();
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

        if (!metadata.isSuspended()) {
            engine.notifyWalTxnCommitted(tableToken, txn);
        }
        return txn;
    }

    @Override
    public long nextTxn(long expectedStructureVersion, int walId, int segmentId, int segmentTxn) {
        // Writing to TableSequencer can happen from multiple threads, so we need to protect against concurrent writes.
        assert !closed;
        checkDropped();
        long txn;
        try {
            if (metadata.getStructureVersion() == expectedStructureVersion) {
                txn = nextTxn(walId, segmentId, segmentTxn);
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
            engine.notifyWalTxnCommitted(tableToken, txn);
        }
        return txn;
    }

    public void open() {
        try {
            walIdGenerator.open(path);
            metadata.open(path, rootLen);
            tableTransactionLog.open(path);
        } catch (Throwable th) {
            LOG.critical().$("could not open sequencer [name=").utf8(tableToken.getDirName())
                    .$(", path=").$(path)
                    .$(", error=").$(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
    }

    @Override
    public void rename(TableToken newTableToken) {
        checkDropped();
        tableToken = newTableToken;
        this.metadata.updateTableToken(newTableToken);
    }

    @Override
    public void resumeTable() {
        metadata.resumeTable();
        engine.notifyWalTxnCommitted(tableToken, Long.MAX_VALUE);
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
        change.apply(sequencerMetadataUpdater, true);
        metadata.syncToMetaFile();
    }

    private void checkDropped() {
        if (metadata.isDropped()) {
            throw CairoException.nonCritical().put("table is dropped [dirName=").put(tableToken.getDirName()).put(']');
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
        path.trimTo(rootLen);
    }

    private long nextTxn(int walId, int segmentId, int segmentTxn) {
        return tableTransactionLog.addEntry(getStructureVersion(), walId, segmentId, segmentTxn, microClock.getTicks());
    }

    void create(int tableId, TableStructure model) {
        schemaLock.writeLock().lock();
        try {
            createSequencerDir(ff, mkDirMode);
            metadata.create(model, tableToken, path, rootLen, tableId);
        } finally {
            schemaLock.writeLock().unlock();
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
