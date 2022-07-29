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

package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.Path;

import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cairo.TableUtils.WAL_INDEX_FILE_NAME;

public class SequencerImpl implements Sequencer {

    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();

    private final CairoEngine engine;
    private final String tableName;
    private Path path;
    private final int rootLen;
    private final SequencerMetadata metadata;
    private final TxnCatalog catalog;
    private final IDGenerator walIdGenerator;

    SequencerImpl(CairoEngine engine, String tableName) {
        this.engine = engine;
        this.tableName = tableName;

        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try {
            path = new Path().of(configuration.getRoot()).concat(tableName).concat(SEQ_DIR);
            rootLen = path.length();

            createSequencerDir(ff, configuration.getMkDirMode());
            metadata = new SequencerMetadata(ff);

            walIdGenerator = new IDGenerator(configuration, WAL_INDEX_FILE_NAME);
            walIdGenerator.open(path);
            catalog = new TxnCatalog(ff);
            catalog.open(path);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public void abortClose() {
        schemaLock.writeLock().lock();
        try {
            metadata.abortClose();
            catalog.abortClose();
            Misc.free(walIdGenerator);
            path = Misc.free(path);
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    @Override
    public void open() {
        metadata.open(path, rootLen);
    }

    @Override
    public long nextTxn(int expectedSchemaVersion, int walId, long segmentId, long segmentTxn) {
        schemaLock.writeLock().lock();
        try {
            if (metadata.getSchemaVersion() == expectedSchemaVersion) {
                return nextTxn(walId, segmentId, segmentTxn);
            }
            return NO_TXN;

        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    @Override
    public void populateDescriptor(TableDescriptor descriptor) {
        schemaLock.readLock().lock();
        try {
            descriptor.of(metadata);
        } finally {
            schemaLock.readLock().unlock();
        }
    }

    @Override
    public WalWriter createWal() {
        return new WalWriter(engine, tableName, (int) walIdGenerator.getNextId(), this);
    }

    @Override
    public SequencerCursor getCursor(long lastCommittedTxn) {
        return catalog.getCursor(lastCommittedTxn);
    }

    @Override
    public long getMaxTxn() {
        return catalog.maxTxn();
    }

    @Override
    public void close() {
        schemaLock.writeLock().lock();
        try {
            Misc.free(metadata);
            Misc.free(catalog);
            Misc.free(walIdGenerator);
            Misc.free(path);
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    private void createSequencerDir(FilesFacade ff, int mkDirMode) {
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("Cannot create sequencer directory: ").put(path);
        }
        path.trimTo(rootLen);
    }

    private long nextTxn(int walId, long segmentId, long segmentTxn) {
        long txn = catalog.addEntry(walId, segmentId, segmentTxn);
        engine.notifyWalTxnCommitted(metadata.getTableId(), tableName, txn);
        return txn;
    }

    void create(int tableId, TableStructure model) {
        schemaLock.writeLock().lock();
        try {
            metadata.create(model, path, rootLen, tableId);
        } finally {
            schemaLock.writeLock().unlock();
        }
    }
}
