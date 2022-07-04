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

import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cairo.TableUtils.*;

public class SequencerImpl implements Sequencer {
    private static final String SEQ_DIR = "seq";

    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();

    private final CairoEngine engine;
    private final String tableName;
    private final Path path;
    private final int rootLen;
    private final SequencerMetadata metadata;
    private final IDGenerator txnGenerator;
    private final IDGenerator walIdGenerator;

    SequencerImpl(CairoEngine engine, CharSequence tableName) {
        this.engine = engine;
        this.tableName = Chars.toString(tableName);

        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        path = new Path().of(configuration.getRoot()).concat(tableName).concat(SEQ_DIR);
        rootLen = path.length();

        createSequencerDir(ff, configuration.getMkDirMode());
        metadata = new SequencerMetadata(ff);

        walIdGenerator = new IDGenerator(configuration, WAL_INDEX_FILE_NAME);
        walIdGenerator.open(path);
        txnGenerator = new IDGenerator(configuration, TXN_FILE_NAME);
        txnGenerator.open(path);
    }

    private void createSequencerDir(FilesFacade ff, int mkDirMode) {
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("Cannot create sequencer directory: ").put(path);
        }
        path.trimTo(rootLen);
    }

    void of(TableStructure model) {
        schemaLock.writeLock().lock();
        try {
            metadata.init(model, path, rootLen);
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    @Override
    public long nextTxn() {
        return txnGenerator.getNextId();
    }

    @Override
    public long nextTxn(int expectedSchemaVersion) {
        schemaLock.readLock().lock();
        try {
            return metadata.getSchemaVersion() == expectedSchemaVersion ? txnGenerator.getNextId() : NO_TXN;
        } finally {
            schemaLock.readLock().unlock();
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
    public long addColumn(int columnIndex, CharSequence columnName, int columnType) {
        schemaLock.writeLock().lock();
        try {
            metadata.addColumn(columnIndex, columnName, columnType, path, rootLen);
            return nextTxn();
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    @Override
    public long removeColumn(int columnIndex) {
        schemaLock.writeLock().lock();
        try {
            metadata.removeColumn(columnIndex, path, rootLen);
            return nextTxn();
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    private final AtomicInteger activeWals = new AtomicInteger(0);

    @Override
    public WalWriter createWal() {
        final WalWriter w = new WalWriter(engine, tableName, (int) walIdGenerator.getNextId(), this);
        activeWals.incrementAndGet();
        return w;
    }

    @Override
    public void unregisterWal(int walId) {
        if (activeWals.decrementAndGet() == 0) {
            close();
        }
    }

    @Override
    public void close() {
        Misc.free(metadata);
        Misc.free(path);
        Misc.free(walIdGenerator);
        Misc.free(txnGenerator);
    }
}
