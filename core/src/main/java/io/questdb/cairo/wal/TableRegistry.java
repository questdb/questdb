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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.AbstractPool;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.wal.Sequencer.SEQ_DIR;

public class TableRegistry extends AbstractPool {
    private static final Log LOG = LogFactory.getLog(TableRegistry.class);
    private final ConcurrentHashMap<Rc> tableRegistry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SequencerImpl> seqRegistry = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<WalWriterPool> walRegistry = new ConcurrentHashMap<>();
    private final CairoEngine engine;

    public TableRegistry(CairoEngine engine, CairoConfiguration configuration) {
        super(configuration, configuration.getInactiveWriterTTL()); //todo: separate config option
        this.engine = engine;
        notifyListener(Thread.currentThread().getId(), "TableRegistry", PoolListener.EV_POOL_OPEN);
    }

    public boolean copyMetadataTo(final CharSequence tableName, final SequencerMetadata metadata) {
        Sequencer sequencer = openSequencer(tableName);
        if (sequencer != null) {
            sequencer.copyMetadataTo(metadata);
            return true;
        }
        return false;
    }

    public @Nullable SequencerCursor getCursor(final CharSequence tableName, long lastCommittedTxn) {
        Sequencer sequencer = openSequencer(tableName);
        if (sequencer != null) {
            return sequencer.getCursor(lastCommittedTxn);
        }
        return null;
    }
//
//    public Sequencer getNewSequencer(int tableId, final TableStructure tableModel) {
//        throwIfClosed();
//        final Rc rc = tableRegistry.computeIfAbsent(tableModel.getTableName(), (final CharSequence tableName) -> {
//            final Rc newRc = new TableRegistry.Rc();
//            newRc.sequencer = new SequencerImpl(this.engine, tableName.toString(), newRc);
//            newRc.sequencer.create(tableId, tableModel);
//            return newRc;
//        });
//        rc.refCount.incrementAndGet();
//        return rc.sequencer;
//    }
//
//    public Sequencer getSequencer(final CharSequence tableName) {
//        throwIfClosed();
//        final Rc rc = tableRegistry.computeIfPresent(tableName, (final CharSequence name, final Rc current) -> {
//            current.sequencer.open();
//            return current;
//        });
//
//        if (rc != null) {
//            rc.refCount.incrementAndGet();
//            return rc.sequencer;
//        }
//        return null;
//    }

    public @Nullable SequencerStructureChangeCursor getStructureChangeCursor(final CharSequence tableName, @Nullable SequencerStructureChangeCursor reusableCursor, long fromSchemaVersion) {
        Sequencer sequencer = openSequencer(tableName);
        if (sequencer != null) {
            return sequencer.getStructureChangeCursor(reusableCursor, fromSchemaVersion);
        }
        return null;
    }

    public boolean hasSequencer(final CharSequence tableName) {
        Sequencer sequencer = seqRegistry.get(tableName);
        if (sequencer != null) {
            return true;
        }
        return isWalTable(tableName, getConfiguration().getRoot(), getConfiguration().getFilesFacade());
    }

    public long nextStructureTxn(final CharSequence tableName, long structureVersion, AlterOperation operation) {
        Sequencer sequencer = openSequencer(tableName);
        if (sequencer != null) {
            return sequencer.nextStructureTxn(structureVersion, operation);
        }
        return Sequencer.NO_TXN; //todo: throw exception ?
    }

    //--------------------

    public long nextTxn(final CharSequence tableName, int walId, long expectedSchemaVersion, long segmentId, long segmentTxn) {
        Sequencer sequencer = openSequencer(tableName);
        if (sequencer != null) {
            return sequencer.nextTxn(expectedSchemaVersion, walId, segmentId, segmentTxn);
        }
        return Sequencer.NO_TXN; //todo: throw exception ?
    }

    public int registerTable(int tableId, final TableStructure tableStructure) {
        return createSequencer(tableId, tableStructure).getTableId();
    }
    public int getNextWalId(final CharSequence tableName) {
        Sequencer sequencer = openSequencer(tableName);
        if (sequencer != null) {
            return sequencer.getNextWalId(); //todo: fix this
        }
        return -1;
    }

    public @NotNull WalWriter getWalWriter(final CharSequence tableName) {
        return getWalPool(tableName).pop();
    }
    // Check if sequencer files exist, e.g. is it WAL table sequencer must exist
    private static boolean isWalTable(final CharSequence tableName, final CharSequence root, final FilesFacade ff) {
        Path path = Path.getThreadLocal2(root);
        path.concat(tableName).concat(SEQ_DIR);
        return ff.exists(path.$());
    }

    public void clear() {
        // create proper clear() and close() methods
        final Set<Map.Entry<CharSequence, Rc>> entries = tableRegistry.entrySet();
        for (Map.Entry<CharSequence, Rc> entry : entries) {
            entry.getValue().close();
        }
        tableRegistry.clear();
    }

    private @NotNull Sequencer createSequencer(int tableId, final TableStructure tableStructure) {
        final String tableName = Chars.toString(tableStructure.getTableName());
        return seqRegistry.compute(tableName, (key, val) -> {
            SequencerImpl sequencer = new SequencerImpl(this.engine, tableName, new Rc());
            sequencer.create(tableId, tableStructure);
            sequencer.open();
            if (val != null) {
                val.close();
            }
            return sequencer;
        });
    }

    private @Nullable SequencerImpl openSequencer(final CharSequence tableName) {
        final String tableNameStr = Chars.toString(tableName);
        return seqRegistry.computeIfAbsent(tableNameStr, (key) -> {
            if (isWalTable(tableName, getConfiguration().getRoot(), getConfiguration().getFilesFacade())) {
//                SequencerImpl sequencer = val == null ? new SequencerImpl(this.engine, tableNameStr, new Rc()): val;
                SequencerImpl sequencer = new SequencerImpl(this.engine, tableName.toString(), new Rc());
                sequencer.open();
                return sequencer;
            }
            return null;
        });
    }

    private @NotNull WalWriterPool getWalPool(final CharSequence tableName) {
        final String tableNameStr = Chars.toString(tableName);
        //todo: configure maxSize
        return walRegistry.computeIfAbsent(tableNameStr, (key) -> new WalWriterPool(10, tableNameStr, this, engine.getConfiguration()));
    }

    @Override
    protected boolean releaseAll(long deadline) {
        boolean removed = false;
        Iterator<Rc> iterator = tableRegistry.values().iterator();
        while (iterator.hasNext()) {
            final Rc rc = iterator.next();
            if ((deadline > rc.lastReleaseTime && rc.refCount.get() == 0)) {
                rc.sequencer.setLifecycleManager(DefaultLifecycleManager.INSTANCE);
                rc.sequencer.close();
                iterator.remove();
                removed = true;
            } else if (deadline == Long.MAX_VALUE) {
                iterator.remove();
                removed = true;
            }
        }
        releaseWals(deadline);
        releaseEntries(deadline);
        return removed;
    }

    private void releaseEntries(long deadline) {
        Iterator<SequencerImpl> iterator = seqRegistry.values().iterator();
        while (iterator.hasNext()) {
            final SequencerImpl sequencer = iterator.next();
            if (deadline > sequencer.getLastOpenTime()) {
                sequencer.setLifecycleManager(DefaultLifecycleManager.INSTANCE);//todo: remove it
                sequencer.close();
                iterator.remove();
            }
        }
    }

    private void releaseWals(long deadline) {
        Iterator<WalWriterPool> iterator = walRegistry.values().iterator();
        while (iterator.hasNext()) {
            final WalWriterPool pool = iterator.next();
            pool.close();
            if (deadline == Long.MAX_VALUE) {
                iterator.remove();
            }
        }
    }
    private boolean returnToPool(final Rc rc) {
        if (isClosed()) {
            return rc.refCount.get() != 0;
        }
        rc.lastReleaseTime = getConfiguration().getMicrosecondClock().getTicks();
        rc.refCount.decrementAndGet();
        return true;
    }

    private void throwIfClosed() {
        if (isClosed()) {
            LOG.info().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    private class Rc implements LifecycleManager {
        private final AtomicLong refCount = new AtomicLong();
        private volatile long lastReleaseTime;
        private SequencerImpl sequencer;

        @Override
        public boolean close() {
            return !TableRegistry.this.returnToPool(this);
        }
    }
}
