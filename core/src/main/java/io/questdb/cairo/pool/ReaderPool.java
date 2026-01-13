/*******************************************************************************
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

package io.questdb.cairo.pool;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionOverwriteControl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxnScoreboardPool;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public class ReaderPool extends AbstractMultiTenantPool<ReaderPool.R> {

    private final MessageBus messageBus;
    private final PartitionOverwriteControl partitionOverwriteControl;
    private final TxnScoreboardPool txnScoreboardPool;
    private ReaderListener readerListener;

    public ReaderPool(CairoConfiguration configuration, TxnScoreboardPool scoreboardPool, MessageBus messageBus, PartitionOverwriteControl partitionOverwriteControl) {
        super(configuration, configuration.getReaderPoolMaxSegments(), configuration.getInactiveReaderTTL());
        this.txnScoreboardPool = scoreboardPool;
        this.messageBus = messageBus;
        this.partitionOverwriteControl = partitionOverwriteControl;
    }

    public ReaderPool(CairoConfiguration configuration, TxnScoreboardPool scoreboardPool, MessageBus messageBus) {
        this(configuration, scoreboardPool, messageBus, null);
    }

    public void attach(TableReader reader) {
        ReaderPool.R rdr = (ReaderPool.R) reader;
        rdr.attach();
    }

    public void detach(TableReader reader) {
        ReaderPool.R rdr = (ReaderPool.R) reader;
        rdr.detach();
    }

    @Override
    public ReaderPool.R get(TableToken tableToken) {
        if (tableToken.isView()) {
            throw CairoException.critical(0).put("cannot get a reader for view [view=").put(tableToken).put(']');
        }
        return super.get(tableToken);
    }

    /**
     * Returns a pooled table reader that is pointed at the same transaction number
     * as the source reader.
     */
    public TableReader getCopyOf(TableReader srcReader) {
        return getCopyOf((ReaderPool.R) srcReader);
    }

    public int getDetachedRefCount(TableReader reader) {
        return ((ReaderPool.R) reader).getDetachedRefCount();
    }

    public void incDetachedRefCount(TableReader reader) {
        ((ReaderPool.R) reader).incrementDetachedRefCount();
    }

    @Override
    public boolean isCopyOfSupported() {
        return true;
    }

    public boolean isDetached(TableReader reader) {
        return ((ReaderPool.R) reader).isDetached();
    }

    @TestOnly
    public void setTableReaderListener(ReaderListener readerListener) {
        this.readerListener = readerListener;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_READER;
    }

    @Override
    protected R newCopyOfTenant(R srcReader, Entry<R> rootEntry, Entry<R> entry, int index, ResourcePoolSupervisor<R> supervisor) {
        return new R(this, rootEntry, entry, index, srcReader, txnScoreboardPool, messageBus, readerListener, partitionOverwriteControl, supervisor);
    }

    @Override
    protected R newTenant(TableToken tableToken, Entry<R> rootEntry, Entry<R> entry, int index, ResourcePoolSupervisor<R> supervisor) {
        return new R(this, rootEntry, entry, index, tableToken, txnScoreboardPool, messageBus, readerListener, partitionOverwriteControl, supervisor);
    }

    @TestOnly
    @FunctionalInterface
    public interface ReaderListener {
        void onOpenPartition(TableToken tableToken, int partitionIndex);
    }

    public static class R extends TableReader implements PoolTenant<R> {
        private final int index;
        private final ReaderListener readerListener;
        private final Entry<R> rootEntry;
        private boolean detached;
        // Reference counter that may be used to track usage of detached readers.
        // A reader may be obtained from the pool and closed on different threads,
        // but that's fine. In that case, there will be synchronization between the threads,
        // so we don't need to make this field volatile/atomic.
        private int detachedRefCount;
        private Entry<R> entry;
        private AbstractMultiTenantPool<R> pool;
        private ResourcePoolSupervisor<R> supervisor;

        public R(
                AbstractMultiTenantPool<R> pool,
                Entry<R> rootEntry,
                Entry<R> entry,
                int index,
                TableToken tableToken,
                TxnScoreboardPool txnScoreboardPool,
                MessageBus messageBus,
                ReaderListener readerListener,
                PartitionOverwriteControl partitionOverwriteControl,
                ResourcePoolSupervisor<R> supervisor
        ) {
            super(entry.getIndex() * pool.getSegmentSize() + index, pool.getConfiguration(), tableToken, txnScoreboardPool, messageBus, partitionOverwriteControl);
            this.pool = pool;
            this.rootEntry = rootEntry;
            this.entry = entry;
            this.index = index;
            this.readerListener = readerListener;
            this.supervisor = supervisor;
        }

        public R(
                AbstractMultiTenantPool<R> pool,
                Entry<R> rootEntry,
                Entry<R> entry,
                int index,
                R srcReader,
                TxnScoreboardPool txnScoreboardPool,
                MessageBus messageBus,
                ReaderListener readerListener,
                PartitionOverwriteControl partitionOverwriteControl,
                ResourcePoolSupervisor<R> supervisor
        ) {
            super(entry.getIndex() * pool.getSegmentSize() + index, pool.getConfiguration(), srcReader, txnScoreboardPool, messageBus, partitionOverwriteControl);
            this.pool = pool;
            this.rootEntry = rootEntry;
            this.entry = entry;
            this.index = index;
            this.readerListener = readerListener;
            this.supervisor = supervisor;
        }

        public void attach() {
            assert detachedRefCount == 0;
            detached = false;
        }

        @Override
        public void close() {
            if (isOpen()) {
                if (!detached) {
                    // report reader closure to the supervisor
                    // so that we do not freak out about reader leaks
                    if (supervisor != null) {
                        supervisor.onResourceReturned(this);
                        supervisor = null;
                    }
                    goPassive();
                    final AbstractMultiTenantPool<R> pool = this.pool;
                    if (pool == null || entry == null || !pool.returnToPool(this)) {
                        super.close();
                    }
                } else {
                    closeExcessPartitions();
                    detachedRefCount--;
                }
            }
        }

        public void detach() {
            detached = true;
            detachedRefCount = 0;
        }

        public int getDetachedRefCount() {
            return detachedRefCount;
        }

        @Override
        public Entry<R> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public Entry<R> getRootEntry() {
            return rootEntry;
        }

        public ResourcePoolSupervisor<R> getSupervisor() {
            return supervisor;
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        public void incrementDetachedRefCount() {
            assert detached;
            detachedRefCount++;
        }

        public boolean isDetached() {
            return detached;
        }

        @Override
        public long openPartition(int partitionIndex) {
            if (readerListener != null) {
                readerListener.onOpenPartition(getTableToken(), partitionIndex);
            }
            return super.openPartition(partitionIndex);
        }

        @Override
        public void refresh(ResourcePoolSupervisor<R> supervisor) {
            this.supervisor = supervisor;
            try {
                goActive();
            } catch (Throwable ex) {
                close();
                throw ex;
            }
        }

        @Override
        public void refreshAt(@Nullable ResourcePoolSupervisor<R> supervisor, R srcReader) {
            this.supervisor = supervisor;
            try {
                goActiveAtTxn(srcReader);
            } catch (Throwable ex) {
                close();
                throw ex;
            }
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("ReaderPool.R{index=").put(index).put(", detached=").put(detached).put(", detachedRefCount=").put(detachedRefCount).put('}');
        }
    }
}
