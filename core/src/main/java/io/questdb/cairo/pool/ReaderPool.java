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

package io.questdb.cairo.pool;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.PartitionOverwriteControl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public class ReaderPool extends AbstractMultiTenantPool<ReaderPool.R> {
    private final MessageBus messageBus;
    private final PartitionOverwriteControl partitionOverwriteControl;
    private ReaderListener readerListener;

    public ReaderPool(CairoConfiguration configuration, MessageBus messageBus, PartitionOverwriteControl partitionOverwriteControl) {
        super(configuration, configuration.getReaderPoolMaxSegments(), configuration.getInactiveReaderTTL());
        this.messageBus = messageBus;
        this.partitionOverwriteControl = partitionOverwriteControl;
    }

    public ReaderPool(CairoConfiguration configuration, MessageBus messageBus) {
        this(configuration, messageBus, null);
    }

    public void attach(TableReader reader) {
        ReaderPool.R rdr = (ReaderPool.R) reader;
        rdr.attach();
    }

    public void detach(TableReader reader) {
        ReaderPool.R rdr = (ReaderPool.R) reader;
        rdr.detach();
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
    protected R newCopyOfTenant(R srcReader, Entry<R> entry, int index, ResourcePoolSupervisor<R> supervisor) {
        return new R(this, entry, index, srcReader, messageBus, readerListener, partitionOverwriteControl, supervisor);
    }

    @Override
    protected R newTenant(TableToken tableToken, Entry<R> entry, int index, ResourcePoolSupervisor<R> supervisor) {
        return new R(this, entry, index, tableToken, messageBus, readerListener, partitionOverwriteControl, supervisor);
    }

    @TestOnly
    @FunctionalInterface
    public interface ReaderListener {
        void onOpenPartition(TableToken tableToken, int partitionIndex);
    }

    public static class R extends TableReader implements PoolTenant<R> {
        private final int index;
        private final ReaderListener readerListener;
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
                Entry<R> entry,
                int index,
                TableToken tableToken,
                MessageBus messageBus,
                ReaderListener readerListener,
                PartitionOverwriteControl partitionOverwriteControl,
                ResourcePoolSupervisor<R> supervisor
        ) {
            super(pool.getConfiguration(), tableToken, messageBus, partitionOverwriteControl);
            this.pool = pool;
            this.entry = entry;
            this.index = index;
            this.readerListener = readerListener;
            this.supervisor = supervisor;
        }

        public R(
                AbstractMultiTenantPool<R> pool,
                Entry<R> entry,
                int index,
                R srcReader,
                MessageBus messageBus,
                ReaderListener readerListener,
                PartitionOverwriteControl partitionOverwriteControl,
                ResourcePoolSupervisor<R> supervisor
        ) {
            super(pool.getConfiguration(), srcReader, messageBus, partitionOverwriteControl);
            this.pool = pool;
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
    }
}
