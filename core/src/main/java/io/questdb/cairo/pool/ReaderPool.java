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

    @TestOnly
    public void setTableReaderListener(ReaderListener readerListener) {
        this.readerListener = readerListener;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_READER;
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

        public void attach() {
            detached = false;
        }

        @Override
        public void close() {
            if (!detached && isOpen()) {
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
            }
        }

        public void detach() {
            detached = true;
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
    }
}
