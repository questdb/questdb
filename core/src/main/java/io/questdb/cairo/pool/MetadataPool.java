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

package io.questdb.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DynamicTableReaderMetadata;
import io.questdb.cairo.GenericTableRecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.std.Chars;

public class MetadataPool extends AbstractMultiTenantPool<MetadataPool.MetadataTenant> {
    private final TableSequencerAPI tableSequencerAPI;
    private final boolean compress;

    public MetadataPool(
            CairoConfiguration configuration,
            TableSequencerAPI tableSequencerAPI,
            boolean compress
    ) {
        super(configuration);
        this.tableSequencerAPI = tableSequencerAPI;
        this.compress = compress;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_METADATA;
    }

    @Override
    protected MetadataTenant newTenant(String tableName, Entry<MetadataTenant> entry, int index) {
        if (tableSequencerAPI.hasSequencer(tableName)) {
            return new SequencerMetadataTenant(this, entry, index, tableName, tableSequencerAPI, compress);
        }
        return new TableReaderMetadataTenant(this, entry, index, tableName);
    }

    public interface MetadataTenant extends TableRecordMetadata, PoolTenant {
    }

    private static class TableReaderMetadataTenant extends DynamicTableReaderMetadata implements MetadataTenant {
        private final int index;
        private AbstractMultiTenantPool<MetadataTenant> pool;
        private AbstractMultiTenantPool.Entry<MetadataTenant> entry;

        public TableReaderMetadataTenant(
                AbstractMultiTenantPool<MetadataTenant> pool,
                AbstractMultiTenantPool.Entry<MetadataTenant> entry,
                int index,
                CharSequence name
        ) {
            super(pool.getConfiguration(), Chars.toString(name));
            load();
            this.pool = pool;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            if (pool != null && getEntry() != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            super.close();
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void refresh() {
            reload();
        }

        @SuppressWarnings("unchecked")
        @Override
        public AbstractMultiTenantPool.Entry<MetadataTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }
    }

    private static class SequencerMetadataTenant extends GenericTableRecordMetadata implements MetadataTenant {
        private final int index;
        private final TableSequencerAPI tableSequencerAPI;
        private AbstractMultiTenantPool<MetadataTenant> pool;
        private AbstractMultiTenantPool.Entry<MetadataTenant> entry;
        private final boolean compress;

        public SequencerMetadataTenant(
                AbstractMultiTenantPool<MetadataTenant> pool,
                Entry<MetadataTenant> entry,
                int index,
                CharSequence tableName,
                TableSequencerAPI tableSequencerAPI,
                boolean compress
        ) {
            this.pool = pool;
            this.entry = entry;
            this.index = index;
            this.tableSequencerAPI = tableSequencerAPI;
            this.compress = compress;
            tableSequencerAPI.getTableMetadata(tableName, this, compress);
        }

        @Override
        public void close() {
            if (pool != null && getEntry() != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            super.close();
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void refresh() {
            tableSequencerAPI.reloadMetadataConditionally(getTableName(), getStructureVersion(), this, compress);
        }

        @SuppressWarnings("unchecked")
        @Override
        public AbstractMultiTenantPool.Entry<MetadataTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }
    }
}