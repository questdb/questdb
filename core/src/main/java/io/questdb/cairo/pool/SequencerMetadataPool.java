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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.seq.TableRecordMetadataSink;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.std.IntList;
import io.questdb.std.Transient;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

public class SequencerMetadataPool extends AbstractMultiTenantPool<SequencerMetadataPool.SequencerMetadataTenantImpl> {
    private final CairoEngine engine;

    public SequencerMetadataPool(CairoConfiguration configuration, CairoEngine engine) {
        super(configuration, configuration.getMetadataPoolCapacity(), configuration.getInactiveReaderTTL());
        this.engine = engine;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_SEQUENCER_METADATA;
    }

    @Override
    protected SequencerMetadataTenantImpl newTenant(
            TableToken tableToken,
            Entry<SequencerMetadataTenantImpl> rootEntry,
            Entry<SequencerMetadataTenantImpl> entry,
            int index,
            @Nullable ResourcePoolSupervisor<SequencerMetadataTenantImpl> supervisor
    ) {
        return new SequencerMetadataTenantImpl(this, rootEntry, entry, index, tableToken, engine.getTableSequencerAPI());
    }

    public static class SequencerMetadataTenantImpl extends GenericRecordMetadata implements TableRecordMetadata, TableRecordMetadataSink, PoolTenant<SequencerMetadataTenantImpl> {
        private final Comparator<TableColumnMetadata> columnOrderComparator;
        private final int index;
        private final AbstractMultiTenantPool.Entry<SequencerMetadataTenantImpl> rootEntry;
        private final TableSequencerAPI tableSequencerAPI;
        private AbstractMultiTenantPool.Entry<SequencerMetadataTenantImpl> entry;
        private long metadataVersion;
        private AbstractMultiTenantPool<SequencerMetadataTenantImpl> pool;
        private IntList readColumnOrder;
        private int tableId;
        private TableToken tableToken;

        public SequencerMetadataTenantImpl(
                AbstractMultiTenantPool<SequencerMetadataTenantImpl> pool,
                Entry<SequencerMetadataTenantImpl> rootEntry,
                Entry<SequencerMetadataTenantImpl> entry,
                int index,
                TableToken tableToken,
                TableSequencerAPI tableSequencerAPI
        ) {
            super();
            columnOrderComparator = this::compareColumnOrder;
            this.pool = pool;
            this.rootEntry = rootEntry;
            this.entry = entry;
            this.index = index;
            this.tableSequencerAPI = tableSequencerAPI;
            this.tableToken = tableToken;
            tableSequencerAPI.getTableMetadata(tableToken, this);
        }

        @Override
        public void addColumn(
                String columnName,
                int columnType,
                boolean columnIndexed,
                int indexValueBlockCapacity,
                boolean symbolTableStatic,
                int writerIndex,
                boolean isDedupKey,
                boolean symbolIsCached,
                int symbolCapacity
        ) {
            if (columnType > -1L) {
                add(
                        new TableColumnMetadata(
                                columnName,
                                columnType,
                                columnIndexed,
                                indexValueBlockCapacity,
                                symbolTableStatic,
                                null,
                                writerIndex,
                                isDedupKey,
                                0,
                                symbolIsCached,
                                symbolCapacity
                        )
                );
            }
        }

        @Override
        public void close() {
            if (pool != null && getEntry() != null) {
                pool.returnToPool(this);
            }
        }

        @Override
        public AbstractMultiTenantPool.Entry<SequencerMetadataTenantImpl> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public long getMetadataVersion() {
            return metadataVersion;
        }

        @Override
        public AbstractMultiTenantPool.Entry<SequencerMetadataTenantImpl> getRootEntry() {
            return rootEntry;
        }

        @Override
        public int getTableId() {
            return tableId;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isWalEnabled() {
            // this class is only used for WAL-enabled tables
            return true;
        }

        @Override
        public void of(
                TableToken tableToken,
                int tableId,
                int timestampIndex,
                int compressedTimestampIndex,
                long structureVersion,
                int columnCount,
                @Transient @Nullable IntList readColumnOrder
        ) {
            this.tableToken = tableToken;
            this.tableId = tableId;
            this.timestampIndex = compressedTimestampIndex;
            this.metadataVersion = structureVersion;

            if (readColumnOrder != null) {
                this.readColumnOrder = readColumnOrder;
                columnMetadata.sort(columnOrderComparator);
                this.readColumnOrder = null;

                columnNameIndexMap.clear();
                for (int i = 0; i < columnCount; i++) {
                    TableColumnMetadata column = columnMetadata.getQuick(i);
                    columnNameIndexMap.put(column.getColumnName(), i);
                    if (column.getWriterIndex() == timestampIndex) {
                        this.timestampIndex = i;
                    }
                }
            }
        }

        @Override
        public void refresh(@Nullable ResourcePoolSupervisor<SequencerMetadataTenantImpl> supervisor) {
            tableSequencerAPI.reloadMetadataConditionally(tableToken, getMetadataVersion(), this);
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("SequencerMetadataTenantImpl{index=").put(index).put(", tableToken=").put(tableToken).put('}');
        }

        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }

        private int compareColumnOrder(TableColumnMetadata a, TableColumnMetadata b) {
            int aOrder = readColumnOrder.getQuick(a.getWriterIndex());
            int bOrder = readColumnOrder.getQuick(b.getWriterIndex());
            return Integer.compare(aOrder, bOrder);
        }
    }
}
