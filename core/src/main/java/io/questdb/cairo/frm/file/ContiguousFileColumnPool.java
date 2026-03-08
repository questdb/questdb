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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.cairo.frm.FrameColumnPool;
import io.questdb.cairo.frm.FrameColumnTypePool;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.mp.ConcurrentPool;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class ContiguousFileColumnPool implements FrameColumnPool, Closeable {
    private final ColumnTypePool columnTypePool = new ColumnTypePool();
    private final ConcurrentQueuePool<ContiguousFileFixFrameColumn> fixColumnPool;
    private final ConcurrentQueuePool<MemoryFixFrameColumn> fixMemColumnPool;
    private final ConcurrentQueuePool<ContiguousFileFixFrameColumn> indexedColumnPool;
    private final ConcurrentQueuePool<ContiguousFileVarFrameColumn> varColumnPool;
    private final ConcurrentQueuePool<MemoryVarFrameColumn> varMemColumnPool;
    private boolean isClosed;

    public ContiguousFileColumnPool(CairoConfiguration configuration) {
        fixColumnPool = new ConcurrentQueuePool<>(() -> new ContiguousFileFixFrameColumn(configuration));
        fixMemColumnPool = new ConcurrentQueuePool<>(MemoryFixFrameColumn::new);
        indexedColumnPool = new ConcurrentQueuePool<>(() -> new ContiguousFileIndexedFrameColumn(configuration));
        varColumnPool = new ConcurrentQueuePool<>(() -> new ContiguousFileVarFrameColumn(configuration));
        varMemColumnPool = new ConcurrentQueuePool<>(MemoryVarFrameColumn::new);
    }

    @Override
    public void close() {
        this.isClosed = true;
    }

    @Override
    public FrameColumnTypePool getPool(int columnType) {
        return columnTypePool;
    }

    private class ColumnTypePool implements FrameColumnTypePool {

        @Override
        public FrameColumn create(
                Path partitionPath,
                CharSequence columnName,
                long columnTxn,
                int columnType,
                int indexBlockCapacity,
                long columnTop,
                int columnIndex,
                boolean isEmpty,
                boolean canWrite
        ) {
            boolean isIndexed = indexBlockCapacity > 0;

            if (ColumnType.isVarSize(columnType)) {
                ContiguousFileVarFrameColumn column = getVarColumn();
                if (canWrite) {
                    column.ofRW(partitionPath, columnName, columnTxn, columnType, columnTop, columnIndex);
                } else {
                    column.ofRO(partitionPath, columnName, columnTxn, columnType, columnTop, columnIndex, isEmpty);
                }
                return column;
            }

            if (columnType == ColumnType.SYMBOL) {
                if (canWrite && isIndexed) {
                    ContiguousFileIndexedFrameColumn indexedColumn = getIndexedColumn();
                    indexedColumn.ofRW(partitionPath, columnName, columnTxn, columnType, indexBlockCapacity, columnTop, columnIndex, isEmpty);
                    return indexedColumn;
                }
            }

            ContiguousFileFixFrameColumn column = getFixColumn();
            if (canWrite) {
                column.ofRW(partitionPath, columnName, columnTxn, columnType, columnTop, columnIndex);
            } else {
                column.ofRO(partitionPath, columnName, columnTxn, columnType, columnTop, columnIndex, isEmpty);
            }
            return column;
        }

        @Override
        public FrameColumn createFromMemoryColumn(
                int columnIndex,
                int columnType,
                long rowCount,
                MemoryCR columnMemoryPrimary,
                MemoryCR columnMemorySecondary
        ) {
            if (!ColumnType.isVarSize(columnType)) {
                // Fixed column
                MemoryFixFrameColumn column = getMemFixColumn();
                column.of(
                        columnIndex,
                        columnType,
                        rowCount,
                        columnMemoryPrimary
                );
                return column;
            } else {
                // Variable column
                MemoryVarFrameColumn column = getMemVarColumn();
                column.of(
                        columnIndex,
                        columnType,
                        rowCount,
                        columnMemoryPrimary,
                        columnMemorySecondary
                );
                return column;
            }
        }

        private ContiguousFileFixFrameColumn getFixColumn() {
            return fixColumnPool.pop();
        }

        private ContiguousFileIndexedFrameColumn getIndexedColumn() {
            return (ContiguousFileIndexedFrameColumn) indexedColumnPool.pop();
        }

        private MemoryFixFrameColumn getMemFixColumn() {
            return fixMemColumnPool.pop();
        }

        private MemoryVarFrameColumn getMemVarColumn() {
            return varMemColumnPool.pop();
        }

        private ContiguousFileVarFrameColumn getVarColumn() {
            return varColumnPool.pop();
        }
    }

    private class ConcurrentQueuePool<T extends FrameColumn> implements RecycleBin<T> {
        private final ObjectFactory<T> factory;
        private final ConcurrentPool<T> pool = new ConcurrentPool<>();

        public ConcurrentQueuePool(ObjectFactory<T> factory) {
            this.factory = factory;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        public T pop() {
            T item = pool.pop();
            if (item != null) {
                return item;
            }
            item = factory.newInstance();
            //noinspection unchecked
            item.setRecycleBin((RecycleBin<FrameColumn>) this);
            return item;
        }

        @Override
        public void put(T frame) {
            pool.push(frame);
        }
    }
}
