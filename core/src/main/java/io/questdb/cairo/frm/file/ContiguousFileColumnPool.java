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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.cairo.frm.FrameColumnPool;
import io.questdb.cairo.frm.FrameColumnTypePool;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class ContiguousFileColumnPool implements FrameColumnPool, Closeable {
    private final ColumnTypePool columnTypePool = new ColumnTypePool();
    private final CairoConfiguration configuration;
    private final ListPool<ContiguousFileFixFrameColumn> fixColumnPool = new ListPool<>();
    private final ListPool<ContiguousFileFixFrameColumn> indexedColumnPool = new ListPool<>();
    private final ListPool<ContiguousFileVarFrameColumn> varColumnPool = new ListPool<>();
    private boolean canWrite;
    private boolean isClosed;

    public ContiguousFileColumnPool(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }

    @Override
    public FrameColumnTypePool getPoolRO(int columnType) {
        this.canWrite = false;
        return columnTypePool;
    }

    @Override
    public FrameColumnTypePool getPoolRW(int columnType) {
        this.canWrite = true;
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
                boolean isEmpty
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

        private ContiguousFileFixFrameColumn getFixColumn() {
            if (fixColumnPool.size() > 0) {
                return fixColumnPool.pop();
            }
            ContiguousFileFixFrameColumn col = new ContiguousFileFixFrameColumn(configuration);
            col.setPool(fixColumnPool);
            return col;
        }

        private ContiguousFileIndexedFrameColumn getIndexedColumn() {
            if (indexedColumnPool.size() > 0) {
                return (ContiguousFileIndexedFrameColumn) indexedColumnPool.pop();
            }
            ContiguousFileIndexedFrameColumn col = new ContiguousFileIndexedFrameColumn(configuration);
            col.setPool(indexedColumnPool);
            return col;
        }

        private ContiguousFileVarFrameColumn getVarColumn() {
            if (varColumnPool.size() > 0) {
                return varColumnPool.pop();
            }
            ContiguousFileVarFrameColumn col = new ContiguousFileVarFrameColumn(configuration);
            col.setPool(varColumnPool);
            return col;
        }
    }

    private class ListPool<T> implements RecycleBin<T> {
        private final ObjList<T> pool = new ObjList<>();

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        public T pop() {
            T last = pool.getLast();
            pool.setPos(pool.size() - 1);
            return last;
        }

        @Override
        public void put(T frame) {
            pool.add(frame);
        }

        public int size() {
            return pool.size();
        }
    }
}
