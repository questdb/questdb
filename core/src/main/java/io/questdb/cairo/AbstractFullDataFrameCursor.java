/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

public abstract class AbstractFullDataFrameCursor implements DataFrameCursor {
    protected final FullTableDataFrame frame = new FullTableDataFrame();
    protected int partitionHi;
    protected int partitionIndex;
    protected TableReader reader;

    @Override
    public void close() {
        reader = Misc.free(reader);
    }

    @Override
    public SymbolMapReader getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    public DataFrameCursor of(TableReader reader) {
        this.reader = reader;
        partitionHi = reader.getPartitionCount();
        toTop();
        return this;
    }

    @TestOnly
    @Override
    public boolean reload() {
        boolean moreData = reader.reload();
        partitionHi = reader.getPartitionCount();
        toTop();
        return moreData;
    }

    @Override
    public long size() {
        return reader.size();
    }

    protected class FullTableDataFrame implements DataFrame {
        protected int partitionIndex;
        protected long rowHi;
        protected long rowLo = 0;

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return reader.getBitmapIndexReader(partitionIndex, columnIndex, direction);
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }
    }
}
