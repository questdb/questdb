/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.cairo.sql.SymbolTable;

public abstract class AbstractFullDataFrameCursor implements DataFrameCursor {
    protected final FullTableDataFrame frame = new FullTableDataFrame();
    protected TableReader reader;
    protected int partitionHi;
    protected int partitionIndex;

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public boolean reload() {
        boolean moreData = reader.reload();
        this.partitionHi = reader.getPartitionCount();
        toTop();
        return moreData;
    }

    @Override
    public DataFrame next() {
        return frame;
    }

    public DataFrameCursor of(TableReader reader) {
        this.reader = reader;
        this.partitionHi = reader.getPartitionCount();
        toTop();
        return this;
    }

    protected class FullTableDataFrame implements DataFrame {
        final static private long rowLo = 0;
        protected long rowHi;
        protected int partitionIndex;

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return reader.getBitmapIndexReader(reader.getColumnBase(partitionIndex), columnIndex, direction);
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
