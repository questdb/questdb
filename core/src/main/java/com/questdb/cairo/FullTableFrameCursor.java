/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
import com.questdb.common.SymbolTable;

public class FullTableFrameCursor implements DataFrameCursor {
    private final FullTableDataFrame frame = new FullTableDataFrame();
    private TableReader reader;
    private int partitionLo;
    private int partitionHi;
    private int partitionIndex;

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public boolean hasNext() {
        while (this.partitionIndex < partitionHi) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex++;
            } else {
                frame.partitionIndex = partitionIndex;
                frame.rowHi = hi;
                partitionIndex++;
                return true;

            }
        }
        return false;
    }

    @Override
    public DataFrame next() {
        return frame;
    }

    public FullTableFrameCursor of(TableReader reader) {
        this.reader = reader;
        this.partitionIndex = this.partitionLo = 0;
        this.partitionHi = reader.getPartitionCount();
        return this;
    }

    @Override
    public boolean reload() {
        boolean moreData = reader.reload();
        this.partitionHi = reader.getPartitionCount();
        toTop();
        return moreData;
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public void toTop() {
        this.partitionIndex = this.partitionLo;
    }

    private class FullTableDataFrame implements DataFrame {
        final static private long rowLo = 0;
        private long rowHi;
        private int partitionIndex;

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex) {
            return reader.getBitmapIndexReader(reader.getColumnBase(partitionIndex), columnIndex);
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

        @Override
        public TableReader getTableReader() {
            return reader;
        }
    }
}
