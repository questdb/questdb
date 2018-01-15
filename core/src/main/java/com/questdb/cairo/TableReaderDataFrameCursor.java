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
import com.questdb.common.RecordMetadata;
import com.questdb.common.SymbolTable;

public class TableReaderDataFrameCursor implements DataFrameCursor {
    private final TableReaderDataFrame frame = new TableReaderDataFrame();
    private TableReader reader;
    private int partitionLo;
    private int partitionHi;
    private int partitionIndex;


    @Override
    public void closeCursor() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return reader.getMetadata();
    }

    @Override
    public void toTop() {
        this.partitionIndex = this.partitionLo;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return this.partitionIndex < partitionHi;
    }

    @Override
    public DataFrame next() {
        frame.partitionIndex = partitionIndex;
        frame.rowLo = 0;
        frame.rowHi = reader.openPartition(partitionIndex++);
        return frame;
    }

    public TableReaderDataFrameCursor of(TableReader reader, int partitionLo, int partitionHi) {
        this.reader = reader;
        this.partitionIndex = this.partitionLo = partitionLo;
        this.partitionHi = partitionHi;
        return this;
    }

    private class TableReaderDataFrame implements DataFrame {
        private long rowLo;
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
    }
}
