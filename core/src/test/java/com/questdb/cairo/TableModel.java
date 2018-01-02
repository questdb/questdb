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

import com.questdb.common.ColumnType;
import com.questdb.std.Chars;
import com.questdb.std.LongList;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.str.Path;

import java.io.Closeable;

public class TableModel implements Closeable {
    private final String name;
    private final int partitionBy;
    private final AppendMemory mem = new AppendMemory();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final LongList columnBits = new LongList();
    private final Path path = new Path();
    private final CairoConfiguration cairoCfg;
    private int timestampIndex = -1;

    public TableModel(CairoConfiguration cairoCfg, String name, int partitionBy) {
        this.cairoCfg = cairoCfg;
        this.name = name;
        this.partitionBy = partitionBy;
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(path);
    }

    public TableModel col(CharSequence name, int type) {
        columnNames.add(Chars.stringOf(name));
        // set default symbol capacity
        columnBits.add((128L << 32) | type);
        columnBits.add(1L);
        return this;
    }

    public CairoConfiguration getCairoCfg() {
        return cairoCfg;
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    public CharSequence getColumnName(int index) {
        return columnNames.getQuick(index);
    }

    public int getColumnType(int index) {
        return (int) columnBits.getQuick(index * 2);
    }

    public AppendMemory getMem() {
        return mem;
    }

    public String getName() {
        return name;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public Path getPath() {
        return path;
    }

    public boolean getSymbolCacheFlag(int index) {
        long bits = columnBits.getQuick(index * 2 + 1);
        return (bits & 1L) == 1L;
    }

    public int getSymbolCapacity(int index) {
        long bits = columnBits.getQuick(index * 2);
        return (int) (bits >> 32);
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public TableModel symbolCapacity(int capacity) {
        int last = columnBits.size() - 2;
        assert last > -1;
        long bits = columnBits.getQuick(last);
        assert ((int) bits == ColumnType.SYMBOL);
        bits = (((long) capacity) << 32) | (int) bits;
        columnBits.setQuick(last, bits);
        return this;
    }

    public TableModel timestamp() {
        assert timestampIndex == -1;
        timestampIndex = columnNames.size();
        col("timestamp", ColumnType.TIMESTAMP);
        return this;
    }
}
