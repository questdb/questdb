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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableStructure;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class TableModel implements TableStructure {
    private static final long COLUMN_FLAG_CACHED = 1L;
    private static final long COLUMN_FLAG_INDEXED = COLUMN_FLAG_CACHED << 1;
    private static final long COLUMN_FLAG_DEDUP_KEY = COLUMN_FLAG_INDEXED << 1;
    private final LongList columnBits = new LongList();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final CairoConfiguration configuration;
    private final String name;
    private final int partitionBy;
    private int timestampIndex = -1;
    private int ttlHoursOrMonths;
    private int walEnabled = -1;

    public TableModel(CairoConfiguration configuration, String name, int partitionBy) {
        this.configuration = configuration;
        this.name = name;
        this.partitionBy = partitionBy;
    }

    public TableModel cached(boolean cached) {
        int last = columnBits.size() - 1;
        assert last > 0;
        assert (ColumnType.isSymbol((int) columnBits.getQuick(last - 1)));
        long bits = columnBits.getQuick(last);
        if (cached) {
            columnBits.setQuick(last, bits | COLUMN_FLAG_CACHED);
        } else {
            columnBits.setQuick(last, bits & ~COLUMN_FLAG_CACHED);
        }
        return this;
    }

    public TableModel col(CharSequence name, int type) {
        columnNames.add(Chars.toString(name));
        // set default symbol capacity
        columnBits.add((128L << 32) | type, COLUMN_FLAG_CACHED);
        return this;
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public CharSequence getColumnName(int index) {
        return columnNames.getQuick(index);
    }

    @Override
    public int getColumnType(int index) {
        return (int) columnBits.getQuick(index * 2);
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public int getIndexBlockCapacity(int index) {
        return (int) (columnBits.getQuick(index * 2 + 1) >> 32);
    }

    @Override
    public int getMaxUncommittedRows() {
        return configuration.getMaxUncommittedRows();
    }

    public String getName() {
        return name;
    }

    @Override
    public long getO3MaxLag() {
        return configuration.getO3MaxLag();
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int index) {
        return (columnBits.getQuick(index * 2 + 1) & COLUMN_FLAG_CACHED) == COLUMN_FLAG_CACHED;
    }

    @Override
    public int getSymbolCapacity(int index) {
        return (int) (columnBits.getQuick(index * 2) >> 32);
    }

    @Override
    public CharSequence getTableName() {
        return name;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public int getTtlHoursOrMonths() {
        return ttlHoursOrMonths;
    }

    public TableModel indexed(boolean indexFlag, int indexBlockCapacity) {
        int pos = columnBits.size() - 1;
        assert pos > 0;
        long bits = columnBits.getQuick(pos);
        if (indexFlag) {
            assert indexBlockCapacity > 1;
            columnBits.setQuick(pos, bits | ((long) Numbers.ceilPow2(indexBlockCapacity) << 32) | COLUMN_FLAG_INDEXED);
        } else {
            columnBits.setQuick(pos, bits & ~COLUMN_FLAG_INDEXED);
        }
        return this;
    }

    @Override
    public boolean isDedupKey(int index) {
        return (columnBits.getQuick(index * 2 + 1) & COLUMN_FLAG_DEDUP_KEY) == COLUMN_FLAG_DEDUP_KEY;
    }

    @Override
    public boolean isIndexed(int index) {
        return (columnBits.getQuick(index * 2 + 1) & COLUMN_FLAG_INDEXED) == COLUMN_FLAG_INDEXED;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled == -1
                ? configuration.getWalEnabledDefault()
                : walEnabled == 1;
    }

    public TableModel noWal() {
        walEnabled = 0;
        return this;
    }

    public TableModel symbolCapacity(int capacity) {
        int pos = columnBits.size() - 2;
        assert pos > -1;
        long bits = columnBits.getQuick(pos);
        assert (ColumnType.isSymbol((int) bits));
        bits = (((long) capacity) << 32) | (int) bits;
        columnBits.setQuick(pos, bits);
        return this;
    }

    public TableModel timestamp(int timestampType) {
        switch (timestampType) {
            case ColumnType.TIMESTAMP_MICRO:
                return timestamp();
            case ColumnType.TIMESTAMP_NANO:
                return timestampNs();
        }
        return this;
    }

    public TableModel timestamp(CharSequence name, int timestampType) {
        switch (timestampType) {
            case ColumnType.TIMESTAMP_MICRO:
                return timestamp(name);
            case ColumnType.TIMESTAMP_NANO:
                return timestampNs(name);
        }
        return this;
    }

    public TableModel timestamp() {
        return timestamp("timestamp");
    }

    public TableModel timestamp(CharSequence name) {
        assert timestampIndex == -1;
        timestampIndex = columnNames.size();
        col(name, ColumnType.TIMESTAMP_MICRO);
        return this;
    }

    public TableModel timestampNs() {
        return timestampNs("timestamp");
    }

    public TableModel timestampNs(CharSequence name) {
        assert timestampIndex == -1;
        timestampIndex = columnNames.size();
        col(name, ColumnType.TIMESTAMP_NANO);
        return this;
    }

    public TableModel ttlHoursOrMonths(int ttlHoursOrMonths) {
        this.ttlHoursOrMonths = ttlHoursOrMonths;
        return this;
    }

    public TableModel wal() {
        walEnabled = 1;
        return this;
    }
}
