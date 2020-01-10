/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.Misc;
import io.questdb.std.microtime.Timestamps;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TableMeta implements Closeable {
    private final Path path;
    private final int rootLen;
    private final CairoConfiguration configuration;
    private final String tableName;
    private final TableReaderTransaction tx;
    private final TableReaderMetadata metadata;
    private final TimestampFloorMethod timestampFloorMethod;
    private final IntervalLengthMethod intervalLengthMethod;
    private final PartitionTimestampCalculatorMethod partitionTimestampCalculatorMethod;
    private final int partitionCount;

    public TableMeta(CairoConfiguration configuration, CharSequence tableName, IntList symbolCountSnapshot, LongHashSet removedPartitions) {
        this.configuration = configuration;
        this.tableName = Chars.toString(tableName);
        this.path = new Path().of(configuration.getRoot()).concat(tableName);
        this.rootLen = path.length();
        this.metadata = openMetaFile();
        switch (this.metadata.getPartitionBy()) {
            case PartitionBy.DAY:
                timestampFloorMethod = Timestamps::floorDD;
                intervalLengthMethod = Timestamps::getDaysBetween;
                partitionTimestampCalculatorMethod = Timestamps::addDays;
                break;
            case PartitionBy.MONTH:
                timestampFloorMethod = Timestamps::floorMM;
                intervalLengthMethod = Timestamps::getMonthsBetween;
                partitionTimestampCalculatorMethod = Timestamps::addMonths;
                break;
            case PartitionBy.YEAR:
                timestampFloorMethod = Timestamps::floorYYYY;
                intervalLengthMethod = Timestamps::getYearsBetween;
                partitionTimestampCalculatorMethod = Timestamps::addYear;
                break;
            default:
                timestampFloorMethod = timestamp -> timestamp;
                intervalLengthMethod = null;
                partitionTimestampCalculatorMethod = null;
                break;
        }
        this.tx = openTxnFile(symbolCountSnapshot, removedPartitions);
        partitionCount = calculatePartitionCount();
    }

    public int getColumnIndex(CharSequence columnName) {
        return metadata.getColumnIndex(columnName);
    }

    public CharSequence getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    public int getPartitionedBy() {
        return metadata.getPartitionBy();
    }

    public TableReaderTransaction getTx() {
        return tx;
    }

    public long getDataVersion() {
        return tx.getDataVersion();
    }

    public boolean isColumnIndexed(int columnIndex) {
        return metadata.isColumnIndexed(columnIndex);
    }

    public TableReaderMetadata getMetadata() {
        return metadata;
    }

    public long getPartitionTableVersion() {
        return tx.getPartitionTableVersion();
    }

    public long getRowCount() {
        return tx.getRowCount();
    }

    public long getTransientRowCount() {
        return tx.getTransientRowCount();
    }

    public long getStructVersion() {
        return tx.getStructVersion();
    }

    private TableReaderTransaction openTxnFile(IntList symbolCountSnapshot, LongHashSet removedPartitions) {
        try {
            return new TableReaderTransaction(configuration, path.concat(TableUtils.TXN_FILE_NAME).$(), symbolCountSnapshot, removedPartitions, timestampFloorMethod);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private TableReaderMetadata openMetaFile() {
        try {
            return new TableReaderMetadata(configuration.getFilesFacade(), path.concat(TableUtils.META_FILE_NAME).$());
        } finally {
            path.trimTo(rootLen);
        }
    }

    @FunctionalInterface
    private interface IntervalLengthMethod {
        long calculate(long minTimestamp, long maxTimestamp);
    }

    @FunctionalInterface
    interface PartitionTimestampCalculatorMethod {
        long calculate(long minTimestamp, int partitionIndex);
    }

    public long getMinTimestamp() {
        return tx.getMinTimestamp();
    }

    public long getMaxTimestamp() {
        return tx.getMaxTimestamp();
    }

    public int getPartitionBy() {
        return metadata.getPartitionBy();
    }

    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public void close() {
        Misc.free(metadata);
        Misc.free(tx);
        Misc.free(path);
    }

    public int calculatePartitionCount() {
        if (getMinTimestamp() == Long.MAX_VALUE) {
            return 0;
        } else {
            return getMaxTimestamp() == Long.MIN_VALUE ? 1 : getPartitionCountBetweenTimestamps(
                    getMinTimestamp(),
                    floorToPartitionTimestamp(getMaxTimestamp())
            ) + 1;
        }
    }

    public int getPartitionCountBetweenTimestamps(long partitionTimestamp1, long partitionTimestamp2) {
        return (int) intervalLengthMethod.calculate(partitionTimestamp1, partitionTimestamp2);
    }

    public long floorToPartitionTimestamp(long timestamp) {
        return timestampFloorMethod.floor(timestamp);
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public long addPartitionToTimestamp(long minTimestamp, int partitionIndex) {
        return partitionTimestampCalculatorMethod.calculate(minTimestamp, partitionIndex);
    }

    public PartitionTimestampCalculatorMethod getPartitionTimestampCalculatorMethod() {
        return partitionTimestampCalculatorMethod;
    }

    public CharSequence getTableName() {
        return tableName;
    }
}
