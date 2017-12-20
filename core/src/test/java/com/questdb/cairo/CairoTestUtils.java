/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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
import com.questdb.common.PartitionBy;
import com.questdb.std.Files;
import com.questdb.std.FilesFacade;
import com.questdb.std.microtime.Dates;
import com.questdb.std.str.Path;

import static com.questdb.cairo.TableUtils.META_FILE_NAME;
import static com.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class CairoTestUtils {

    public static void create(TableModel model) {
        final Path path = model.getPath();
        final FilesFacade ff = model.getCairoCfg().getFilesFacade();

        path.of(model.getCairoCfg().getRoot()).concat(model.getName());
        final int rootLen = path.length();
        if (ff.mkdirs(path.put(Files.SEPARATOR).$(), model.getCairoCfg().getMkDirMode()) == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot create dir: ").put(path);
        }

        try (AppendMemory mem = model.getMem()) {

            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

            int count = model.getColumnCount();
            int symbolMapCount = 0;
            mem.putInt(count);
            mem.putInt(model.getPartitionBy());
            mem.putInt(model.getTimestampIndex());
            for (int i = 0; i < count; i++) {
                int type = model.getColumnType(i);
                mem.putInt(type);
                if (type == ColumnType.SYMBOL) {
                    symbolMapCount++;
                }

            }
            for (int i = 0; i < count; i++) {
                mem.putStr(model.getColumnName(i));
            }

            // create symbol maps
            for (int i = 0; i < count; i++) {
                int type = model.getColumnType(i);
                if (type == ColumnType.SYMBOL) {
                    CharSequence columnName = model.getColumnName(i);
                    mem.of(ff, path.trimTo(rootLen).concat(columnName).put(".o").$(), ff.getPageSize());
                    mem.putInt(model.getSymbolCapacity(i));
                    mem.putBool(model.getSymbolCacheFlag(i));
                    mem.jumpTo(SymbolMapWriter.HEADER_SIZE);
                    mem.close();

                    if (!ff.touch(path.trimTo(rootLen).concat(columnName).put(".c").$())) {
                        throw CairoException.instance(ff.errno()).put("Cannot create ").put(path);
                    }

                    BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName);
                    mem.of(ff, path, ff.getPageSize());
                    BitmapIndexWriter.initKeyMemory(mem, 4);
                }
            }
            mem.of(ff, path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), ff.getPageSize());
            TableUtils.resetTxn(mem, symbolMapCount);
        }
    }

    public static void createAllTable(CairoConfiguration configuration, int partitionBy) {
        try (TableModel model = getAllTypesModel(configuration, partitionBy)) {
            create(model);
        }
    }

    public static TableModel getAllTypesModel(CairoConfiguration configuration, int partitionBy) {
        return new TableModel(configuration, "all", partitionBy)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL).symbolCapacity(64)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE);
    }

    static boolean isSamePartition(long timestampA, long timestampB, int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.NONE:
                return true;
            case PartitionBy.DAY:
                return Dates.floorDD(timestampA) == Dates.floorDD(timestampB);
            case PartitionBy.MONTH:
                return Dates.floorMM(timestampA) == Dates.floorMM(timestampB);
            case PartitionBy.YEAR:
                return Dates.floorYYYY(timestampA) == Dates.floorYYYY(timestampB);
            default:
                throw CairoException.instance(0).put("Cannot compare timestamps for unsupported partition type: [").put(partitionBy).put(']');
        }
    }
}
