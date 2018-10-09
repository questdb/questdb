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

import com.questdb.std.Files;
import com.questdb.std.FilesFacade;
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
            mem.putInt(count);
            mem.putInt(model.getPartitionBy());
            mem.putInt(model.getTimestampIndex());
            mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < count; i++) {
                mem.putByte((byte) model.getColumnType(i));
                mem.putBool(model.getIndexedFlag(i));
                mem.putInt(model.getIndexBlockCapacity(i));
                mem.skip(10); // reserved
            }
            for (int i = 0; i < count; i++) {
                mem.putStr(model.getColumnName(i));
            }

            // create symbol maps
            int symbolMapCount = 0;
            for (int i = 0; i < count; i++) {
                if (model.getColumnType(i) == ColumnType.SYMBOL) {
                    SymbolMapWriter.createSymbolMapFiles(
                            ff,
                            mem,
                            path.trimTo(rootLen),
                            model.getColumnName(i),
                            model.getSymbolCapacity(i),
                            model.getSymbolCacheFlag(i)
                    );
                    symbolMapCount++;
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

}
