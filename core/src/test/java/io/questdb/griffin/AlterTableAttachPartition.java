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

package io.questdb.griffin;

import io.questdb.cairo.CairoTestUtils;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import org.junit.Test;

import java.io.IOException;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


public class AlterTableAttachPartition extends AbstractGriffinTest {

    @Test
    public void testAttachePartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst", PartitionBy.DAY)) {

                createSequentialDailyPartitionTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000, "2020-01-01", 10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try (Path p1 = new Path().of(configuration.getRoot()).concat(src.getName()).concat("2020-01-01").$();
                     Path p2 = new Path().of(configuration.getRoot()).concat(dst.getName()).concat("2020-01-01").$()) {
                    copyDirectory(p1, p2);
                }

                compiler.compile("ALTER TABLE dst ATTACH PARTITION LIST '2020-01-01';", sqlExecutionContext);
            }
        });
    }

    private void copyDirectory(Path from, Path to) throws IOException {
        Files.mkdir(to, 0);

        java.nio.file.Path dest = java.nio.file.Path.of(to.toString() + Files.SEPARATOR);
        java.nio.file.Path src = java.nio.file.Path.of(from.toString() + Files.SEPARATOR);
        java.nio.file.Files.walk(src)
                .forEach(file -> {
                    java.nio.file.Path destination = dest.resolve(src.relativize(file));
                    try {
                        java.nio.file.Files.copy(file, destination, REPLACE_EXISTING);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    private void createSequentialDailyPartitionTable(TableModel tableModel, int totalRows, String startDate, int partitionCount) throws NumericException, SqlException {
        long fromTimestamp = TimestampFormatUtils.parseTimestamp(startDate + "T00:00:00.000Z");
        long increment = totalRows > 0 ? Math.max((Timestamps.addDays(fromTimestamp, partitionCount - 1) - fromTimestamp) / totalRows, 1) : 0;

        StringBuilder sql = new StringBuilder();
        sql.append("create table " + tableModel.getName() + " as (" + Misc.EOL + "select" + Misc.EOL);
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            int colType = tableModel.getColumnType(i);
            CharSequence colName = tableModel.getColumnName(i);
            switch (colType) {
                case ColumnType.INT:
                    sql.append("cast(x as int) " + colName);
                    break;
                case ColumnType.STRING:
                    sql.append("CAST(x as STRING) " + colName);
                    break;
                case ColumnType.LONG:
                    sql.append("x " + colName);
                    break;
                case ColumnType.DOUBLE:
                    sql.append("x / 1000.0 " + colName);
                    break;
                case ColumnType.TIMESTAMP:
                    sql.append("CAST(" + fromTimestamp + "L AS TIMESTAMP) + x * " + increment + "  " + colName);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            if (i < tableModel.getColumnCount() - 1) {
                sql.append("," + Misc.EOL);
            }
        }

        sql.append(Misc.EOL + "from long_sequence(" + totalRows + ")");
        sql.append(")" + Misc.EOL);
        if (tableModel.getTimestampIndex() != -1) {
            CharSequence timestampCol = tableModel.getColumnName(tableModel.getTimestampIndex());
            sql.append(" timestamp(" + timestampCol + ") Partition By DAY");
        }
        compiler.compile(sql.toString(), sqlExecutionContext);
    }
}
