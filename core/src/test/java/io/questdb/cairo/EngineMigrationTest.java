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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.EngineMigration.TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT;
import static io.questdb.cairo.EngineMigration.VERSION_TX_STRUCT_UPDATE_1;
import static io.questdb.cairo.TableUtils.*;

public class EngineMigrationTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testMigrateTableNoSymbolsNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.NONE)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 0
                );

                String query = "select sum(c1) from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithDayPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.DAY)) {
                createPopulateTable(
                        src.col("c1", ColumnType.INT).col("ts", ColumnType.TIMESTAMP).timestamp(),
                        100, "2020-01-01", 10
                );

                String query = "select sum(c1) from src";
                assertMigration(src, query);
            }
        });
    }

    @Test
    public void testMigrateTableWithSymbols() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", PartitionBy.NONE)) {
                createPopulateTable(
                        src.col("s1", ColumnType.SYMBOL).indexed(true, 4096)
                                .col("c1", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("c2", ColumnType.LONG)
                                .col("ts", ColumnType.TIMESTAMP).timestamp(),
                        10, "2020-01-01", 0
                );

                String query = "select distinct s1, s2 from src";
                assertMigration(src, query);
            }
        });
    }

    private static DateFormat getPartitionDateFmt(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return fmtDay;
            case PartitionBy.MONTH:
                return fmtMonth;
            case PartitionBy.YEAR:
                return fmtYear;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
        }
    }

    private void assertMigration(TableModel src, String query) throws SqlException {
        var expected = executeSql(query).toString();

        // There are no symbols, no partition, tx file is same. Downgrade version
        downgradeTxFile(src, null);

        // Act
        new EngineMigration(engine, configuration).migrateEngineTo(ColumnType.VERSION);

        // Verify
        TestUtils.assertEquals(expected, executeSql(query));
    }

    private void downgradeTxFile(TableModel src, LongList removedPartitions) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        try (var path = new Path()) {
            path.concat(root).concat(src.getName()).concat(TableUtils.META_FILE_NAME);
            var ff = configuration.getFilesFacade();
            try (var rwTx = new ReadWriteMemory(ff, path.$(), ff.getPageSize())) {
                rwTx.putInt(META_OFFSET_VERSION, VERSION_TX_STRUCT_UPDATE_1 - 1);
            }

            // Read current symbols list
            var symbolCounts = new IntList();
            path.trimTo(0).concat(root).concat(src.getName());
            var attachedPartitions = new LongList();
            try (var txFile = new TransactionFileReader(ff, path.$())) {
                txFile.initPartitionBy(src.getPartitionBy());
                txFile.open();
                txFile.read();

                for (int i = 0; i < txFile.getAttachedPartitionsCount() - 1; i++) {
                    attachedPartitions.add(txFile.getPartitionTimestamp(i));
                    attachedPartitions.add(txFile.getPartitionSize(i));
                }
                txFile.readSymbolCounts(symbolCounts);
            }

            path.trimTo(0).concat(root).concat(src.getName()).concat(TXN_FILE_NAME);
            try (var rwTx = new ReadWriteMemory(ff, path.$(), ff.getPageSize())) {
                rwTx.putInt(TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT, symbolCounts.size());
                rwTx.jumpTo(TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + 4);

                // Tx file used to have 4 bytes per symbol
                for (int i = 0; i < symbolCounts.size(); i++) {
                    rwTx.putInt(symbolCounts.getQuick(i));
                }

                // and stored removed partitions list
                if (removedPartitions != null) {
                    rwTx.putLong(removedPartitions.size());
                    for (int i = 0; i < removedPartitions.size(); i++) {
                        rwTx.putLong(removedPartitions.getQuick(i));
                    }
                } else {
                    rwTx.putInt(0);
                }
            }

            // and have file _archive in each folder the file size except last partition
            if (src.getPartitionBy() != PartitionBy.NONE) {
                var partitionFmt = getPartitionDateFmt(src.getPartitionBy());
                StringSink sink = new StringSink();
                for (int i = 0; i < attachedPartitions.size() / 2; i++) {
                    long partitionTs = attachedPartitions.getQuick(i * 2);
                    long partitionSize = attachedPartitions.getQuick(i * 2 + 1);
                    sink.clear();
                    partitionFmt.format(partitionTs, null, null, sink);
                    path.trimTo(0).concat(root).concat(src.getName()).concat(sink).concat("_archive");
                    try (var rwAr = new ReadWriteMemory(ff, path.$(), 8)) {
                        rwAr.putLong(partitionSize);
                    }
                }
            }

            path.trimTo(0).concat(root).concat(UPGRADE_FILE_NAME);
            if (ff.exists(path.$())) {
                ff.remove(path.$());
            }
//            try (var rwTx = new ReadWriteMemory(ff, path.$(), 1024)) {
//                rwTx.putInt(VERSION_TX_STRUCT_UPDATE_1 - 1);
//            }
        }
    }

    private CharSequence executeSql(String sql) throws SqlException {
        try (RecordCursorFactory rcf = compiler.compile(sql
                , sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = rcf.getCursor(sqlExecutionContext)) {
                sink.clear();
                printer.print(cursor, rcf.getMetadata(), true);
                return sink;
            }
        }
    }
}
