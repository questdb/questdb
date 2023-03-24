/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.cairo.TableReaderTest.assertOpenPartitionCount;
import static org.junit.Assert.assertEquals;

public class TableReaderReloadFuzzTest extends AbstractGriffinTest {
    private static final int ADD = 0;
    private static final Log LOG = LogFactory.getLog(TableReaderReloadFuzzTest.class);
    private static final int MAX_NUM_OF_INSERTS = 10;
    private static final int MAX_NUM_OF_STRUCTURE_CHANGES = 10;
    private static final int MIN_NUM_OF_INSERTS = 1;
    private static final int MIN_NUM_OF_STRUCTURE_CHANGES = 5;
    private static final long ONE_DAY = 24 * 60 * 60 * 1000L * 1000L;
    private static final long ONE_YEAR = 365 * 24 * 60 * 60 * 1000L * 1000L;
    private static final int REMOVE = 1;
    private static final int RENAME = 2;
    private final AtomicInteger columNameGen = new AtomicInteger(0);
    private final ObjList<Column> columns = new ObjList<>();
    private final IntList removableColumns = new IntList();
    private Rnd random;

    @Before
    public void setUp() {
        super.setUp();
        random = TestUtils.generateRandom(LOG);
    }

    @Test
    public void testAddRemove() {
        testFuzzReload(20, 1, 1, 0);
    }

    @Test
    public void testBalanced() {
        testFuzzReload(20, 1, 1, 1);
    }

    @Test
    public void testExplosion() throws SqlException {
        final String tableName = "exploding";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).timestamp()) {
            CreateTableTestUtils.create(model);
        }

        try (TableWriter writer = newTableWriter(configuration, tableName, metrics)) {
            TableWriter.Row row = writer.newRow(0L);
            row.append();
            writer.commit();

            try (TableReader reader = newTableReader(configuration, tableName)) {
                TestUtils.printSql(compiler, sqlExecutionContext, tableName, sink);

                for (int i = 0; i < 64; i++) {
                    writer.addColumn("col" + i, ColumnType.INT);
                    row = writer.newRow(i * ONE_DAY);
                    row.append();
                }
                writer.commit();

                reader.reload();
                assertReaderWriterMetadata(writer, reader);
                assertOpenPartitionCount(reader);
            }
        }
    }

    @Test
    public void testMostlyAdd() {
        testFuzzReload(10, 3, 1, 1);
    }

    @Test
    public void testMostlyDelete() {
        testFuzzReload(5, 1, 4, 0);
    }

    @Test
    public void testRename() {
        testFuzzReload(15, 0, 0, 1);
    }

    private void assertReaderWriterMetadata(TableWriter writer, TableReader reader) {
        final ObjList<Column> writerColumns = extractLiveColumns(writer.getMetadata());
        final TableReaderMetadata readerMetadata = reader.getMetadata();
        for (int k = 0; k < readerMetadata.getColumnCount(); k++) {
            assertEquals(writerColumns.get(k).name, readerMetadata.getColumnName(k));
            assertEquals(writerColumns.get(k).type, readerMetadata.getColumnType(k));
        }
    }

    private void changeTableStructure(int addFactor, int removeFactor, int renameFactor, TableWriter writer) {
        final TableRecordMetadata writerMetadata = writer.getMetadata();
        final int numOfStructureChanges = MIN_NUM_OF_STRUCTURE_CHANGES + random.nextInt(MAX_NUM_OF_STRUCTURE_CHANGES - MIN_NUM_OF_STRUCTURE_CHANGES);
        for (int j = 0; j < numOfStructureChanges; j++) {
            final int structureChangeType = selectStructureChange(addFactor, removeFactor, renameFactor);
            switch (structureChangeType) {
                case ADD:
                    final int columnType = random.nextInt(12) + 1;
                    writer.addColumn("col" + columNameGen.incrementAndGet(), columnType);
                    break;
                case REMOVE:
                    final int removeIndex = selectColumn(writerMetadata);
                    if (removeIndex > -1) {
                        writer.removeColumn(writerMetadata.getColumnName(removeIndex));
                    }
                    break;
                case RENAME:
                    final int renameIndex = selectColumn(writerMetadata);
                    if (renameIndex > -1) {
                        writer.renameColumn(writerMetadata.getColumnName(renameIndex), "col" + columNameGen.incrementAndGet());
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid structure change type value [type=" + structureChangeType + "]");
            }
        }
    }

    private void createTable() {
        try (TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, PartitionBy.DAY)) {
            model.timestamp();
            CreateTableTestUtils.create(model);
        }
    }

    private ObjList<Column> extractLiveColumns(TableRecordMetadata metadata) {
        columns.clear();
        final int columnCount = metadata.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            final int type = metadata.getColumnType(i);
            if (type > -1) {
                columns.add(new Column(metadata.getColumnName(i), type));
            }
        }
        return columns;
    }

    private void ingest(TableWriter writer) {
        final int numOfInserts = MIN_NUM_OF_INSERTS + random.nextInt(MAX_NUM_OF_INSERTS - MIN_NUM_OF_INSERTS);
        for (int i = 0; i < numOfInserts; i++) {
            final TableWriter.Row row = writer.newRow(random.nextLong(ONE_YEAR));
            row.append();
        }
        writer.commit();
    }

    private int selectColumn(TableRecordMetadata metadata) {
        final int columnCount = metadata.getColumnCount();
        final int timestampIndex = metadata.getTimestampIndex();

        removableColumns.clear();
        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) > -1 && i != timestampIndex) {
                removableColumns.add(i);
            }
        }

        final int numOfColumns = removableColumns.size();
        if (numOfColumns > 0) {
            return removableColumns.get(random.nextInt(numOfColumns));
        }
        return -1;
    }

    private int selectStructureChange(int addFactor, int removeFactor, int renameFactor) {
        final int x = random.nextInt(addFactor + removeFactor + renameFactor);
        if (x < addFactor) {
            return ADD;
        }
        if (x < addFactor + removeFactor) {
            return REMOVE;
        }
        return RENAME;
    }

    private void testFuzzReload(int numOfReloads, int addFactor, int removeFactor, int renameFactor) {
        createTable();
        try (TableWriter writer = newTableWriter(configuration, "all", metrics)) {
            try (TableReader reader = newTableReader(configuration, "all")) {
                for (int i = 0; i < numOfReloads; i++) {
                    ingest(writer);
                    changeTableStructure(addFactor, removeFactor, renameFactor, writer);
                    reader.reload();
                    assertReaderWriterMetadata(writer, reader);
                    assertOpenPartitionCount(reader);
                }
            }
        }
    }

    private static class Column {
        String name;
        int type;

        public Column(String name, int type) {
            this.name = name;
            this.type = type;
        }
    }
}
