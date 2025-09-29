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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.cairo.TableReaderTest.assertOpenPartitionCount;
import static org.junit.Assert.assertEquals;

public class TableReaderReloadFuzzTest extends AbstractCairoTest {
    private static final int ADD = 0;
    private static final int CONVERT = 3;
    private static final Log LOG = LogFactory.getLog(TableReaderReloadFuzzTest.class);
    private static final int MAX_NUM_OF_INSERTS = 10;
    private static final int MAX_NUM_OF_STRUCTURE_CHANGES = 10;
    private static final int MIN_NUM_OF_INSERTS = 1;
    private static final int MIN_NUM_OF_STRUCTURE_CHANGES = 5;
    private static final long ONE_YEAR = 365 * 24 * 60 * 60 * 1000L * 1000L;
    private static final int REMOVE = 1;
    private static final int RENAME = 2;
    private final AtomicInteger columnNameGen = new AtomicInteger(0);
    private final ObjList<Column> columns = new ObjList<>();
    private final IntList removableColumns = new IntList();
    private Rnd random;
    private TimestampDriver timestampDriver;
    private int timestampType;

    @Before
    public void setUp() {
        super.setUp();
        random = TestUtils.generateRandom(LOG);
        timestampType = random.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
        timestampDriver = ColumnType.getTimestampDriver(timestampType);
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
    public void testConvertPartition() {
        testFuzzReload(10, 1);
    }

    @Test
    public void testExplosion() throws Exception {
        final String tableName = "exploding";
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).timestamp();
        AbstractCairoTest.create(model);

        try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
            TableWriter.Row row = writer.newRow(0L);
            row.append();
            writer.commit();

            try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
                engine.print(tableName, sink, sqlExecutionContext);

                for (int i = 0; i < 64; i++) {
                    writer.addColumn("col" + i, ColumnType.INT);
                    row = writer.newRow(timestampDriver.fromDays(i));
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

    private void changeTableStructure(int addFactor, int removeFactor, int renameFactor, int convertFactor, TableWriter writer) {
        final TableRecordMetadata writerMetadata = writer.getMetadata();
        final int numOfStructureChanges = MIN_NUM_OF_STRUCTURE_CHANGES + random.nextInt(MAX_NUM_OF_STRUCTURE_CHANGES - MIN_NUM_OF_STRUCTURE_CHANGES);
        for (int j = 0; j < numOfStructureChanges; j++) {
            final int structureChangeType = selectStructureChange(addFactor, removeFactor, renameFactor, convertFactor);
            switch (structureChangeType) {
                case ADD:
                    final int columnType = random.nextInt(12) + 1;
                    writer.addColumn("col" + columnNameGen.incrementAndGet(), columnType);
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
                        writer.renameColumn(writerMetadata.getColumnName(renameIndex), "col" + columnNameGen.incrementAndGet());
                    }
                    break;
                case CONVERT:
                    final int partitionCount = writer.getPartitionCount();
                    final boolean convert = partitionCount > 2 && random.nextBoolean();
                    if (convert) {
                        final int partition = Math.max(0, random.nextInt(partitionCount - 1));
                        final boolean delete = random.nextBoolean();
                        final boolean isParquet = writer.getPartitionParquetFileSize(partition) > 0;
                        final long timestamp = writer.getPartitionTimestamp(partition);
                        if (isParquet) {
                            writer.convertPartitionParquetToNative(timestamp);
                        } else {
                            writer.convertPartitionNativeToParquet(timestamp);
                            if (delete) {
                                writer.convertPartitionNativeToParquet(writer.getPartitionTimestamp(1));
                                writer.removePartition(writer.getPartitionTimestamp(0));
                            }
                        }
                        ingest(writer);
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid structure change type value [type=" + structureChangeType + "]");
            }
        }
    }

    private void createTable() {
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, PartitionBy.DAY);
        model.timestamp(timestampType);
        AbstractCairoTest.create(model);
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
            final TableWriter.Row row = writer.newRow(random.nextLong(timestampDriver.fromMicros(ONE_YEAR)));
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

    private int selectStructureChange(int addFactor, int removeFactor, int renameFactor, int convertFactor) {
        final int x = random.nextInt(addFactor + removeFactor + renameFactor + convertFactor);
        if (x < addFactor) {
            return ADD;
        }
        if (x < addFactor + removeFactor) {
            return REMOVE;
        }
        if (x < addFactor + removeFactor + renameFactor) {
            return RENAME;
        }
        return CONVERT;
    }

    private void testFuzzReload(int numOfReloads, int addFactor, int removeFactor, int renameFactor) {
        testFuzzReload(numOfReloads, addFactor, removeFactor, renameFactor, 0);
    }

    private void testFuzzReload(int numOfReloads, int convertFactor) {
        testFuzzReload(numOfReloads, 0, 0, 0, convertFactor);
    }

    private void testFuzzReload(int numOfReloads, int addFactor, int removeFactor, int renameFactor, int convertFactor) {
        createTable();
        try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
            try (TableReader reader = newOffPoolReader(configuration, "all")) {
                for (int i = 0; i < numOfReloads; i++) {
                    ingest(writer);
                    reader.reload();
                    for (int j = 0; j < reader.getPartitionCount(); j++) {
                        reader.openPartition(j);
                    }
                    changeTableStructure(addFactor, removeFactor, renameFactor, convertFactor, writer);
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
