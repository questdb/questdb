/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class TableReaderReloadFuzzTest extends AbstractCairoTest {
    private static final int ADD = 0;
    private static final Log LOG = LogFactory.getLog(TableReaderReloadFuzzTest.class);
    private static final int REMOVE = 1;
    private static final int RENAME = 2;

    private final AtomicInteger columNameGen = new AtomicInteger(0);
    private final ObjList<Column> columns = new ObjList<>();
    private Rnd random;

    @Before
    public void setUp() {
        super.setUp();
        random = TestUtils.generateRandom(LOG);
    }

    @Test
    public void testAddRemove() {
        testFuzzReload(75, 1, 1, 0);
    }

    @Test
    public void testBalanced() {
        testFuzzReload(75, 1, 1, 1);
    }

    @Test
    public void testMostlyAdd() {
        testFuzzReload(50, 5, 1, 1);
    }

    @Test
    public void testRename() {
        testFuzzReload(100, 0, 0, 1);
    }

    private void copyLiveColumns(TableRecordMetadata metadata, ObjList<Column> columns) {
        columns.clear();
        final int columnCount = metadata.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            final int type = metadata.getColumnType(i);
            if (type > -1) {
                columns.add(new Column(metadata.getColumnName(i), type));
            }
        }
    }

    private int selectColumn(TableRecordMetadata metadata) {
        final int columnCount = metadata.getColumnCount();
        final int timestampIndex = metadata.getTimestampIndex();

        final IntList removableColumns = new IntList();
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
        try (TableModel model = CairoTestUtils.getAllTypesModel(configuration, PartitionBy.DAY)) {
            model.timestamp();
            CairoTestUtils.create(model);
        }

        final int minNumOfStructureChanges = 5;
        final int maxNumOfStructureChanges = 20;
        try (TableWriter writer = new TableWriter(configuration, "all", metrics)) {
            final TableRecordMetadata writerMetadata = writer.getMetadata();
            try (TableReader reader = new TableReader(configuration, "all")) {
                for (int i = 0; i < numOfReloads; i++) {
                    final int numOfStructureChanges = minNumOfStructureChanges + random.nextInt(maxNumOfStructureChanges - minNumOfStructureChanges);
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

                    reader.reload();
                    copyLiveColumns(writerMetadata, columns);
                    final TableReaderMetadata readerMetadata = reader.getMetadata();
                    for (int k = 0; k < readerMetadata.getColumnCount(); k++) {
                        assertEquals(columns.get(k).name, readerMetadata.getColumnName(k));
                        assertEquals(columns.get(k).type, readerMetadata.getColumnType(k));
                    }
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
