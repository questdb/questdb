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

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TableReaderMetadataTest extends AbstractCairoTest {

    @Before
    public void setUp2() {
        CairoTestUtils.createAllTable(configuration, PartitionBy.DAY);
    }

    @Test
    public void testAddColumn() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "xyz:STRING\n";
        assertThat(expected, (w) -> w.addColumn("xyz", ColumnType.STRING), 12);
    }

    @Test
    public void testColumnIndex() {
        ObjIntHashMap<String> expected = new ObjIntHashMap<>();
        expected.put("int", 0);
        expected.put("byte", 2);
        expected.put("bin", 9);
        expected.put("short", 1);
        expected.put("float", 4);
        expected.put("long", 5);
        expected.put("xyz", -1);
        expected.put("str", 6);
        expected.put("double", 3);
        expected.put("sym", 7);
        expected.put("bool", 8);

        expected.put("zall.sym", -1);

        try (Path path = new Path().of(root).concat("all").concat(TableUtils.META_FILE_NAME).$();
             TableReaderMetadata metadata = new TableReaderMetadata(FilesFacadeImpl.INSTANCE, path)) {
            for (ObjIntHashMap.Entry<String> e : expected) {
                Assert.assertEquals(e.value, metadata.getColumnIndexQuiet(e.key));
            }
        }
    }

    @Test
    public void testDeleteTwoAddOneColumn() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "xyz:STRING\n";
        assertThat(expected, (w) -> {
            w.removeColumn("double");
            w.removeColumn("str");
            w.addColumn("xyz", ColumnType.STRING);

        }, 10);
    }

    @Test
    public void testFreeNullAddressAsIndex() {
        TableReaderMetadata.freeTransitionIndex(0);
    }

    @Test
    public void testRemoveAllColumns() throws Exception {
        final String expected = "";
        assertThat(expected, (w) -> {
            w.removeColumn("int");
            w.removeColumn("short");
            w.removeColumn("byte");
            w.removeColumn("float");
            w.removeColumn("long");
            w.removeColumn("str");
            w.removeColumn("sym");
            w.removeColumn("bool");
            w.removeColumn("bin");
            w.removeColumn("date");
            w.removeColumn("double");
        }, 0);
    }

    @Test
    public void testRemoveAndAddSameColumn() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "str:STRING\n";
        assertThat(expected, (w) -> {
            w.removeColumn("str");
            w.addColumn("str", ColumnType.STRING);
        }, 11);
    }

    @Test
    public void testRemoveColumnAndReAdd() throws Exception {
        final String expected = "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n" +
                "str:STRING\n" +
                "short:INT\n";

        assertThat(expected, (w) -> {
            w.removeColumn("short");
            w.removeColumn("str");
            w.removeColumn("int");
            w.addColumn("str", ColumnType.STRING);
            // change column type
            w.addColumn("short", ColumnType.INT);
        }, 10);
    }

    @Test
    public void testRemoveDenseColumns() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n";
        assertThat(expected, (w) -> {
            w.removeColumn("double");
            w.removeColumn("float");
        }, 9);
    }

    @Test
    public void testRemoveFirstAndLastColumns() throws Exception {
        final String expected = "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n";
        assertThat(expected, (w) -> {
            w.removeColumn("date");
            w.removeColumn("int");
        }, 9);
    }

    @Test
    public void testRemoveFirstColumn() throws Exception {
        final String expected =
                "short:SHORT\n" +
                        "byte:BYTE\n" +
                        "double:DOUBLE\n" +
                        "float:FLOAT\n" +
                        "long:LONG\n" +
                        "str:STRING\n" +
                        "sym:SYMBOL\n" +
                        "bool:BOOLEAN\n" +
                        "bin:BINARY\n" +
                        "date:DATE\n";
        assertThat(expected, (w) -> w.removeColumn("int"), 10);
    }

    @Test
    public void testRemoveLastColumn() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "double:DOUBLE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "str:STRING\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n";
        assertThat(expected, (w) -> w.removeColumn("date"), 10);
    }

    @Test
    public void testRemoveSparseColumns() throws Exception {
        final String expected = "int:INT\n" +
                "short:SHORT\n" +
                "byte:BYTE\n" +
                "float:FLOAT\n" +
                "long:LONG\n" +
                "sym:SYMBOL\n" +
                "bool:BOOLEAN\n" +
                "bin:BINARY\n" +
                "date:DATE\n";

        assertThat(expected, (w) -> {
            w.removeColumn("double");
            w.removeColumn("str");
        }, 9);
    }

    private void assertThat(String expected, ColumnManipulator manipulator, int columnCount) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(root).concat("all")) {
                try (TableReaderMetadata metadata = new TableReaderMetadata(FilesFacadeImpl.INSTANCE, path.concat(TableUtils.META_FILE_NAME).$())) {

                    try (TableWriter writer = new TableWriter(configuration, "all")) {
                        manipulator.restructure(writer);
                    }

                    long pTransitionIndex = metadata.createTransitionIndex();
                    StringSink sink = new StringSink();
                    try {
                        metadata.applyTransitionIndex(pTransitionIndex);
                        Assert.assertEquals(columnCount, metadata.getColumnCount());
                        for (int i = 0; i < columnCount; i++) {
                            sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i))).put('\n');
                        }

                        TestUtils.assertEquals(expected, sink);

                        if (expected.length() > 0) {
                            String[] lines = expected.split("\n");
                            Assert.assertEquals(columnCount, lines.length);

                            for (int i = 0; i < columnCount; i++) {
                                int p = lines[i].indexOf(':');
                                Assert.assertEquals(i, metadata.getColumnIndexQuiet(lines[i].substring(0, p)));
                            }
                        }
                    } finally {
                        TableReaderMetadata.freeTransitionIndex(pTransitionIndex);
                    }
                }
            }
        });
    }

    @FunctionalInterface
    public interface ColumnManipulator {
        void restructure(TableWriter writer);
    }
}