/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderMetadataCorruptionTest extends AbstractCairoTest {

    @Test
    public void testColumnNameMissing() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length + 10,
                5,
                "index flag is only supported for SYMBOL column type" //failed validation on garbage flags value
        );
    }

    @Test
    public void testColumnNameMissing2() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length + 1000,
                5,
                "file is too small, column types are missing",
                4096,
                4096
        );
    }

    @Test
    public void testDuplicateColumnName() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                5,
                "Duplicate"
        );
    }

    @Test
    public void testFileIsTooSmall() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length + 10,
                5,
                "File is too small",
                4906,
                1
        );
    }

    @Test
    public void testFileIsTooSmallForColumnNames() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                5,
                "File is too small, size=341, required=342",
                4906,
                341
        );
    }

    @Test
    public void testFileIsTooSmallForColumnNames2() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length - 1,
                -1,
                "String length of 0 is invalid at offset 308",
                4906,
                342
        );
    }

    @Test
    public void testFileIsTooSmallForColumnTypes() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length + 10,
                5,
                "file is too small, column types are missing",
                4906,
                128
        );
    }

    @Test
    public void testIncorrectTimestampIndex1() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.DATE};

        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                23,
                "timestamp index is outside of range"
        );
    }

    @Test
    public void testIncorrectTimestampIndex2() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.DATE};

        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                -2,
                "timestamp index is outside of range"
        );
    }

    @Test
    public void testIncorrectTimestampIndex3() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.DATE};

        int timestampIndex = 2; // this is incorrect because column is of type STRING
        Assert.assertEquals(ColumnType.STRING, types[timestampIndex]);
        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                timestampIndex,
                ColumnType.nameOf(ColumnType.STRING)
        );
    }

    @Test
    public void testInvalidColumnType() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, 566, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                5,
                "invalid column type"
        );
    }

    @Test
    public void testNullColumnName() throws Exception {
        final String[] names = new String[]{"a", "b", "c", null, "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                5,
                "NULL column"
        );
    }

    @Test
    public void testReplacingColumnIndexBeyondColumnCount() throws Exception {
        // this index would send the chain walk past the end of the _meta file
        assertReplacingIndexIgnored(100_000, 2);
    }

    @Test
    public void testReplacingColumnIndexForwardReference() throws Exception {
        // a replacing column always comes after the column it replaces, never before
        assertReplacingIndexIgnored(9, 2);
    }

    @Test
    public void testReplacingColumnIndexIsClearedOnNextDdl() throws Exception {
        // any ALTER rewrites the column block, which drops the junk
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                TableToken tableToken = createAllTableWithReplacingIndex(path, 100_000, 2);

                try (TableWriter writer = TestUtils.newOffPoolWriter(configuration, tableToken, engine)) {
                    writer.addColumn("z", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                }

                Assert.assertEquals(0, readMetaInt(path, replacingIndexOffset(2)));
            }
        });
    }

    @Test
    public void testReplacingColumnIndexOfDeletedColumnIsHonoured() throws Exception {
        // what a real ALTER COLUMN TYPE leaves: "double" (3) replaces "short" (1), which is deleted
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                TableToken tableToken = createAllTableWithReplacingIndex(path, 1, 3);
                // a deleted column has a negative type
                pokeMetaInt(path, TableUtils.META_OFFSET_COLUMN_TYPES + TableUtils.META_COLUMN_DATA_SIZE, -ColumnType.SHORT);

                try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken)) {
                    metadata.loadMetadata();
                    // the replacement takes over the position of the column it replaced
                    Assert.assertEquals(-1, metadata.getColumnIndexQuiet("short"));
                    Assert.assertEquals("double", metadata.getColumnName(1));
                    Assert.assertEquals(3, metadata.getWriterIndex(1));
                    Assert.assertEquals(1, metadata.getOriginalWriterIndex(1));
                }
            }
        });
    }

    @Test
    public void testReplacingColumnIndexPointsAtLiveColumn() throws Exception {
        // in range and below column 3, but column 1 is live, so it is not a replacement
        assertReplacingIndexIgnored(1, 3);
    }

    @Test
    public void testReplacingColumnIndexSelfReference() throws Exception {
        assertReplacingIndexIgnored(3, 3);
    }

    @Test
    public void testTransitionIndexWhenColumnCountIsBeyondFileSize() throws Exception {
        // this test asserts that validator compares column count to file size, where
        // file is prepared to be smaller than count. On Windows this setup does not work
        // because appender cannot truncate file to size smaller than default page size
        // when reader is open.
        if (Os.type != Os.WINDOWS) {
            assertTransitionIndexValidation(99, "index flag is only supported for SYMBOL column type");
        }
    }

    @Test
    public void testTransitionIndexWhenColumnCountOverflows() throws Exception {
        assertTransitionIndexValidation(Integer.MAX_VALUE - 1, "file is too small, column types are missing");
    }

    private void assertMetaConstructorFailure(String[] names, int[] types, int columnCount, int timestampIndex, String contains) throws Exception {
        assertMetaConstructorFailure(names, types, columnCount, timestampIndex, contains, TestFilesFacadeImpl.INSTANCE.getPageSize(), -1);
        assertMetaConstructorFailure(names, types, columnCount, timestampIndex, contains, 65536, -1);
    }

    private void assertMetaConstructorFailure(String[] names, int[] types, int columnCount, int timestampIndex, String contains, long pageSize, long trimSize) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String tableName = "x";
                path.of(root).concat(tableName);
                final int rootLen = path.size();
                if (TestFilesFacadeImpl.INSTANCE.mkdirs(path.slash(), configuration.getMkDirMode()) == -1) {
                    throw CairoException.critical(TestFilesFacadeImpl.INSTANCE.errno()).put("Cannot create dir: ").put(path);
                }

                try (MemoryMA mem = Vm.getPMARInstance(null)) {
                    mem.of(
                            TestFilesFacadeImpl.INSTANCE,
                            path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$(),
                            pageSize,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_NONE
                    );

                    mem.putInt(columnCount);
                    mem.putInt(PartitionBy.NONE);
                    mem.putInt(timestampIndex);
                    mem.putInt(ColumnType.VERSION);
                    mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

                    for (int i = 0; i < names.length; i++) {
                        mem.putInt(types[i]);
                        mem.putLong(0);
                        mem.putInt(0);
                        mem.skip(16);
                    }
                    for (int i = 0; i < names.length; i++) {
                        mem.putStr(names[i]);
                    }
                }

                if (trimSize > -1) {
                    FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
                    path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$();
                    long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    assert fd > -1;
                    ff.truncate(fd, trimSize);
                    ff.close(fd);
                }

                try (TableReaderMetadata metadata = new TableReaderMetadata(configuration)) {
                    metadata.loadMetadata(path.$());
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), contains);
                }
            }
        });
    }

    private void assertReplacingIndexIgnored(int replacingIndex, int columnIndex) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                TableToken tableToken = createAllTableWithReplacingIndex(path, replacingIndex, columnIndex);

                // both metadata paths read it as "no replacement" and leave the columns alone
                try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken)) {
                    metadata.loadMetadata();
                    Assert.assertEquals(columnIndex, metadata.getWriterIndex(columnIndex));
                    for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                        Assert.assertEquals(metadata.getWriterIndex(i), metadata.getOriginalWriterIndex(i));
                    }
                }

                try (TableWriter writer = TestUtils.newOffPoolWriter(configuration, tableToken, engine)) {
                    Assert.assertEquals(columnIndex, writer.getMetadata().getWriterIndex(columnIndex));
                }
            }
        });
    }

    private void assertTransitionIndexValidation(int columnCount, String contains) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {

                CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE, ColumnType.TIMESTAMP_MICRO);

                String tableName = "all";
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(root).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();

                long len = TestFilesFacadeImpl.INSTANCE.length(path.$());

                try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken)) {
                    metadata.loadMetadata();
                    try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                        mem.smallFile(TestFilesFacadeImpl.INSTANCE, path.$(), MemoryTag.MMAP_DEFAULT);
                        mem.jumpTo(0);
                        mem.putInt(columnCount);
                        mem.skip(len - 4);
                    }

                    try {
                        metadata.prepareTransition(0);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), contains);
                    }
                }
            }
        });

    }

    private TableToken createAllTableWithReplacingIndex(Path path, int replacingIndex, int columnIndex) {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE, ColumnType.TIMESTAMP_MICRO);
        TableToken tableToken = engine.verifyTableName("all");
        path.of(root).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
        // the on-disk encoding is 1-based, with 0 meaning "no replacement"
        pokeMetaInt(path, replacingIndexOffset(columnIndex), replacingIndex + 1);
        return tableToken;
    }

    private void pokeMetaInt(Path path, long offset, int value) {
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.smallFile(TestFilesFacadeImpl.INSTANCE, path.$(), MemoryTag.MMAP_DEFAULT);
            mem.putInt(offset, value);
        }
    }

    private int readMetaInt(Path path, long offset) {
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.smallFile(TestFilesFacadeImpl.INSTANCE, path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(offset);
        }
    }

    private long replacingIndexOffset(int columnIndex) {
        return TableUtils.META_OFFSET_COLUMN_TYPES + columnIndex * TableUtils.META_COLUMN_DATA_SIZE + 24;
    }
}
