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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderMetadataCorruptionTest extends AbstractCairoTest {

    @Test
    public void testColumnCountIsBeyondFileSize() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                names.length + 1,
                5,
                "Index flag is only supported for SYMBOL" //failed validation on garbage flags value
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
    public void testIncorrectTimestampIndex1() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.DATE};

        assertMetaConstructorFailure(
                names,
                types,
                names.length,
                23,
                "Timestamp"
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
                "Timestamp"
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
                "STRING"
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
                "Invalid column type"
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
    public void testTransitionIndexWhenColumnCountIsBeyondFileSize() throws Exception {
        // this test asserts that validator compares column count to file size, where
        // file is prepared to be smaller than count. On windows this setup does not work
        // because appender cannot truncate file to size smaller than default page size
        // when reader is open.
        if (Os.type != Os.WINDOWS) {
            assertTransitionIndexValidation(99);
        }
    }

    @Test
    public void testTransitionIndexWhenColumnCountOverflows() throws Exception {
        assertTransitionIndexValidation(Integer.MAX_VALUE - 1);
    }

    private void assertMetaConstructorFailure(String[] names, int[] types, int columnCount, int timestampIndex, String contains) throws Exception {
        assertMetaConstructorFailure(names, types, columnCount, timestampIndex, contains, FilesFacadeImpl.INSTANCE.getPageSize());
        assertMetaConstructorFailure(names, types, columnCount, timestampIndex, contains, 65536);
    }

    private void assertMetaConstructorFailure(String[] names, int[] types, int columnCount, int timestampIndex, String contains, long pageSize) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(root).concat("x");
                final int rootLen = path.length();
                if (FilesFacadeImpl.INSTANCE.mkdirs(path.slash$(), configuration.getMkDirMode()) == -1) {
                    throw CairoException.instance(FilesFacadeImpl.INSTANCE.errno()).put("Cannot create dir: ").put(path);
                }

                try (MemoryMA mem = Vm.getMAInstance()) {

                    mem.of(FilesFacadeImpl.INSTANCE, path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$(), pageSize, MemoryTag.MMAP_DEFAULT);

                    mem.putInt(columnCount);
                    mem.putInt(PartitionBy.NONE);
                    mem.putInt(timestampIndex);
                    mem.putInt(ColumnType.VERSION);
                    mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

                    for (int i = 0; i < names.length; i++) {
                        mem.putInt(types[i]);
                        mem.putLong(0);
                        mem.putInt(0);
                    }
                    for (int i = 0; i < names.length; i++) {
                        mem.putStr(names[i]);
                    }
                }

                try {
                    new TableReaderMetadata(FilesFacadeImpl.INSTANCE, path);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), contains);
                }
            }
        });
    }

    private void assertTransitionIndexValidation(int columnCount) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {

                CairoTestUtils.createAllTable(configuration, PartitionBy.NONE);

                path.of(root).concat("all").concat(TableUtils.META_FILE_NAME).$();

                long len = FilesFacadeImpl.INSTANCE.length(path);

                try (TableReaderMetadata metadata = new TableReaderMetadata(FilesFacadeImpl.INSTANCE, path)) {
                    try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                        mem.smallFile(FilesFacadeImpl.INSTANCE, path, MemoryTag.MMAP_DEFAULT);
                        mem.jumpTo(0);
                        mem.putInt(columnCount);
                        mem.skip(len - 4);
                    }

                    try {
                        metadata.createTransitionIndex();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid metadata at ");
                    }
                }
            }
        });

    }
}
