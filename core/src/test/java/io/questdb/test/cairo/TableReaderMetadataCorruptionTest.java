/*******************************************************************************
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableMetadataFileBlock;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.file.BlockFileUtils.HEADER_SIZE;

public class TableReaderMetadataCorruptionTest extends AbstractCairoTest {

    @Test
    public void testDuplicateColumnName() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "b", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                5,
                "Duplicate"
        );
    }

    @Test
    public void testFileIsTooSmall() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                5,
                "block file too small",
                1
        );
    }

    @Test
    public void testFileIsTooSmallForHeader() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        assertMetaConstructorFailure(
                names,
                types,
                5,
                "block file too small",
                HEADER_SIZE - 1
        );
    }

    @Test
    public void testFileIsTooSmallForRegion() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        // Truncate just after the header - causes checksum mismatch
        assertMetaConstructorFailure(
                names,
                types,
                5,
                "block file checksum mismatch",
                HEADER_SIZE + 10
        );
    }

    @Test
    public void testFileIsTooSmallForBlocks() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        // Truncate in the middle of the data region
        assertMetaConstructorFailure(
                names,
                types,
                5,
                "block file checksum mismatch",
                HEADER_SIZE + 50
        );
    }

    @Test
    public void testMissingCoreBlock() throws Exception {
        // Write a block file without CORE block
        assertMetaConstructorFailureWithMissingBlock(
                TableMetadataFileBlock.BLOCK_TYPE_CORE,
                "metadata core block not found"
        );
    }

    @Test
    public void testMissingColumnsBlock() throws Exception {
        // Write a block file without COLUMNS block
        assertMetaConstructorFailureWithMissingBlock(
                TableMetadataFileBlock.BLOCK_TYPE_COLUMNS,
                "metadata columns block not found"
        );
    }

    @Test
    public void testNullColumnName() throws Exception {
        // Write a block file with a null column name - causes NPE which is caught as error
        assertMetaConstructorFailureWithNullColumnName();
    }

    @Test
    public void testIncorrectTimestampIndex1() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        // Timestamp index 23 is outside the range of 6 columns
        assertMetaConstructorFailure(
                names,
                types,
                23,
                "timestamp column index is out of range"
        );
    }

    @Test
    public void testIncorrectTimestampIndex2() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};

        // Timestamp index -2 is invalid (only -1 is allowed for "no timestamp")
        assertMetaConstructorFailure(
                names,
                types,
                -2,
                "timestamp column index is out of range"
        );
    }

    @Test
    public void testIncorrectTimestampIndex3() throws Exception {
        final String[] names = new String[]{"a", "b", "c", "d", "e", "f"};
        final int[] types = new int[]{ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.DATE};

        // Timestamp index 2 points to a STRING column, which is incorrect
        int timestampIndex = 2;
        Assert.assertEquals(ColumnType.STRING, types[timestampIndex]);
        assertMetaConstructorFailure(
                names,
                types,
                timestampIndex,
                "TIMESTAMP"
        );
    }

    @Test
    public void testInvalidColumnType() throws Exception {
        // Write a block file with an invalid column type (566)
        assertMetaConstructorFailureWithInvalidColumnType(
                566,
                "invalid column type"
        );
    }

    @Test
    public void testTransitionIndexWhenFileTruncated() throws Exception {
        // this test asserts that validator detects truncated file during transition.
        // On Windows this setup does not work because appender cannot truncate file
        // to size smaller than default page size when reader is open.
        if (Os.type != Os.WINDOWS) {
            assertTransitionIndexValidation(HEADER_SIZE - 1, "block file too small");
        }
    }


    private void assertMetaConstructorFailure(String[] names, int[] types, int timestampIndex, String contains) throws Exception {
        assertMetaConstructorFailure(names, types, timestampIndex, contains, -1);
    }

    private void assertMetaConstructorFailure(String[] names, int[] types, int timestampIndex, String contains, long trimSize) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String tableName = "x";
                path.of(root).concat(tableName);
                final int rootLen = path.size();
                if (TestFilesFacadeImpl.INSTANCE.mkdirs(path.slash(), configuration.getMkDirMode()) == -1) {
                    throw CairoException.critical(TestFilesFacadeImpl.INSTANCE.errno()).put("Cannot create dir: ").put(path);
                }

                path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$();

                // Create columns from names and types
                ObjList<TableColumnMetadata> columns = new ObjList<>();
                for (int i = 0; i < names.length; i++) {
                    columns.add(createColumn(names[i], types[i], i));
                }

                // Write metadata using BlockFileWriter
                try (BlockFileWriter writer = new BlockFileWriter(TestFilesFacadeImpl.INSTANCE, CommitMode.SYNC)) {
                    writer.of(path.$());
                    TableMetadataFileBlock.write(
                            writer,
                            ColumnType.VERSION,
                            1, // tableId
                            PartitionBy.NONE,
                            timestampIndex,
                            false, // walEnabled
                            1000, // maxUncommittedRows
                            0L, // o3MaxLag
                            0, // ttlHoursOrMonths
                            columns
                    );
                }

                if (trimSize > -1) {
                    FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
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

    private void assertMetaConstructorFailureWithMissingBlock(int blockTypeToSkip, String contains) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String tableName = "x";
                path.of(root).concat(tableName);
                final int rootLen = path.size();
                if (TestFilesFacadeImpl.INSTANCE.mkdirs(path.slash(), configuration.getMkDirMode()) == -1) {
                    throw CairoException.critical(TestFilesFacadeImpl.INSTANCE.errno()).put("Cannot create dir: ").put(path);
                }

                path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$();

                // Write raw blocks, skipping the specified block type
                try (BlockFileWriter writer = new BlockFileWriter(TestFilesFacadeImpl.INSTANCE, CommitMode.SYNC)) {
                    writer.of(path.$());

                    // Write CORE block (if not skipping)
                    if (blockTypeToSkip != TableMetadataFileBlock.BLOCK_TYPE_CORE) {
                        AppendableBlock block = writer.append();
                        block.putInt(ColumnType.VERSION); // formatVersion
                        block.putInt(1); // tableId
                        block.putInt(PartitionBy.NONE); // partitionBy
                        block.putInt(5); // timestampIndex
                        block.putInt(6); // columnCount
                        block.putLong(0); // placeholder
                        block.putBool(false); // walEnabled
                        block.commit(TableMetadataFileBlock.BLOCK_TYPE_CORE);
                    }

                    // Write COLUMNS block (if not skipping)
                    if (blockTypeToSkip != TableMetadataFileBlock.BLOCK_TYPE_COLUMNS) {
                        AppendableBlock block = writer.append();
                        block.putInt(6); // columnCount
                        String[] names = {"a", "b", "c", "d", "e", "f"};
                        for (int i = 0; i < 6; i++) {
                            block.putInt(ColumnType.INT); // type
                            block.putInt(0); // flags
                            block.putInt(0); // indexValueBlockCapacity
                            block.putInt(0); // symbolCapacity
                            block.putInt(i); // writerIndex
                            block.putInt(-1); // replacingIndex
                            block.putStr(names[i]); // name
                        }
                        block.commit(TableMetadataFileBlock.BLOCK_TYPE_COLUMNS);
                    }

                    // Write SETTINGS block
                    AppendableBlock block = writer.append();
                    block.putInt(1000); // maxUncommittedRows
                    block.putLong(0L); // o3MaxLag
                    block.putInt(0); // ttlHoursOrMonths
                    block.commit(TableMetadataFileBlock.BLOCK_TYPE_SETTINGS);

                    writer.commit();
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

    private void assertMetaConstructorFailureWithInvalidColumnType(int invalidType, String contains) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String tableName = "x";
                path.of(root).concat(tableName);
                final int rootLen = path.size();
                if (TestFilesFacadeImpl.INSTANCE.mkdirs(path.slash(), configuration.getMkDirMode()) == -1) {
                    throw CairoException.critical(TestFilesFacadeImpl.INSTANCE.errno()).put("Cannot create dir: ").put(path);
                }

                path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$();

                // Write raw blocks with invalid column type
                try (BlockFileWriter writer = new BlockFileWriter(TestFilesFacadeImpl.INSTANCE, CommitMode.SYNC)) {
                    writer.of(path.$());

                    // Write CORE block
                    AppendableBlock block = writer.append();
                    block.putInt(ColumnType.VERSION); // formatVersion
                    block.putInt(1); // tableId
                    block.putInt(PartitionBy.NONE); // partitionBy
                    block.putInt(5); // timestampIndex
                    block.putInt(6); // columnCount
                    block.putLong(0); // placeholder
                    block.putBool(false); // walEnabled
                    block.commit(TableMetadataFileBlock.BLOCK_TYPE_CORE);

                    // Write COLUMNS block with invalid column type
                    block = writer.append();
                    block.putInt(6); // columnCount
                    int[] types = {ColumnType.INT, ColumnType.INT, invalidType, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};
                    String[] names = {"a", "b", "c", "d", "e", "f"};
                    for (int i = 0; i < 6; i++) {
                        block.putInt(types[i]); // type
                        block.putInt(0); // flags
                        block.putInt(0); // indexValueBlockCapacity
                        block.putInt(0); // symbolCapacity
                        block.putInt(i); // writerIndex
                        block.putInt(-1); // replacingIndex
                        block.putStr(names[i]); // name
                    }
                    block.commit(TableMetadataFileBlock.BLOCK_TYPE_COLUMNS);

                    // Write SETTINGS block
                    block = writer.append();
                    block.putInt(1000); // maxUncommittedRows
                    block.putLong(0L); // o3MaxLag
                    block.putInt(0); // ttlHoursOrMonths
                    block.commit(TableMetadataFileBlock.BLOCK_TYPE_SETTINGS);

                    writer.commit();
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

    private void assertMetaConstructorFailureWithNullColumnName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                String tableName = "x";
                path.of(root).concat(tableName);
                final int rootLen = path.size();
                if (TestFilesFacadeImpl.INSTANCE.mkdirs(path.slash(), configuration.getMkDirMode()) == -1) {
                    throw CairoException.critical(TestFilesFacadeImpl.INSTANCE.errno()).put("Cannot create dir: ").put(path);
                }

                path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$();

                // Write raw blocks with null column name
                try (BlockFileWriter writer = new BlockFileWriter(TestFilesFacadeImpl.INSTANCE, CommitMode.SYNC)) {
                    writer.of(path.$());

                    // Write CORE block
                    AppendableBlock block = writer.append();
                    block.putInt(ColumnType.VERSION); // formatVersion
                    block.putInt(1); // tableId
                    block.putInt(PartitionBy.NONE); // partitionBy
                    block.putInt(5); // timestampIndex
                    block.putInt(6); // columnCount
                    block.putLong(0); // placeholder
                    block.putBool(false); // walEnabled
                    block.commit(TableMetadataFileBlock.BLOCK_TYPE_CORE);

                    // Write COLUMNS block with null column name
                    block = writer.append();
                    block.putInt(6); // columnCount
                    int[] types = {ColumnType.INT, ColumnType.INT, ColumnType.STRING, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP};
                    String[] names = {"a", "b", "c", null, "e", "f"};
                    for (int i = 0; i < 6; i++) {
                        block.putInt(types[i]); // type
                        block.putInt(0); // flags
                        block.putInt(0); // indexValueBlockCapacity
                        block.putInt(0); // symbolCapacity
                        block.putInt(i); // writerIndex
                        block.putInt(-1); // replacingIndex
                        block.putStr(names[i]); // name - will write null
                    }
                    block.commit(TableMetadataFileBlock.BLOCK_TYPE_COLUMNS);

                    // Write SETTINGS block
                    block = writer.append();
                    block.putInt(1000); // maxUncommittedRows
                    block.putLong(0L); // o3MaxLag
                    block.putInt(0); // ttlHoursOrMonths
                    block.commit(TableMetadataFileBlock.BLOCK_TYPE_SETTINGS);

                    writer.commit();
                }

                try (TableReaderMetadata metadata = new TableReaderMetadata(configuration)) {
                    metadata.loadMetadata(path.$());
                    Assert.fail("Expected exception for null column name");
                } catch (NullPointerException e) {
                    // Expected - null column name causes NPE in hash map
                } catch (CairoException e) {
                    // Also acceptable if validation was added
                    TestUtils.assertContains(e.getFlyweightMessage(), "NULL");
                }
            }
        });
    }

    private void assertTransitionIndexValidation(long trimSize, String contains) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE, ColumnType.TIMESTAMP_MICRO);

                String tableName = "all";
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(root).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();

                try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken)) {
                    metadata.loadMetadata();

                    // Truncate the file to corrupt it
                    FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
                    long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    assert fd > -1;
                    ff.truncate(fd, trimSize);
                    ff.close(fd);

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

    private TableColumnMetadata createColumn(String name, int type, int writerIndex) {
        return new TableColumnMetadata(
                name,
                type,
                false, // indexFlag
                0, // indexValueBlockCapacity
                false, // symbolTableStatic
                null, // metadata
                writerIndex,
                false, // dedupKeyFlag
                -1, // replacingIndex (-1 means no replacement)
                true, // symbolCacheFlag
                0 // symbolCapacity
        );
    }
}
