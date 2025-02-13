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

package io.questdb.test.cairo.meta;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileUtils;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.file.WritableBlock;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.vm.Vm.STRING_LENGTH_BYTES;

@RunWith(Parameterized.class)
public class BlockFileTest extends AbstractCairoTest {
    protected final static Log LOG = LogFactory.getLog(BlockFileTest.class);
    private static final short MSG_TYPE_A = 1;
    private static final short MSG_TYPE_ALL = 4;
    private static final byte MSG_TYPE_ALL_VERSION_1 = 1;
    private static final byte MSG_TYPE_A_VERSION_1 = 1;
    private static final byte MSG_TYPE_A_VERSION_2 = 2;
    private static final short MSG_TYPE_B = 2;
    private static final byte MSG_TYPE_B_VERSION_1 = 1;
    private static final short MSG_TYPE_C = 3;
    private static final byte MSG_TYPE_C_VERSION_1 = 1;
    protected static int commitMode = CommitMode.NOSYNC;

    public BlockFileTest(int commitMode) {
        BlockFileTest.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "mode={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {CommitMode.NOSYNC},
                {CommitMode.SYNC},
                {CommitMode.ASYNC}
        });
    }

    @Test
    public void testCreateEmptyDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.touch(path.$()));

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    Assert.assertEquals(ff.getPageSize(), ff.length(path.$()));
                    Assert.assertEquals(0, writer.getVersionVolatile());
                    Assert.assertEquals(0, writer.getRegionOffset(0));
                    Assert.assertEquals(0, writer.getRegionOffset(1));
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("Empty definition file"));
                }
            }
        });
    }

    @Test
    public void testReadEmptyDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.touch(path.$()));

                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.$());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("cannot read meta file, expected at least 40 bytes"));

                }
            }
        });
    }

    @Test
    public void testReadEmptyDefinitionFile2() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();

                // size > HEADER_SIZE
                // version = 0
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.of(ff, path.$(), ff.getPageSize(), ff.getPageSize(), 0);
                    mem.putInt(0, 0);
                }

                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.$());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("cannot read meta file, expected at least 1 commited data block"));
                }
            }
        });
    }

    @Test
    public void testReadNonExistingDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.$());
                    Assert.fail("Expected exception");
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("[2] cannot open meta file"));
                }
            }
        });
    }

    @Test
    public void testReadWriteAppendNewRegion() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                long prevRegionOffset;
                long prevRegionLength;
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgAVersion2(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                }

                readAllBlocks(path, 1);
            }
        });
    }

    @Test
    public void testReadWriteAppendNewRegionNoSpace() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                long prevRegionOffset;
                long prevRegionLength;
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgAVersion2(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                // message C will not fit into the same region
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgCVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 3, regionLength, prevRegionOffset + prevRegionLength);
                }

                readAllBlocks(path, 1);
            }
        });
    }

    @Test
    public void testReadWriteChecksum() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    AppendableBlock memory1 = writer.append();
                    commitMsgAVersion1(memory1);
                    writer.commit();
                }
                readAllBlocks(path, 1);

                // corrupt data
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                    mem.putInt(BlockFileUtils.HEADER_SIZE + BlockFileUtils.REGION_HEADER_SIZE + 8, -42);
                }

                try {
                    readAllBlocks(path, 1);
                    Assert.fail("checksum mismatch expected");
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("meta file checksum mismatch"));
                }
            }
        });
    }

    @Test
    public void testReadWriteConcurrently() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = configuration.getFilesFacade();
            Rnd rnd = TestUtils.generateRandom(LOG);
            int readerThreads = 4;
            CyclicBarrier start = new CyclicBarrier(readerThreads + 1);
            AtomicInteger done = new AtomicInteger();
            int iterations = 1000;
            Thread writerThread = new Thread(() -> {
                try (Path path = getDefinitionFilePath("test")) {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        int msg = rnd.nextInt(4);
                        try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                            writer.of(path.$());
                            switch (msg) {
                                case 0:
                                    commitMsgAVersion1(writer.append());
                                    break;
                                case 1:
                                    commitMsgAVersion2(writer.append());
                                    break;
                                case 2:
                                    commitMsgBVersion1(writer.append());
                                    break;
                                case 3:
                                    commitMsgCVersion1(writer.append());
                                    break;
                            }
                            writer.commit();
                        }
                    }
                } catch (Exception e) {
                    LOG.error().$("Error in writer thread: ").$(e).$();
                } finally {
                    done.incrementAndGet();
                }
            });

            Thread[] readers = new Thread[readerThreads];
            for (int th = 0; th < readerThreads; th++) {
                Thread readerThread = new Thread(() -> {
                    try (Path path = getDefinitionFilePath("test")) {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            Os.sleep(1); // interleave reads and writes
                            readAllBlocks(path, 1);
                        }
                    } catch (Exception e) {
                        LOG.error().$("Error in reader thread: ").$(e).$();
                    }
                });
                readers[th] = readerThread;
                readerThread.start();
            }

            writerThread.start();
            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }
        });
    }

    @Test
    public void testReadWriteOverwriteNewRegion() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                long prevRegionOffset;
                long prevRegionLength;
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgAVersion2(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                }

                // this version will overwrite the previous region
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgBVersion1(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 3, regionLength, 0);
                }

                readAllBlocks(path, 1);
            }
        });
    }

    @Test
    public void testReadWriteSimple() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());

                    AppendableBlock memory1 = writer.append();
                    commitMsgAVersion1(memory1);
                    final int commitedLength = memory1.length();

                    WritableBlock memory2 = writer.reserve(commitedLength);
                    Assert.assertEquals(commitedLength, memory2.length());
                    commitMsgAVersion1RW(memory2);

                    AppendableBlock memory3 = writer.append();
                    commitMsgAVersion2(memory3);

                    writer.commit();
                }

                readAllBlocks(path, 3);
            }
        });
    }

    @Test
    public void testReadWriteSimpleAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());

                    AppendableBlock memory1 = writer.append();
                    commitAllTypesMsgAppendAPI(memory1);
                    final int commitedLength = memory1.length();

                    WritableBlock memory2 = writer.reserve(commitedLength);
                    Assert.assertEquals(commitedLength, memory2.length());
                    commitAllTypesMsgWriteAPI(memory2);
                    Assert.assertEquals(commitedLength, memory2.length());

                    AppendableBlock memory3 = writer.append();
                    commitAllTypesMsgAppendAPI(memory3);

                    Assert.assertTrue(writer.commit());
                    Assert.assertFalse(writer.commit());
                }

                readAllBlocks(path, 3);
            }
        });
    }

    private static void assertRegionOffset(BlockFileWriter writer, long expectedVersion, int expectedRegionLength, long expectedRegionOffset) {
        final long version = writer.getVersionVolatile();
        Assert.assertEquals(expectedVersion, version);
        final long regionOffset = writer.getRegionOffset(version);
        final long regionLength = writer.getRegionLength(version);
        Assert.assertEquals(expectedRegionOffset, regionOffset);
        Assert.assertEquals(expectedRegionLength, regionLength);
    }

    private static int commitAllTypesMsgAppendAPI(AppendableBlock memory) {

        BinarySequence binarySequence = new BinarySequence() {
            @Override
            public byte byteAt(long index) {
                return index % 2 == 0 ? (byte) 0 : (byte) 1;
            }

            @Override
            public long length() {
                return 42;
            }
        };

        memory.skip(42);
        memory.putBin(binarySequence);
        memory.putBool(true);
        memory.putByte((byte) 123);
        memory.putChar('A');
        memory.putDouble(123.456);
        memory.putFloat(78.9f);
        memory.putInt(123456);
        memory.putLong(123456789L);
        memory.putShort((short) 12345);
        memory.putStr("Hello, World!");
        memory.putVarchar(new GcUtf8String("Hello, UTF-8 World!"));
        Assert.assertTrue(memory.commit(MSG_TYPE_ALL, MSG_TYPE_ALL_VERSION_1, (byte) 0));
        Assert.assertFalse(memory.commit(MSG_TYPE_ALL, MSG_TYPE_ALL_VERSION_1, (byte) 0));
        return memory.length();
    }

    private static int commitAllTypesMsgWriteAPI(WritableBlock memory) {
        BinarySequence binarySequence = new BinarySequence() {
            @Override
            public byte byteAt(long index) {
                return index % 2 == 0 ? (byte) 0 : (byte) 1;
            }

            @Override
            public long length() {
                return 42;
            }
        };

        long offset = 42; // skip 42 bytes
        memory.putBin(offset, binarySequence);
        offset += 8 + 42;
        memory.putBool(offset, true);
        offset += 1;
        memory.putByte(offset, (byte) 123);
        offset += 1;
        memory.putChar(offset, 'A');
        offset += 2;
        memory.putDouble(offset, 123.456);
        offset += 8;
        memory.putFloat(offset, 78.9f);
        offset += 4;
        memory.putInt(offset, 123456);
        offset += 4;
        memory.putLong(offset, 123456789L);
        offset += 8;
        memory.putShort(offset, (short) 12345);
        offset += 2;
        memory.putStr(offset, "Hello, World!");
        offset += Vm.getStorageLength("Hello, World!");
        memory.putVarchar(offset, new GcUtf8String("Hello, UTF-8 World!"));

        Assert.assertTrue(memory.commit(MSG_TYPE_ALL, MSG_TYPE_ALL_VERSION_1, (byte) 0));
        Assert.assertFalse(memory.commit(MSG_TYPE_ALL, MSG_TYPE_ALL_VERSION_1, (byte) 0));
        return memory.length();
    }

    private static int commitMsgAVersion1(AppendableBlock memory) {
        memory.putStr("Hello");
        memory.putInt(123);
        memory.putVarchar(new GcUtf8String("World"));
        memory.putInt(456);
        memory.commit(MSG_TYPE_A, MSG_TYPE_A_VERSION_1, (byte) 0);
        return memory.length();
    }

    private static void commitMsgAVersion1RW(WritableBlock memory) {
        String hello = "Hello";
        Utf8Sequence worldUtf8 = new GcUtf8String("World");
        int offset = 0;
        memory.putStr(offset, hello);
        offset += Vm.getStorageLength(hello);
        memory.putInt(offset, 123);
        offset += Integer.BYTES;
        memory.putVarchar(offset, worldUtf8);
        offset += STRING_LENGTH_BYTES + worldUtf8.size();
        memory.putInt(offset, 456);
        memory.commit(MSG_TYPE_A, MSG_TYPE_A_VERSION_1, (byte) 0);
    }

    private static int commitMsgAVersion2(AppendableBlock memory) {
        memory.putInt(123);
        memory.putStr("Hello");
        memory.putInt(456);
        memory.putStr("World");
        memory.commit(MSG_TYPE_A, MSG_TYPE_A_VERSION_2, (byte) 0);
        return memory.length();
    }

    private static int commitMsgBVersion1(AppendableBlock memory) {
        memory.putStr("Hello");
        memory.putStr("World");
        memory.commit(MSG_TYPE_B, MSG_TYPE_B_VERSION_1, (byte) 0);
        return memory.length();
    }

    private static int commitMsgCVersion1(AppendableBlock memory) {
        memory.putStr("Hello");
        final int count = 10;
        for (int i = 0; i < count; i++) {
            memory.putInt(i);
            memory.putStr("World");
        }
        memory.commit(MSG_TYPE_C, MSG_TYPE_C_VERSION_1, (byte) 0);
        return memory.length();
    }

    private static Path getDefinitionFilePath(final String tableName) {
        Path path = new Path().of(configuration.getDbRoot()).concat(tableName).slash();
        FilesFacade ff = configuration.getFilesFacade();
        ff.mkdirs(path, configuration.getMkDirMode());
        return path.of(configuration.getDbRoot()).concat(tableName).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
    }

    private static void readAllBlocks(Path path, int expectedBlocks) {
        readAllBlocks(path, configuration, expectedBlocks);
    }

    private static void readAllBlocks(Path path, CairoConfiguration configuration, int expectedBlocks) {
        try (BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(path.$());
            int blockCount = 0;
            BlockFileReader.BlockCursor cursor = reader.getCursor();
            while (cursor.hasNext()) {
                ReadableBlock block = cursor.next();
                final short type = block.type();
                final byte version = block.version();
                final byte flags = block.flags();
                final long msgLen = block.length() - BlockFileUtils.BLOCK_HEADER_SIZE;
                Assert.assertEquals(0, flags);
                switch (type) {
                    case MSG_TYPE_A:
                        switch (version) {
                            case MSG_TYPE_A_VERSION_1:
                                readMsgAVersion1(block);
                                break;
                            case MSG_TYPE_A_VERSION_2:
                                readMsgAVersion2(block);
                                break;
                            default:
                                Assert.fail("Unexpected version");
                        }
                        break;
                    case MSG_TYPE_B:
                        if (version == MSG_TYPE_B_VERSION_1) {
                            readMsgBVersion1(block);
                        } else {
                            Assert.fail("Unexpected version");
                        }
                        break;
                    case MSG_TYPE_C:
                        if (version == MSG_TYPE_C_VERSION_1) {
                            readMsgCVersion1(block);
                        } else {
                            Assert.fail("Unexpected version");
                        }
                        break;
                    case MSG_TYPE_ALL:
                        if (version == MSG_TYPE_ALL_VERSION_1) {
                            long len = readAllTypesMsg(block);
                            Assert.assertEquals(msgLen, len);
                        } else {
                            Assert.fail("Unexpected version");
                        }
                        break;
                    default:
                        Assert.fail("Unexpected type");
                }
                blockCount++;
            }
            Assert.assertEquals(expectedBlocks, blockCount);
        }
    }

    private static long readAllTypesMsg(ReadableBlock memory) {
        long offset = 42;
        BinarySequence readBinarySequence = memory.getBin(offset);
        Assert.assertNotNull(readBinarySequence);
        Assert.assertEquals(42, readBinarySequence.length());
        for (int i = 0; i < 42; i++) {
            Assert.assertEquals(i % 2 == 0 ? (byte) 0 : (byte) 1, readBinarySequence.byteAt(i));
        }
        offset += 8 + 42;

        Assert.assertTrue(memory.getBool(offset));
        offset += 1;

        Assert.assertEquals((byte) 123, memory.getByte(offset));
        offset += 1;

        Assert.assertEquals('A', memory.getChar(offset));
        offset += 2;

        Assert.assertEquals(123.456, memory.getDouble(offset), 0.0001);
        offset += 8;

        Assert.assertEquals(78.9f, memory.getFloat(offset), 0.0001);
        offset += 4;

        Assert.assertEquals(123456, memory.getInt(offset));
        offset += 4;

        Assert.assertEquals(123456789L, memory.getLong(offset));
        offset += 8;

        Assert.assertEquals((short) 12345, memory.getShort(offset));
        offset += 2;

        Assert.assertEquals("Hello, World!", memory.getStr(offset).toString());
        offset += Vm.getStorageLength("Hello, World!");

        Utf8Sequence readUtf8Sequence = memory.getVarchar(offset);
        Assert.assertEquals("Hello, UTF-8 World!", readUtf8Sequence.toString());
        return offset + STRING_LENGTH_BYTES + readUtf8Sequence.size();
    }

    private static void readMsgAVersion1(ReadableBlock memory) {
        Assert.assertEquals(MSG_TYPE_A, memory.type());
        Assert.assertEquals(MSG_TYPE_A_VERSION_1, memory.version());
        Assert.assertEquals(0, memory.flags());
        long offset = 0;
        CharSequence str = memory.getStr(offset);
        Assert.assertEquals("Hello", str.toString());
        offset += Vm.getStorageLength(str);
        Assert.assertEquals(123, memory.getInt(offset));
        offset += Integer.BYTES;
        Utf8Sequence var = memory.getVarchar(offset);
        Assert.assertEquals("World", var.toString());
        offset += (STRING_LENGTH_BYTES + var.size());
        Assert.assertEquals(456, memory.getInt(offset));
    }

    private static void readMsgAVersion2(ReadableBlock memory) {
        Assert.assertEquals(MSG_TYPE_A, memory.type());
        Assert.assertEquals(MSG_TYPE_A_VERSION_2, memory.version());
        Assert.assertEquals(0, memory.flags());
        long offset = 0;
        Assert.assertEquals(123, memory.getInt(offset));
        offset += Integer.BYTES;
        CharSequence str = memory.getStr(offset);
        Assert.assertEquals("Hello", str.toString());
        offset += Vm.getStorageLength(str);
        Assert.assertEquals(456, memory.getInt(offset));
        offset += Integer.BYTES;
        str = memory.getStr(offset);
        Assert.assertEquals("World", str.toString());
    }

    private static void readMsgBVersion1(ReadableBlock memory) {
        Assert.assertEquals(MSG_TYPE_B, memory.type());
        Assert.assertEquals(MSG_TYPE_B_VERSION_1, memory.version());
        Assert.assertEquals(0, memory.flags());
        long offset = 0;
        CharSequence str = memory.getStr(offset);
        Assert.assertEquals("Hello", str.toString());
        offset += Vm.getStorageLength(str);
        str = memory.getStr(offset);
        Assert.assertEquals("World", str.toString());
    }

    private static void readMsgCVersion1(ReadableBlock memory) {
        Assert.assertEquals(MSG_TYPE_C, memory.type());
        Assert.assertEquals(MSG_TYPE_C_VERSION_1, memory.version());
        Assert.assertEquals(0, memory.flags());
        long offset = 0;
        CharSequence str = memory.getStr(offset);
        Assert.assertEquals("Hello", str.toString());
        offset += Vm.getStorageLength(str);
        final int count = 10;
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(i, memory.getInt(offset));
            offset += Integer.BYTES;
            str = memory.getStr(offset);
            Assert.assertEquals("World", str.toString());
            offset += Vm.getStorageLength(str);
        }
    }
}
