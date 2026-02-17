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

package io.questdb.test.cairo.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
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
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
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
    private static final byte MSG_TYPE_A1 = 1;
    private static final byte MSG_TYPE_A2 = 2;
    private static final short MSG_TYPE_ALL = 5;
    private static final short MSG_TYPE_B = 3;
    private static final short MSG_TYPE_C = 4;
    private static final short MSG_TYPE_NLONGS = 6;
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

    @Before
    public void setUp() {
        super.setUp();
        FilesFacade ff = configuration.getFilesFacade();
        ff.remove(getDefinitionFilePath().$());
    }

    @Test
    public void testCreateEmptyDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.touch(path.$()));

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    Assert.assertEquals(ff.getPageSize(), ff.length(path.$()));
                    Assert.assertEquals(0, writer.getVersionVolatile());
                    Assert.assertEquals(0, writer.getRegionOffset(0));
                    Assert.assertEquals(0, writer.getRegionOffset(1));
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "Empty definition file");
                }
            }
        });
    }

    @Test
    public void testReadEmptyDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.touch(path.$()));

                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.$());
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "block file too small [expected=40, actual=0");
                }
            }
        });
    }

    @Test
    public void testReadEmptyDefinitionFile2() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
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
                    TestUtils.assertContains(e.getMessage(), "cannot read block file, expected at least 1 commited data block");
                }
            }
        });
    }

    @Test
    public void testReadNonExistingDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.$());
                    Assert.fail("Expected exception");
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "cannot open block file");
                }
            }
        });
    }

    @Test
    public void testReadWriteAppendNewRegion() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                long prevRegionOffset;
                long prevRegionLength;
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgA1(writer.append(), 1);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgA2(writer.append(), 1);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgA1(writer.append(), 2);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                }

                readAllBlocks(path, 1, 2);
            }
        });
    }

    @Test
    public void testReadWriteAppendNewRegionNoSpace() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                long prevRegionOffset;
                long prevRegionLength;
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgA1(writer.append(), 1);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgA2(writer.append(), 1);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgA1(writer.append(), 2);
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
                    regionLength += commitMsgC(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 3, regionLength, prevRegionOffset + prevRegionLength);
                }

                readAllBlocks(path, 1, 3);
            }
        });
    }

    @Test
    public void testReadWriteChecksum() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    AppendableBlock memory1 = writer.append();
                    commitMsgA1(memory1, 1);
                    writer.commit();
                }
                readAllBlocks(path, 1, 1);

                // corrupt data
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                    mem.putInt(BlockFileUtils.HEADER_SIZE + BlockFileUtils.REGION_HEADER_SIZE + 8, -42);
                }

                try {
                    readAllBlocks(path, 1, 0);
                    Assert.fail("checksum mismatch expected");
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "block file checksum mismatch");
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
            AtomicInteger errorCounter = new AtomicInteger();
            AtomicInteger done = new AtomicInteger();
            int iterations = 1000;
            Thread writerThread = new Thread(() -> {
                try (Path path = getDefinitionFilePath()) {
                    final long pageSize = ff.getPageSize();
                    long maxCommitedRegionLength = 0;
                    BlockFileWriter writer = new BlockFileWriter(ff, commitMode);
                    writer.of(path.$());
                    try {
                        for (int i = 0; i < iterations; i++) {
                            int msg = rnd.nextInt(6);
                            // Reopen the writer occasionally.
                            if (rnd.nextInt(100) > 95) {
                                Misc.free(writer);
                                writer = new BlockFileWriter(ff, commitMode);
                                writer.of(path.$());
                            }
                            switch (msg) {
                                case 0:
                                    commitMsgA1(writer.append(), i + 1);
                                    break;
                                case 1:
                                    commitMsgA2(writer.append(), i + 1);
                                    break;
                                case 2:
                                    commitMsgB(writer.append());
                                    break;
                                case 3:
                                    commitMsgC(writer.append());
                                    break;
                                case 4:
                                    commitAllTypesMsgAppendAPI(writer.append());
                                    break;
                                case 5:
                                    // generate several pages of longs
                                    int bound = (int) pageSize / Long.BYTES;
                                    int numLongs = bound / 2 + rnd.nextInt(bound);
                                    commitLongsMsgAppendAPI(writer.append(), numLongs);
                                    break;
                            }
                            writer.commit();
                            maxCommitedRegionLength = Math.max(
                                    maxCommitedRegionLength,
                                    writer.getRegionLength(writer.getVersionVolatile())
                            );
                            // start reading after first commit
                            if (i == 0) {
                                start.await();
                            }
                        }
                    } finally {
                        Misc.free(writer);
                    }

                    long maxCommitSize = maxCommitedRegionLength + BlockFileUtils.HEADER_SIZE;
                    // file grows in pages, never truncated
                    long actualSize = ((maxCommitSize / pageSize) + 1) * pageSize;
                    long fileSize = ff.length(path.$());
                    // A truly random data file can grow if Size(v) > Size(v-2) + Size(v-3) + ... + Size(1), where v is the version.
                    // In this test, we generate a maximum of 2 pages of data per commit,
                    // so the file should not grow beyond 3 versions of the maximum size.
                    Assert.assertTrue(fileSize < 3 * actualSize);
                } catch (Throwable th) {
                    LOG.error().$("Error in writer thread: ").$(th).$();
                    errorCounter.incrementAndGet();
                } finally {
                    done.incrementAndGet();
                }
            });

            Thread[] readers = new Thread[readerThreads];
            for (int t = 0; t < readerThreads; t++) {
                Thread readerThread = new Thread(() -> {
                    try (Path path = getDefinitionFilePath()) {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            Os.pause(); // interleave reads and writes
                            readAllBlocks(path, 1, -1);
                        }
                    } catch (Throwable th) {
                        LOG.error().$("Error in reader thread: ").$(th).$();
                        errorCounter.incrementAndGet();
                    }
                });
                readers[t] = readerThread;
                readerThread.start();
            }

            writerThread.start();
            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }
            Assert.assertEquals(0, errorCounter.get());
        });
    }

    @Test
    public void testReadWriteOverwriteNewRegion() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                long prevRegionOffset;
                long prevRegionLength;
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgA1(writer.append(), 1);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgA2(writer.append(), 1);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgA1(writer.append(), 2);
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                }

                // this version will overwrite the previous region
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());
                    int regionLength = BlockFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgB(writer.append());
                    regionLength += BlockFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 3, regionLength, 0);
                }

                readAllBlocks(path, 1, 3);
            }
        });
    }

    @Test
    public void testReadWriteSimple() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());

                    AppendableBlock memory1 = writer.append();
                    commitMsgA1(memory1, 1);
                    final int commitedLength = memory1.length();

                    WritableBlock memory2 = writer.reserve(commitedLength);
                    Assert.assertEquals(commitedLength, memory2.length());
                    commitMsgA1RW(memory2);

                    AppendableBlock memory3 = writer.append();
                    commitMsgA2(memory3, 1);

                    writer.commit();
                }

                readAllBlocks(path, 3, 1);
            }
        });
    }

    @Test
    public void testReadWriteSimpleAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath()) {
                FilesFacade ff = configuration.getFilesFacade();
                try (BlockFileWriter writer = new BlockFileWriter(ff, commitMode)) {
                    writer.of(path.$());

                    // No blocks were committed yet, so commit must fail.
                    try {
                        writer.commit();
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    AppendableBlock memory1 = writer.append();
                    commitAllTypesMsgAppendAPI(memory1);
                    final int commitedLength = memory1.length();

                    WritableBlock memory2 = writer.reserve(commitedLength);
                    Assert.assertEquals(commitedLength, memory2.length());
                    commitAllTypesMsgWriteAPI(memory2);
                    Assert.assertEquals(commitedLength, memory2.length());

                    AppendableBlock memory3 = writer.append();
                    commitAllTypesMsgAppendAPI(memory3);

                    writer.commit();
                    try {
                        writer.commit();
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                }

                readAllBlocks(path, 3, 1);
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

    private static void commitAllTypesMsgAppendAPI(AppendableBlock memory) {
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
        memory.commit(MSG_TYPE_ALL);
        try {
            memory.commit(MSG_TYPE_ALL);
            Assert.fail();
        } catch (CairoException ignored) {
        }
    }

    private static void commitAllTypesMsgWriteAPI(WritableBlock memory) {
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

        memory.commit(MSG_TYPE_ALL);
        try {
            memory.commit(MSG_TYPE_ALL);
            Assert.fail();
        } catch (CairoException ignored) {
        }
    }

    private static void commitLongsMsgAppendAPI(AppendableBlock memory, int numLongs) {
        memory.putInt(numLongs);
        for (int i = 0; i < numLongs; i++) {
            memory.putLong(i);
        }
        memory.commit(MSG_TYPE_NLONGS);
        try {
            memory.commit(MSG_TYPE_NLONGS);
            Assert.fail();
        } catch (CairoException ignored) {
        }
    }

    private static int commitMsgA1(AppendableBlock memory, long regionVersion) {
        memory.putStr("Hello");
        memory.putLong(regionVersion);
        memory.putVarchar(new GcUtf8String("World"));
        memory.putInt(456);
        memory.commit(MSG_TYPE_A1);
        return memory.length();
    }

    private static void commitMsgA1RW(WritableBlock memory) {
        String hello = "Hello";
        Utf8Sequence worldUtf8 = new GcUtf8String("World");
        int offset = 0;
        memory.putStr(offset, hello);
        offset += Vm.getStorageLength(hello);
        memory.putLong(offset, 1);
        offset += Long.BYTES;
        memory.putVarchar(offset, worldUtf8);
        offset += STRING_LENGTH_BYTES + worldUtf8.size();
        memory.putInt(offset, 456);
        memory.commit(MSG_TYPE_A1);
    }

    private static int commitMsgA2(AppendableBlock memory, long regionVersion) {
        memory.putLong(regionVersion);
        memory.putStr("Hello");
        memory.putInt(456);
        memory.putStr("World");
        memory.commit(MSG_TYPE_A2);
        return memory.length();
    }

    private static int commitMsgB(AppendableBlock memory) {
        memory.putStr("Hello");
        memory.putStr("World");
        memory.commit(MSG_TYPE_B);
        return memory.length();
    }

    private static int commitMsgC(AppendableBlock memory) {
        memory.putStr("Hello");
        final int count = 10;
        for (int i = 0; i < count; i++) {
            memory.putInt(i);
            memory.putStr("World");
        }
        memory.commit(MSG_TYPE_C);
        return memory.length();
    }

    private static Path getDefinitionFilePath() {
        Path path = new Path().of(configuration.getDbRoot()).concat("test").slash();
        FilesFacade ff = configuration.getFilesFacade();
        ff.mkdirs(path, configuration.getMkDirMode());
        return path.of(configuration.getDbRoot()).concat("test").concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
    }

    private static void readAllBlocks(Path path, int expectedBlocks, long expectedRegionVersion) {
        readAllBlocks(path, configuration, expectedBlocks, expectedRegionVersion);
    }

    private static void readAllBlocks(Path path, CairoConfiguration configuration, int expectedBlocks, long expectedRegionVersion) {
        try (BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(path.$());
            int blockCount = 0;
            BlockFileReader.BlockCursor cursor = reader.getCursor();
            if (expectedRegionVersion != -1) {
                Assert.assertEquals(expectedRegionVersion, cursor.getRegionVersion());
            }
            final long regionVersion = cursor.getRegionVersion();
            while (cursor.hasNext()) {
                ReadableBlock block = cursor.next();
                final int type = block.type();
                final long msgLen = block.length() - BlockFileUtils.BLOCK_HEADER_SIZE;
                switch (type) {
                    case MSG_TYPE_A1:
                        readMsgA1(block, regionVersion);
                        break;
                    case MSG_TYPE_A2:
                        readMsgA2(block, regionVersion);
                        break;
                    case MSG_TYPE_B:
                        readMsgB(block);
                        break;
                    case MSG_TYPE_C:
                        readMsgC(block);
                        break;
                    case MSG_TYPE_ALL:
                        long len = readAllTypesMsg(block);
                        Assert.assertEquals(msgLen, len);
                        break;
                    case MSG_TYPE_NLONGS:
                        readLongsMsg(block);
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

    private static void readLongsMsg(ReadableBlock memory) {
        long offset = 0;
        int numLongs = memory.getInt(offset);
        offset += Integer.BYTES;
        for (int i = 0; i < numLongs; i++) {
            Assert.assertEquals(i, memory.getLong(offset));
            offset += Long.BYTES;
        }
    }

    private static void readMsgA1(ReadableBlock memory, long regionVersion) {
        Assert.assertEquals(MSG_TYPE_A1, memory.type());
        long offset = 0;
        CharSequence str = memory.getStr(offset);
        Assert.assertEquals("Hello", str.toString());
        offset += Vm.getStorageLength(str);
        Assert.assertEquals(regionVersion, memory.getLong(offset));
        offset += Long.BYTES;
        Utf8Sequence var = memory.getVarchar(offset);
        Assert.assertEquals("World", var.toString());
        offset += (STRING_LENGTH_BYTES + var.size());
        Assert.assertEquals(456, memory.getInt(offset));
    }

    private static void readMsgA2(ReadableBlock memory, long regionVersion) {
        Assert.assertEquals(MSG_TYPE_A2, memory.type());
        long offset = 0;
        Assert.assertEquals(regionVersion, memory.getLong(offset));
        offset += Long.BYTES;
        CharSequence str = memory.getStr(offset);
        Assert.assertEquals("Hello", str.toString());
        offset += Vm.getStorageLength(str);
        Assert.assertEquals(456, memory.getInt(offset));
        offset += Integer.BYTES;
        str = memory.getStr(offset);
        Assert.assertEquals("World", str.toString());
    }

    private static void readMsgB(ReadableBlock memory) {
        Assert.assertEquals(MSG_TYPE_B, memory.type());
        long offset = 0;
        CharSequence str = memory.getStr(offset);
        Assert.assertEquals("Hello", str.toString());
        offset += Vm.getStorageLength(str);
        str = memory.getStr(offset);
        Assert.assertEquals("World", str.toString());
    }

    private static void readMsgC(ReadableBlock memory) {
        Assert.assertEquals(MSG_TYPE_C, memory.type());
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
