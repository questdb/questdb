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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.mv.AppendOnlyBlock;
import io.questdb.cairo.mv.DefinitionFileReader;
import io.questdb.cairo.mv.DefinitionFileUtils;
import io.questdb.cairo.mv.DefinitionFileWriter;
import io.questdb.cairo.mv.RandomAccessBlock;
import io.questdb.cairo.mv.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.MAT_VIEW_FILE_NAME;

public class DefinitionFileTest extends AbstractCairoTest {
    protected final static Log LOG = LogFactory.getLog(DefinitionFileTest.class);
    private static final short MSG_TYPE_A = 1;
    private static final byte MSG_TYPE_A_VERSION_1 = 1;
    private static final byte MSG_TYPE_A_VERSION_2 = 2;
    private static final short MSG_TYPE_B = 2;
    private static final byte MSG_TYPE_B_VERSION_1 = 1;
    private static final short MSG_TYPE_C = 3;
    private static final byte MSG_TYPE_C_VERSION_1 = 1;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        Zip.init();
    }

    @Test
    public void testCreateEmptyDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.touch(path.$()));

                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
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

                try (DefinitionFileReader reader = new DefinitionFileReader(ff)) {
                    reader.of(path.$());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("Expected at least 1 block"));
                }
            }
        });
    }

    @Test
    public void testReadEmptyDefinitionFile2() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();

                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                }

                try (DefinitionFileReader reader = new DefinitionFileReader(ff)) {
                    reader.of(path.$());
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("Expected at least 1 block"));
                }
            }
        });
    }

    @Test
    public void testReadNonExistingDefinitionFile() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                try (DefinitionFileReader reader = new DefinitionFileReader(ff)) {
                    reader.of(path.$());
                    Assert.fail("Expected exception");
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("Cannot find file"));
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
                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgAVersion2(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                }

                readAllBlocks(ff, path, 1);
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
                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgAVersion2(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                // message C will not fit into the same region
                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgCVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 3, regionLength, prevRegionOffset + prevRegionLength);
                }

                readAllBlocks(ff, path, 1);
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
                        try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
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
                            readAllBlocks(ff, path, 1);
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
                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    regionLength += commitMsgAVersion2(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 1, regionLength, 0);
                    prevRegionOffset = writer.getRegionOffset(writer.getVersionVolatile());
                    prevRegionLength = writer.getRegionLength(writer.getVersionVolatile());
                }

                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgAVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 2, regionLength, prevRegionOffset + prevRegionLength);
                }

                // this version will overwrite the previous region
                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());
                    int regionLength = DefinitionFileUtils.REGION_HEADER_SIZE;
                    regionLength += commitMsgBVersion1(writer.append());
                    regionLength += DefinitionFileUtils.BLOCK_HEADER_SIZE;
                    writer.commit();
                    assertRegionOffset(writer, 3, regionLength, 0);
                }

                readAllBlocks(ff, path, 1);
            }
        });
    }

    @Test
    public void testReadWriteSimple() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = getDefinitionFilePath("test")) {
                FilesFacade ff = configuration.getFilesFacade();
                try (DefinitionFileWriter writer = new DefinitionFileWriter(ff)) {
                    writer.of(path.$());

                    AppendOnlyBlock memory1 = writer.append();
                    commitMsgAVersion1(memory1);
                    final int commitedLength = memory1.length();

                    RandomAccessBlock memory2 = writer.reserve(commitedLength);
                    Assert.assertEquals(commitedLength, memory2.length());
                    commitMsgAVersion1RW(memory2);

                    AppendOnlyBlock memory3 = writer.append();
                    commitMsgAVersion2(memory3);

                    writer.commit();
                }

                readAllBlocks(ff, path, 3);
            }
        });
    }

    private static void assertRegionOffset(DefinitionFileWriter writer, long expectedVersion, int expectedRegionLength, long expectedRegionOffset) {
        final long version = writer.getVersionVolatile();
        Assert.assertEquals(expectedVersion, version);
        final long regionOffset = writer.getRegionOffset(version);
        final long regionLength = writer.getRegionLength(version);
        Assert.assertEquals(expectedRegionOffset, regionOffset);
        Assert.assertEquals(expectedRegionLength, regionLength);
    }

    private static int commitMsgAVersion1(AppendOnlyBlock memory) {
        memory.putStr("Hello");
        memory.putInt(123);
        memory.putStr("World");
        memory.putInt(456);
        memory.commit(MSG_TYPE_A, MSG_TYPE_A_VERSION_1, (byte) 0);
        return memory.length();
    }

    private static int commitMsgAVersion2(AppendOnlyBlock memory) {
        memory.putInt(123);
        memory.putStr("Hello");
        memory.putInt(456);
        memory.putStr("World");
        memory.commit(MSG_TYPE_A, MSG_TYPE_A_VERSION_2, (byte) 0);
        return memory.length();
    }

    private static int commitMsgBVersion1(AppendOnlyBlock memory) {
        memory.putStr("Hello");
        memory.putStr("World");
        memory.commit(MSG_TYPE_B, MSG_TYPE_B_VERSION_1, (byte) 0);
        return memory.length();
    }

    private static void commitMsgAVersion1RW(RandomAccessBlock memory) {
        String hello = "Hello";
        String world = "World";
        int offset = 0;
        memory.putStr(offset, hello);
        offset += Vm.getStorageLength(world);
        memory.putInt(offset, 123);
        offset += Integer.BYTES;
        memory.putStr(offset, world);
        offset += Vm.getStorageLength(world);
        memory.putInt(offset, 456);
        memory.commit(MSG_TYPE_A, MSG_TYPE_A_VERSION_1, (byte) 0);
    }

    private static int commitMsgCVersion1(AppendOnlyBlock memory) {
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
        Path path = new Path().of(configuration.getRoot()).concat(tableName).slash();
        FilesFacade ff = configuration.getFilesFacade();
        ff.mkdirs(path, configuration.getMkDirMode());
        return path.of(configuration.getRoot()).concat(tableName).concat(MAT_VIEW_FILE_NAME);
    }

    private static void readAllBlocks(FilesFacade ff, Path path, int expectedBlocks) throws IOException {
        try (DefinitionFileReader reader = new DefinitionFileReader(ff)) {
            reader.of(path.$());
            int blockCount = 0;
            DefinitionFileReader.BlocksCursor cursor = reader.getCursor();
            while (cursor.hasNext()) {
                ReadableBlock block = cursor.next();
                final short type = block.type();
                final byte version = block.version();
                final byte flags = block.flags();
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
                    default:
                        Assert.fail("Unexpected type");
                }
                blockCount++;
            }
            Assert.assertEquals(expectedBlocks, blockCount);
        }
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
        str = memory.getStr(offset);
        Assert.assertEquals("World", str.toString());
        offset += Vm.getStorageLength(str);
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
