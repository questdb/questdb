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

package io.questdb.test.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.CsvFileIndexer;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static io.questdb.test.cutlass.text.ParallelCsvFileImporterTest.*;

public class CsvFileIndexerTest extends AbstractCairoTest {

    public void assertFailureFor(FilesFacade ff, String fileName, int timestampIndex, String errorMessage) {
        try {
            assertChunksFor(ff, fileName, 10, timestampIndex, 16);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), errorMessage);
        }
    }

    @Before
    public void before() throws IOException {
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = temp.newFolder("imports" + System.nanoTime()).getAbsolutePath();
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldAtLineEndSplitBetweenTinyReadBuffers() throws Exception {
        assertChunksFor("test-quotes-tslast.csv", 1, 3,
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L)
        );
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldAtLineEndSplitBetweenTinyReadBuffers2() throws Exception {
        assertChunksFor("test-quotes-tslast2.csv", 1, 3,
                chunk("2022-05-10/0_1", 1652183520000000L, 14L)
        );
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldSplitBetweenMinLengthReadBuffers() throws Exception {
        assertChunksFor("test-quotes-small.csv", 1, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L, 1652269920001000L, 185)
        );
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldSplitBetweenTinyReadBuffers() throws Exception {
        assertChunksFor("test-quotes-small.csv", 10, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L, 1652269920001000L, 185)
        );
    }

    @Test//timestamp is ignored if it doesn't fit in ts rolling buffer
    public void testIndexChunksInCsvWithTooLongTimestampFieldSplitBetweenMinLengthReadBuffers() throws Exception {
        assertChunksFor("test-quotes-tstoolong.csv", 1, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 14L),
                chunk("2022-05-11/0_1", 1652269920001000L, 263L)
        );
    }

    @Test//timestamp is ignored if it doesn't fit in ts rolling buffer
    public void testIndexChunksInCsvWithTooLongTimestampFieldSplitBetweenTinyReadBuffers() throws Exception {
        assertChunksFor("test-quotes-tstoolong.csv", 10, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 14L),
                chunk("2022-05-11/0_1", 1652269920001000L, 263L)
        );
    }

    @Test
    public void testIndexFileFailsWhenIndexFileAlreadyExists() {
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            final String partition = "2022-05-10" + File.separator + "0_1";

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.endsWithAscii(path, partition)) {
                    return true;
                }
                return super.exists(path);
            }
        };

        assertFailureFor(ff, "test-quotes-small.csv", 1, "index file already exists");
    }

    @Test
    public void testIndexFileFailsWhenItCantCreatePartitionDirectory() {
        FilesFacadeImpl ff = new TestFilesFacadeImpl() {
            final String partition = "2022-05-10" + File.separator;

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.endsWithAscii(path, partition)) {
                    return false;
                }
                return super.exists(path);
            }

            @Override
            public int mkdir(LPSZ path, int mode) {
                if (Utf8s.endsWithAscii(path, partition)) {
                    return -1;
                }
                return super.mkdir(path, mode);
            }
        };

        assertFailureFor(ff, "test-quotes-small.csv", 1, "Couldn't create partition dir ");
    }

    @Test
    public void testIndexFileWithLowChunkSizeLimitProducesMoreFiles() throws Exception {
        assertChunksFor("test-quotes-small.csv", 10, 1, 16, chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L),
                chunk("2022-05-11/0_2", 1652269920001000L, 185)
        );
    }

    private void assertChunksFor(String fileName, long bufSize, int timestampIndex, IndexChunk... chunks) throws Exception {
        assertChunksFor(fileName, bufSize, timestampIndex, -1, chunks);
    }

    private void assertChunksFor(String fileName, long bufSize, int timestampIndex, int chunkSize, IndexChunk... chunks) throws Exception {
        assertChunksFor(TestFilesFacadeImpl.INSTANCE, fileName, bufSize, timestampIndex, chunkSize, chunks);
    }

    private void assertChunksFor(FilesFacade ff2, String fileName, long bufSize, int timestampIndex, int chunkSize, IndexChunk... chunks) throws Exception {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            long bufAddr = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);

            CairoConfiguration conf = new CairoConfigurationWrapper(engine.getConfiguration()) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff2 != null ? ff2 : ff;
                }

                @Override
                public long getSqlCopyMaxIndexChunkSize() {
                    if (chunkSize > 0) {
                        return chunkSize;
                    }
                    return super.getSqlCopyMaxIndexChunkSize();
                }
            };

            try (CsvFileIndexer indexer = new CsvFileIndexer(conf);
                 DirectUtf16Sink utf16sink = new DirectUtf16Sink(engine.getConfiguration().getTextConfiguration().getUtf8SinkSize());
                 DirectUtf8Sink utf8sink = new DirectUtf8Sink(engine.getConfiguration().getTextConfiguration().getUtf8SinkSize())
            ) {

                long length = ff.length(Path.getThreadLocal(inputRoot).concat(fileName).$());

                indexer.of(
                        fileName,
                        inputWorkRoot,
                        0,
                        ColumnType.TIMESTAMP,
                        PartitionBy.DAY,
                        (byte) ',',
                        timestampIndex,
                        getAdapter(utf16sink, utf8sink),
                        true, Atomicity.SKIP_COL,
                        null
                );

                indexer.index(
                        0,
                        length,
                        0,
                        new LongList(),
                        bufAddr,
                        bufSize
                );

                indexer.parseLast();

                ObjList<IndexChunk> actualChunks = readIndexChunks(new File(inputWorkRoot));
                Assert.assertEquals(list(chunks), actualChunks);
            } finally {
                Unsafe.free(bufAddr, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private TimestampAdapter getAdapter(DirectUtf16Sink utf16Sink, DirectUtf8Sink utf8Sink) {
        TextConfiguration textConfiguration = engine.getConfiguration().getTextConfiguration();
        TypeManager typeManager = new TypeManager(textConfiguration, utf16Sink, utf8Sink);
        DateFormat dateFormat = TypeManager.adaptiveGetTimestampFormat("yyyy-MM-ddTHH:mm:ss.SSSZ");
        return (TimestampAdapter) typeManager.nextTimestampAdapter(false, dateFormat,
                configuration.getTextConfiguration().getDefaultDateLocale(), "yyyy-MM-ddTHH:mm:ss.SSSZ"
        );
    }
}
