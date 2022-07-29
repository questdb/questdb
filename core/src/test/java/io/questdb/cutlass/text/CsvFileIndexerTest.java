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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import static io.questdb.cutlass.text.ParallelCsvFileImporterTest.*;

public class CsvFileIndexerTest extends AbstractGriffinTest {

    @Before
    public void before() throws IOException {
        inputRoot = new File("./src/test/resources/csv/").getAbsolutePath();
        inputWorkRoot = temp.newFolder("imports" + System.nanoTime()).getAbsolutePath();
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldSplitBetweenMinLengthReadBuffers() throws Exception {
        assertChunksFor("test-quotes-small.csv", 1, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L, 1652269920001000L, 185));
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldSplitBetweenTinyReadBuffers() throws Exception {
        assertChunksFor("test-quotes-small.csv", 10, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L, 1652269920001000L, 185));
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldAtLineEndSplitBetweenTinyReadBuffers() throws Exception {
        assertChunksFor("test-quotes-tslast.csv", 1, 3,
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L));
    }

    @Test//timestamp should be reassembled properly via rolling buffer
    public void testIndexChunksInCsvWithTimestampFieldAtLineEndSplitBetweenTinyReadBuffers2() throws Exception {
        assertChunksFor("test-quotes-tslast2.csv", 1, 3,
                chunk("2022-05-10/0_1", 1652183520000000L, 14L));
    }

    @Test//timestamp is ignored if it doesn't fit in ts rolling buffer 
    public void testIndexChunksInCsvWithTooLongTimestampFieldSplitBetweenMinLengthReadBuffers() throws Exception {
        assertChunksFor("test-quotes-tstoolong.csv", 1, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 14L),
                chunk("2022-05-11/0_1", 1652269920001000L, 263L));
    }

    @Test//timestamp is ignored if it doesn't fit in ts rolling buffer 
    public void testIndexChunksInCsvWithTooLongTimestampFieldSplitBetweenTinyReadBuffers() throws Exception {
        assertChunksFor("test-quotes-tstoolong.csv", 10, 1,
                chunk("2022-05-10/0_1", 1652183520000000L, 14L),
                chunk("2022-05-11/0_1", 1652269920001000L, 263L));
    }

    @Test
    public void testIndexFileWithLowChunkSizeLimitProducesMoreFiles() throws Exception {
        assertChunksFor("test-quotes-small.csv", 10, 1, 16, chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L),
                chunk("2022-05-11/0_2", 1652269920001000L, 185));
    }

    @Test
    public void testIndexFileFailsWhenItCantCreatePartitionDirectory() {
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            final String partition = "2022-05-10" + File.separator;

            @Override
            public boolean exists(LPSZ path) {
                if (Chars.endsWith(path, partition)) {
                    return false;
                }
                return super.exists(path);
            }

            @Override
            public int mkdir(Path path, int mode) {
                if (Chars.endsWith(path, partition)) {
                    return -1;
                }
                return super.mkdir(path, mode);
            }
        };

        assertFailureFor(ff, "test-quotes-small.csv", 1, "Couldn't create partition dir ");
    }

    @Test
    public void testIndexFileFailsWhenIndexFileAlreadyExists() {
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            final String partition = "2022-05-10" + File.separator + "0_1";

            @Override
            public boolean exists(LPSZ path) {
                if (Chars.endsWith(path, partition)) {
                    return true;
                }
                return super.exists(path);
            }
        };

        assertFailureFor(ff, "test-quotes-small.csv", 1, "index file already exists");
    }

    public void assertFailureFor(FilesFacade ff, String fileName, int timestampIndex, String errorMessage) {
        try {
            assertChunksFor(ff, fileName, 10, timestampIndex, 16);
            Assert.fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString(errorMessage));
        }
    }

    private void assertChunksFor(String fileName, long bufSize, int timestampIndex, IndexChunk... chunks) throws Exception {
        assertChunksFor(fileName, bufSize, timestampIndex, -1, chunks);
    }

    private void assertChunksFor(String fileName, long bufSize, int timestampIndex, int chunkSize, IndexChunk... chunks) throws Exception {
        assertChunksFor(FilesFacadeImpl.INSTANCE, fileName, bufSize, timestampIndex, chunkSize, chunks);
    }

    private void assertChunksFor(FilesFacade ff2, String fileName, long bufSize, int timestampIndex, int chunkSize, IndexChunk... chunks) throws Exception {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            long bufAddr = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);

            CairoConfiguration conf = new CairoConfigurationWrapper(engine.getConfiguration()) {
                @Override
                public long getSqlCopyMaxIndexChunkSize() {
                    if (chunkSize > 0) {
                        return chunkSize;
                    }
                    return super.getSqlCopyMaxIndexChunkSize();
                }

                @Override
                public FilesFacade getFilesFacade() {
                    return ff2 != null ? ff2 : ff;
                }
            };

            try (CsvFileIndexer indexer = new CsvFileIndexer(conf);
                 DirectCharSink sink = new DirectCharSink(engine.getConfiguration().getTextConfiguration().getUtf8SinkSize())) {

                long length = ff.length(Path.getThreadLocal(inputRoot).concat(fileName).$());

                indexer.of(
                        fileName,
                        inputWorkRoot,
                        0,
                        PartitionBy.DAY,
                        (byte) ',',
                        timestampIndex,
                        getAdapter("yyyy-MM-ddTHH:mm:ss.SSSZ", sink),
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

    private TimestampAdapter getAdapter(String format, DirectCharSink sink) {
        TextConfiguration textConfiguration = engine.getConfiguration().getTextConfiguration();
        TypeManager typeManager = new TypeManager(textConfiguration, sink);
        DateFormat dateFormat = typeManager.getInputFormatConfiguration().getTimestampFormatFactory().get(format);
        return (TimestampAdapter) typeManager.nextTimestampAdapter(false, dateFormat,
                configuration.getTextConfiguration().getDefaultDateLocale());
    }
}
