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

import io.questdb.cairo.PartitionBy;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

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
        assertChunksFor("test-quotes-tslast.csv", 10, 3,
                chunk("2022-05-10/0_1", 1652183520000000L, 15L),
                chunk("2022-05-11/0_1", 1652269920000000L, 90L));
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

    private void assertChunksFor(String fileName, long bufSize, int timestampIndex, IndexChunk... chunks) throws Exception {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            long bufAddr = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);

            try (CsvFileIndexer indexer = new CsvFileIndexer(engine.getConfiguration());
                 DirectCharSink sink = new DirectCharSink(engine.getConfiguration().getTextConfiguration().getUtf8SinkSize())) {

                long length = ff.length(Path.getThreadLocal(inputRoot).concat(fileName).$());

                indexer.of(fileName, inputWorkRoot, 0, PartitionBy.DAY, (byte) ',', timestampIndex, getAdapter("yyyy-MM-ddTHH:mm:ss.SSSZ", sink), true, Atomicity.SKIP_COL);
                indexer.index(0, length, 0, new LongList(), bufAddr, bufSize);
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
