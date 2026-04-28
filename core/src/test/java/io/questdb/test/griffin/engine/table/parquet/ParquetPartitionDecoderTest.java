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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.ParquetMetaFileWriter;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Lifecycle tests for {@link ParquetPartitionDecoder} that guard the
 * two critical invariants introduced when {@link io.questdb.cairo.ParquetMetaFileReader}
 * gained a lazy native handle:
 * <ol>
 *   <li>{@code of(...)} must release any pre-existing native handle from the
 *       embedded reader before re-init. Guarded by {@link #testOfReinitDoesNotLeak}.</li>
 *   <li>{@code destroy()} must clear the embedded reader unconditionally,
 *       not gated on the {@code owned} flag, otherwise non-owning shallow
 *       copies leak the lazy native handle. Guarded by
 *       {@link #testShallowCopyClosesReaderHandle}.</li>
 * </ol>
 */
public class ParquetPartitionDecoderTest extends AbstractCairoTest {

    @BeforeClass
    public static void loadNativeLib() {
        Os.init();
    }

    @Test
    public void testOfReinitDoesNotLeak() throws Exception {
        // Guards lifecycle invariant 1: calling the owning of() overload
        // twice in a row must release the first instance's lazy native
        // handle before allocating a fresh one. assertMemoryLeak fails
        // the test if the first handle is stranded.
        assertMemoryLeak(() -> {
            try (
                    ParquetMetaTestFile file1 = buildFile(1, 100);
                    ParquetMetaTestFile file2 = buildFile(2, 200)
            ) {
                ParquetPartitionDecoder decoder = new ParquetPartitionDecoder();
                try {
                    // First init + lazy native handle allocation.
                    decoder.of(
                            file1.dataPtr,
                            file1.dataLen,
                            0L,
                            Long.MAX_VALUE,
                            MemoryTag.NATIVE_DEFAULT
                    );
                    try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                        Assert.assertFalse(decoder.metadata().canSkipRowGroup(0, filters, 0));
                    }

                    // Re-init via the owning of() overload. The previous
                    // native handle must be freed by destroy() → clear()
                    // before storing the new addresses.
                    decoder.of(
                            file2.dataPtr,
                            file2.dataLen,
                            0L,
                            Long.MAX_VALUE,
                            MemoryTag.NATIVE_DEFAULT
                    );
                    Assert.assertEquals(2, decoder.metadata().getColumnCount());

                    try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                        // Second skip call lazily allocates a fresh native
                        // handle over the new mmap.
                        Assert.assertFalse(decoder.metadata().canSkipRowGroup(0, filters, 0));
                    }
                } finally {
                    decoder.close();
                }
            }
        });
    }

    @Test
    public void testShallowCopyClosesReaderHandle() throws Exception {
        // Guards lifecycle invariant 2: destroy() must clear the embedded
        // reader unconditionally. The shallow-copy decoder owns its own
        // ParquetMetaFileReader instance (line 47 in
        // ParquetMetaPartitionDecoder), and its native handle must be
        // freed when the shallow copy closes — even though owned == false.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                ParquetPartitionDecoder source = new ParquetPartitionDecoder();
                ParquetPartitionDecoder copy = new ParquetPartitionDecoder();
                try {
                    source.of(
                            file.dataPtr,
                            file.dataLen,
                            0L,
                            Long.MAX_VALUE,
                            MemoryTag.NATIVE_DEFAULT
                    );

                    // Force the source's lazy native handle to allocate.
                    try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                        Assert.assertFalse(source.metadata().canSkipRowGroup(0, filters, 0));
                    }

                    // Create a non-owning shallow copy and force its own
                    // lazy native handle to allocate.
                    copy.of(source);
                    try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                        Assert.assertFalse(copy.metadata().canSkipRowGroup(0, filters, 0));
                    }
                } finally {
                    // Closing the shallow copy must free its own native
                    // handle even though owned == false. Closing the
                    // source frees its handle. assertMemoryLeak proves
                    // both happen.
                    copy.close();
                    source.close();
                }
            }
        });
    }

    /**
     * Builds a `_pm` byte buffer with the given column count and a single
     * row group of size {@code numRows}, then wraps it in a {@link ParquetMetaTestFile}.
     */
    private static ParquetMetaTestFile buildFile(int columnCount, long numRows) {
        long writerPtr = ParquetMetaFileWriter.create();
        try {
            ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, -1);
            for (int i = 0; i < columnCount; i++) {
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("col_").put(i);
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), i, 5, 0, 0, 0, 0, 0);
                }
            }
            ParquetMetaFileWriter.addRowGroup(writerPtr, numRows);
            ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
            long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
            return new ParquetMetaTestFile(resultPtr);
        } finally {
            ParquetMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    private static final class ParquetMetaTestFile implements AutoCloseable {
        final long dataLen;
        final long dataPtr;
        final long resultPtr;

        ParquetMetaTestFile(long resultPtr) {
            this.resultPtr = resultPtr;
            this.dataPtr = ParquetMetaFileWriter.resultDataPtr(resultPtr);
            this.dataLen = ParquetMetaFileWriter.resultDataLen(resultPtr);
        }

        @Override
        public void close() {
            ParquetMetaFileWriter.destroyResult(resultPtr);
        }
    }
}
