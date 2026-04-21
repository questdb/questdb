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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ParquetMetaFileWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.table.parquet.ParquetColumnChunkResolver;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
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

    private static final TableToken FAKE_TABLE = new TableToken(
            "test_table",
            "test_table~1",
            null,
            1,
            false,
            false,
            false
    );

    @BeforeClass
    public static void loadNativeLib() {
        Os.init();
    }

    @After
    public void clearColdResolver() {
        ParquetColumnChunkResolver.Holder.INSTANCE = null;
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
        // ParquetPartitionDecoder), and its native handle must be
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

    @Test
    public void testDecodeRowGroupColdPathInvokesResolver() throws Exception {
        // Confirms the cold-path branch builds a partition path from the
        // table/timestampType/partitionBy/timestamp/nameTxn fields stored on
        // the decoder and hands it to the registered resolver.
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                CapturingResolver resolver = new CapturingResolver();
                ParquetColumnChunkResolver.Holder.INSTANCE = resolver;
                ParquetPartitionDecoder decoder = new ParquetPartitionDecoder();
                try (
                        RowGroupBuffers bufs = new RowGroupBuffers(MemoryTag.NATIVE_DEFAULT);
                        DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT)
                ) {
                    columns.add(0); // parquet column index
                    columns.add(8); // ColumnType.TIMESTAMP_MICRO

                    decoder.of(
                            file.dataPtr,
                            file.dataLen,
                            0L, // parquetAddr = 0 -> cold path
                            0L,
                            FAKE_TABLE,
                            PartitionBy.DAY,
                            8, // ColumnType.TIMESTAMP_MICRO
                            1_700_000_000_000_000L,
                            42L,
                            MemoryTag.NATIVE_DEFAULT
                    );
                    try {
                        decoder.decodeRowGroup(bufs, columns, 0, 0, 100);
                        Assert.fail("expected resolver sentinel exception");
                    } catch (RuntimeException e) {
                        Assert.assertTrue(
                                "expected resolver sentinel, got: " + e.getMessage(),
                                e.getMessage() != null && e.getMessage().contains("RESOLVER_REACHED")
                        );
                    }
                    Assert.assertEquals(1, resolver.calls);
                    Assert.assertEquals(1, resolver.lastColumnsSize);
                    Assert.assertNotNull(resolver.lastPartitionPath);
                    Assert.assertTrue(
                            "partition path must include table dir, was: " + resolver.lastPartitionPath,
                            resolver.lastPartitionPath.contains("test_table~")
                    );
                } finally {
                    decoder.close();
                }
            }
        });
    }

    @Test
    public void testDecodeRowGroupColdPathWithoutResolverThrows() throws Exception {
        // No resolver registered + parquetAddr=0 must fail loudly, not NPE.
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                ParquetPartitionDecoder decoder = new ParquetPartitionDecoder();
                try (
                        RowGroupBuffers bufs = new RowGroupBuffers(MemoryTag.NATIVE_DEFAULT);
                        DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT)
                ) {
                    columns.add(0);
                    columns.add(8);

                    decoder.of(
                            file.dataPtr, file.dataLen,
                            0L, 0L,
                            FAKE_TABLE, PartitionBy.DAY, 8,
                            1_700_000_000_000_000L, 42L,
                            MemoryTag.NATIVE_DEFAULT
                    );
                    try {
                        decoder.decodeRowGroup(bufs, columns, 0, 0, 100);
                        Assert.fail("expected CairoException about missing resolver");
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                "unexpected message: " + e.getMessage(),
                                e.getMessage().contains("no column chunk resolver configured")
                        );
                    }
                } finally {
                    decoder.close();
                }
            }
        });
    }

    @Test
    public void testFindRowGroupByTimestampColdRequiresInlineStats() throws Exception {
        // The cold-path finder errors when timestamp inline stats are
        // unavailable (build_ts_pm equivalent: no min/max set).
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                ParquetPartitionDecoder decoder = new ParquetPartitionDecoder();
                try {
                    decoder.of(
                            file.dataPtr, file.dataLen,
                            0L, 0L,
                            FAKE_TABLE, PartitionBy.DAY, 8,
                            1_700_000_000_000_000L, 42L,
                            MemoryTag.NATIVE_DEFAULT
                    );
                    try {
                        decoder.findRowGroupByTimestamp(0L, 0L, 100L, 0);
                        Assert.fail("expected error about missing inline timestamp stats");
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                "unexpected message: " + e.getMessage(),
                                e.getMessage().contains("_pm is missing inline timestamp min/max stats")
                                        && e.getMessage().contains("Rebuild _pm")
                        );
                    }
                } finally {
                    decoder.close();
                }
            }
        });
    }

    @Test
    public void testShallowCopyCarriesColdPathFields() throws Exception {
        // Regression: of(other) must copy table/partitionBy/timestampType/
        // timestamp/nameTxn so the copy can build the partition path on the
        // cold path. Without the copy, buildColdPartitionPath() NPEs.
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                CapturingResolver resolver = new CapturingResolver();
                ParquetColumnChunkResolver.Holder.INSTANCE = resolver;
                ParquetPartitionDecoder source = new ParquetPartitionDecoder();
                ParquetPartitionDecoder copy = new ParquetPartitionDecoder();
                try (
                        RowGroupBuffers bufs = new RowGroupBuffers(MemoryTag.NATIVE_DEFAULT);
                        DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT)
                ) {
                    columns.add(0);
                    columns.add(8);

                    source.of(
                            file.dataPtr, file.dataLen,
                            0L, 0L,
                            FAKE_TABLE, PartitionBy.DAY, 8,
                            1_700_000_000_000_000L, 42L,
                            MemoryTag.NATIVE_DEFAULT
                    );
                    copy.of(source);

                    try {
                        copy.decodeRowGroup(bufs, columns, 0, 0, 100);
                        Assert.fail("expected resolver sentinel");
                    } catch (RuntimeException e) {
                        Assert.assertTrue(
                                "expected resolver sentinel, got: " + e.getMessage(),
                                e.getMessage() != null && e.getMessage().contains("RESOLVER_REACHED")
                        );
                    }
                    Assert.assertEquals(1, resolver.calls);
                    Assert.assertNotNull(resolver.lastPartitionPath);
                } finally {
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

    /**
     * Test resolver that records the partition path and column count from
     * the call, then aborts the decode by throwing a sentinel
     * {@link CairoException}. The exception is intentional: it short-circuits
     * the decode before any chunk pointers are dereferenced (the captured
     * fields aren't real backing storage).
     */
    private static final class CapturingResolver implements ParquetColumnChunkResolver {
        int calls;
        long lastFirstByteLength = -1;
        long lastFirstByteOffset = -1;
        int lastColumnsSize;
        String lastPartitionPath;

        @Override
        public void release(DirectLongList chunks, int columnsSize) {
            // No-op: the resolver always throws before allocating buffers.
        }

        @Override
        public void resolve(
                DirectUtf8Sequence partitionPath,
                DirectLongList byteRanges,
                int columnsSize,
                DirectLongList chunksOut
        ) {
            calls++;
            lastColumnsSize = columnsSize;
            lastPartitionPath = partitionPath.toString();
            if (columnsSize > 0) {
                lastFirstByteOffset = byteRanges.get(0);
                lastFirstByteLength = byteRanges.get(1);
            }
            throw CairoException.critical(0).put("RESOLVER_REACHED");
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
