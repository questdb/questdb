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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.*;

/**
 * Benchmark test for streaming parquet export without network I/O.
 * This isolates the serialization performance from network overhead.
 * <p>
 * Schema matches benchmark_parquet_export.sh:
 * - 3 SYMBOL columns (publisher, exch, symbol)
 * - 3 LONG columns (event_id, ccy, last_seen_md_id)
 * - 11 DOUBLE columns (incr_pnl, real_pnl, est_pnl, incr_fee, fee_amt, rebate_amt,
 * net_traded_qty, excess_qty, expected_qty, pnl_ccy_usd_rate, fee_ccy_usd_rate)
 * - 1 TIMESTAMP column (designated timestamp)
 */
@Ignore
public class StreamingParquetBenchmarkTest extends AbstractCairoTest {
    private static final int DATA_PAGE_SIZE = 1024 * 1024;  // 1MB
    private static final Log LOG = LogFactory.getLog(StreamingParquetBenchmarkTest.class);
    private static final int PARQUET_VERSION = ParquetVersion.PARQUET_VERSION_V2;
    private static final boolean RAW_ARRAY_ENCODING = false;
    // Configuration - adjust these for your benchmark
    private static final long ROW_COUNT = 140_000_000;  // 10M rows for quick test, use 140M for full benchmark
    private static final int ROW_GROUP_SIZE = 220_000;
    private static final boolean STATISTICS_ENABLED = true;
    private static final String TABLE_NAME = "benchmark_table";

    /**
     * Pure read benchmark - reads all data without Parquet encoding.
     * This measures raw disk/mmap read speed.
     */
    @Test
    public void testPureReadBenchmark() throws Exception {
        assertMemoryLeak(() -> {
            ensureTableExists();
            // Get table disk size
            long tableDiskSize = getTableDiskSize(TABLE_NAME);
            LOG.info().$("Table disk size: ").$(tableDiskSize / (1024 * 1024)).$(" MB").$();

            engine.releaseInactive();
            flushDiskCache();

            // Run the streaming export benchmark
            runStreamingExportBenchmark(tableDiskSize, ParquetCompression.COMPRESSION_LZ4_RAW, "LZ4RAW", 0);

            engine.releaseInactive();
            flushDiskCache();

            // Run the streaming export benchmark
            runStreamingExportBenchmark(tableDiskSize, ParquetCompression.COMPRESSION_UNCOMPRESSED, "UNCOMPRESSED", 0);


            engine.releaseInactive();
            flushDiskCache();

            // Run the streaming export benchmark
            runStreamingExportBenchmark(tableDiskSize, ParquetCompression.COMPRESSION_ZSTD, "ZSTD", 9);

            engine.releaseInactive();
            flushDiskCache();

            // Run pure read benchmark
            runPureReadBenchmark(tableDiskSize);
        });
    }

    private void ensureTableExists() throws Exception {
        // Table doesn't exist and no backup, create it
        LOG.info().$("Creating table with ").$(ROW_COUNT).$(" rows...").$();

        execute("CREATE TABLE " + TABLE_NAME + " (" +
                "publisher SYMBOL," +
                "exch SYMBOL," +
                "symbol SYMBOL," +
                "event_id LONG," +
                "ccy LONG," +
                "incr_pnl DOUBLE," +
                "real_pnl DOUBLE," +
                "est_pnl DOUBLE," +
                "incr_fee DOUBLE," +
                "fee_amt DOUBLE," +
                "rebate_amt DOUBLE," +
                "net_traded_qty DOUBLE," +
                "excess_qty DOUBLE," +
                "expected_qty DOUBLE," +
                "pnl_ccy_usd_rate DOUBLE," +
                "fee_ccy_usd_rate DOUBLE," +
                "last_seen_md_id LONG," +
                "timestamp TIMESTAMP" +
                ") TIMESTAMP(timestamp) PARTITION BY DAY");

        execute("INSERT INTO " + TABLE_NAME + " " +
                "SELECT " +
                "rnd_symbol('PUB_A', 'PUB_B', 'PUB_C', 'PUB_D', 'PUB_E') as publisher," +
                "rnd_symbol('NYSE', 'NASDAQ', 'CME', 'EUREX', 'LSE') as exch," +
                "rnd_symbol('AAPL', 'GOOG', 'MSFT', 'AMZN', 'META', 'NVDA', 'TSLA', 'JPM', 'BAC', 'WFC') as symbol," +
                "x as event_id," +
                "rnd_long(1, 100, 0) as ccy," +
                "rnd_double() * 10000 - 5000 as incr_pnl," +
                "rnd_double() * 100000 - 50000 as real_pnl," +
                "rnd_double() * 100000 - 50000 as est_pnl," +
                "rnd_double() * 100 as incr_fee," +
                "rnd_double() * 1000 as fee_amt," +
                "rnd_double() * 500 as rebate_amt," +
                "rnd_double() * 10000 as net_traded_qty," +
                "rnd_double() * 100 as excess_qty," +
                "rnd_double() * 10000 as expected_qty," +
                "rnd_double() * 2 as pnl_ccy_usd_rate," +
                "rnd_double() * 2 as fee_ccy_usd_rate," +
                "rnd_long(1, 1000000000, 0) as last_seen_md_id," +
                "'2025-01-01'::timestamp + x * 10000L as timestamp " +
                "FROM long_sequence(" + ROW_COUNT + ")");

        engine.releaseInactive();
        LOG.info().$("Table created successfully").$();
    }

    private void flushDiskCache() throws Exception {
        String osName = System.getProperty("os.name").toLowerCase();
        if (!osName.contains("linux")) {
            throw new UnsupportedOperationException("Disk cache flush only supported on Linux");
        }

        LOG.info().$("Flushing disk cache...").$();

        // First sync to flush pending writes
        Process syncProcess = new ProcessBuilder("sync").start();
        int syncExit = syncProcess.waitFor();
        if (syncExit != 0) {
            throw new RuntimeException("sync failed with exit code " + syncExit);
        }

        // Drop pagecache, dentries, and inodes
        Process dropProcess = new ProcessBuilder("sh", "-c", "echo 3 > /proc/sys/vm/drop_caches").start();
        int dropExit = dropProcess.waitFor();
        if (dropExit != 0) {
            throw new RuntimeException("drop_caches failed with exit code " + dropExit + " (requires root)");
        }

        LOG.info().$("Disk cache flushed successfully").$();
    }

    private long getTableDiskSize(String tableName) throws Exception {
        long diskSize = 0;
        try (RecordCursorFactory factory = select("SELECT sum(diskSize) FROM table_partitions('" + tableName + "')")) {
            try (var cursor = factory.getCursor(sqlExecutionContext)) {
                if (cursor.hasNext()) {
                    diskSize = cursor.getRecord().getLong(0);
                }
            }
        }
        return diskSize;
    }

    private void runPureReadBenchmark(long tableDiskSize) throws Exception {
        try (RecordCursorFactory factory = select("SELECT * FROM " + TABLE_NAME)) {
            RecordMetadata metadata = factory.getMetadata();
            int columnCount = metadata.getColumnCount();

            try (PageFrameCursor pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {

                long totalSum = 0;
                long totalRows = 0;
                long totalBytesRead = 0;
                int frameCount = 0;

                // Pre-allocate 4KB buffer for memcpy
                final int BUFFER_SIZE = 4096;
                long buffer = Unsafe.getUnsafe().allocateMemory(BUFFER_SIZE);

                LOG.info().$("Starting pure read benchmark (no Parquet, no Rust)...").$();

                ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                long startCpuTime = threadMXBean.getCurrentThreadCpuTime();
                long startTime = System.nanoTime();

                try {
                    PageFrame frame;
                    while ((frame = pageFrameCursor.next()) != null) {
                        long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();

                        // Read all columns in 4KB chunks using memcpy
                        for (int col = 0; col < columnCount; col++) {
                            long pageAddress = frame.getPageAddress(col);
                            long pageSize = frame.getPageSize(col);

                            if (pageAddress > 0 && pageSize > 0) {
                                // Copy in 4KB chunks
                                for (long offset = 0; offset < pageSize; offset += BUFFER_SIZE) {
                                    long chunkSize = Math.min(BUFFER_SIZE, pageSize - offset);
                                    Unsafe.getUnsafe().copyMemory(pageAddress + offset, buffer, chunkSize);
                                }
                                // Read one value to prevent optimization
                                totalSum += Unsafe.getUnsafe().getLong(buffer);
                                totalBytesRead += pageSize;
                            }

                            // Also read aux page if present (for var-length columns)
                            long auxPageAddress = frame.getAuxPageAddress(col);
                            long auxPageSize = frame.getAuxPageSize(col);
                            if (auxPageAddress > 0 && auxPageSize > 0) {
                                for (long offset = 0; offset < auxPageSize; offset += BUFFER_SIZE) {
                                    long chunkSize = Math.min(BUFFER_SIZE, auxPageSize - offset);
                                    Unsafe.getUnsafe().copyMemory(auxPageAddress + offset, buffer, chunkSize);
                                }
                                totalSum += Unsafe.getUnsafe().getLong(buffer);
                                totalBytesRead += auxPageSize;
                            }
                        }

                        totalRows += frameRowCount;
                        frameCount++;
                    }
                } finally {
                    Unsafe.getUnsafe().freeMemory(buffer);
                }

                long elapsedNs = System.nanoTime() - startTime;
                long cpuTimeNs = threadMXBean.getCurrentThreadCpuTime() - startCpuTime;

                double elapsedSec = elapsedNs / 1_000_000_000.0;
                double cpuTimeSec = cpuTimeNs / 1_000_000_000.0;
                double cpuUtilization = (cpuTimeNs * 100.0) / elapsedNs;
                double throughputMBps = (totalBytesRead / (1024.0 * 1024.0)) / elapsedSec;
                double tableThroughputMBps = (tableDiskSize / (1024.0 * 1024.0)) / elapsedSec;

                LOG.info().$("=== Pure Read Benchmark Results ===").$();
                LOG.info().$("Total rows: ").$(totalRows).$();
                LOG.info().$("Total frames: ").$(frameCount).$();
                LOG.info().$("Bytes read: ").$(totalBytesRead / (1024 * 1024)).$(" MB").$();
                LOG.info().$("Table disk size: ").$(tableDiskSize / (1024 * 1024)).$(" MB").$();
                LOG.info().$("Checksum (to prevent optimization): ").$(totalSum).$();
                LOG.info().$("Elapsed (wall) time: ").$(String.format("%.3f", elapsedSec)).$("s").$();
                LOG.info().$("CPU time: ").$(String.format("%.3f", cpuTimeSec)).$("s").$();
                LOG.info().$("CPU utilization: ").$(String.format("%.1f", cpuUtilization)).$("%").$();
                LOG.info().$("Read throughput: ").$(String.format("%.2f", throughputMBps)).$(" MB/s").$();
                LOG.info().$("Table throughput: ").$(String.format("%.2f", tableThroughputMBps)).$(" MB/s").$();
                LOG.info().$("==========================================").$();

                if (cpuUtilization > 90) {
                    LOG.info().$("Analysis: CPU-bound (memory/compute limited)").$();
                } else if (cpuUtilization > 50) {
                    LOG.info().$("Analysis: Mixed CPU/IO").$();
                } else {
                    LOG.info().$("Analysis: IO-bound (disk limited)").$();
                }
            }
        }
    }

    private void runStreamingExportBenchmark(long tableDiskSize, int compressionLz4Raw, String codecName, int compressionLevel) throws Exception {
        try (RecordCursorFactory factory = select("SELECT * FROM " + TABLE_NAME)) {
            RecordMetadata metadata = factory.getMetadata();

            try (PageFrameCursor pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC);
                 DirectUtf8Sink columnNames = new DirectUtf8Sink(256, false, MemoryTag.NATIVE_PARQUET_EXPORTER);
                 DirectLongList columnMetadata = new DirectLongList(64, MemoryTag.NATIVE_PARQUET_EXPORTER, true);
                 DirectLongList columnData = new DirectLongList(256, MemoryTag.NATIVE_PARQUET_EXPORTER, true)) {

                // Initialize the sinks
                columnNames.reopen();
                columnMetadata.reopen();
                columnData.reopen();

                // Setup column names and metadata
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    CharSequence columnName = metadata.getColumnName(i);
                    int startSize = columnNames.size();
                    columnNames.put(columnName);
                    columnMetadata.add(columnNames.size() - startSize);
                    int columnType = metadata.getColumnType(i);

                    if (ColumnType.isSymbol(columnType)) {
                        StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(i);
                        assert symbolTable != null;
                        int symbolColumnType = columnType;
                        if (!symbolTable.containsNullValue()) {
                            symbolColumnType |= 1 << 31;
                        }
                        columnMetadata.add((long) metadata.getWriterIndex(i) << 32 | symbolColumnType);
                    } else {
                        columnMetadata.add((long) metadata.getWriterIndex(i) << 32 | columnType);
                    }
                }

                // Create streaming writer
                long streamWriter = createStreamingParquetWriter(
                        Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_EXPORTER),
                        metadata.getColumnCount(),
                        columnNames.ptr(),
                        columnNames.size(),
                        columnMetadata.getAddress(),
                        metadata.getTimestampIndex(),
                        false,  // not descending
                        ParquetCompression.packCompressionCodecLevel(compressionLz4Raw, compressionLevel),
                        STATISTICS_ENABLED,
                        RAW_ARRAY_ENCODING,
                        ROW_GROUP_SIZE,
                        DATA_PAGE_SIZE,
                        PARQUET_VERSION
                );

                try {
                    // Benchmark variables
                    long totalBytesWritten = 0;
                    long totalRows = 0;
                    int frameCount = 0;
                    int rowGroupCount = 0;

                    LOG.info().$("Starting streaming export benchmark compression codec=").$(codecName)
                            .$(" level=").$(compressionLevel)
                            .$("...").$();

                    // CPU time measurement
                    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                    long startCpuTime = threadMXBean.getCurrentThreadCpuTime();
                    long startTime = System.nanoTime();

                    // Process all page frames
                    PageFrame frame;
                    while ((frame = pageFrameCursor.next()) != null) {
                        if (frame.getFormat() == PartitionFormat.NATIVE) {
                            columnData.clear();
                            long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();

                            for (int i = 0, n = frame.getColumnCount(); i < n; i++) {
                                long localColTop = frame.getPageAddress(i) > 0 ? 0 : frameRowCount;
                                int columnType = metadata.getColumnType(i);

                                if (ColumnType.isSymbol(columnType)) {
                                    SymbolMapReader symbolMapReader = (SymbolMapReader) pageFrameCursor.getSymbolTable(i);
                                    MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                                    MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();

                                    columnData.add(localColTop);
                                    columnData.add(frame.getPageAddress(i));
                                    columnData.add(frame.getPageSize(i));
                                    columnData.add(symbolValuesMem.addressOf(0));
                                    columnData.add(symbolValuesMem.size());
                                    columnData.add(symbolOffsetsMem.addressOf(HEADER_SIZE));
                                    columnData.add(symbolMapReader.getSymbolCount());
                                } else {
                                    columnData.add(localColTop);
                                    columnData.add(frame.getPageAddress(i));
                                    columnData.add(frame.getPageSize(i));
                                    columnData.add(frame.getAuxPageAddress(i));
                                    columnData.add(frame.getAuxPageSize(i));
                                    columnData.add(0L);
                                    columnData.add(0L);
                                }
                            }

                            // Write chunk - returns buffer when row group is complete
                            long buffer = writeStreamingParquetChunk(streamWriter, columnData.getAddress(), frameRowCount);
                            while (buffer != 0) {
                                long dataSize = Unsafe.getUnsafe().getLong(buffer);
                                totalBytesWritten += dataSize;
                                rowGroupCount++;

                                // Get next buffer (if any)
                                buffer = writeStreamingParquetChunk(streamWriter, 0, 0);
                            }

                            totalRows += frameRowCount;
                            frameCount++;
                        }
                    }

                    // Finish writing (flush remaining data)
                    long buffer = finishStreamingParquetWrite(streamWriter);
                    while (buffer != 0) {
                        long dataSize = Unsafe.getUnsafe().getLong(buffer);
                        totalBytesWritten += dataSize;
                        rowGroupCount++;
                        buffer = writeStreamingParquetChunk(streamWriter, 0, 0);
                    }

                    long elapsedNs = System.nanoTime() - startTime;
                    long cpuTimeNs = threadMXBean.getCurrentThreadCpuTime() - startCpuTime;

                    double elapsedSec = elapsedNs / 1_000_000_000.0;
                    double cpuTimeSec = cpuTimeNs / 1_000_000_000.0;
                    double cpuUtilization = (cpuTimeNs * 100.0) / elapsedNs;
                    double outputThroughputMBps = (totalBytesWritten / (1024.0 * 1024.0)) / elapsedSec;
                    double inputThroughputMBps = (tableDiskSize / (1024.0 * 1024.0)) / elapsedSec;

                    LOG.info().$("=== Streaming Export Benchmark Results ===").$();
                    LOG.info().$("Total rows: ").$(totalRows).$();
                    LOG.info().$("Total frames: ").$(frameCount).$();
                    LOG.info().$("Total row groups: ").$(rowGroupCount).$();
                    LOG.info().$("Table disk size: ").$(tableDiskSize / (1024 * 1024)).$(" MB").$();
                    LOG.info().$("Parquet bytes written: ").$(totalBytesWritten).$(" (")
                            .$(totalBytesWritten / (1024 * 1024)).$(" MB)").$();
                    LOG.info().$("Elapsed (wall) time: ").$(String.format("%.3f", elapsedSec)).$("s").$();
                    LOG.info().$("CPU time: ").$(String.format("%.3f", cpuTimeSec)).$("s").$();
                    LOG.info().$("CPU utilization: ").$(String.format("%.1f", cpuUtilization)).$("%").$();
                    LOG.info().$("Input throughput (table): ").$(String.format("%.2f", inputThroughputMBps)).$(" MB/s").$();
                    LOG.info().$("Output throughput (parquet): ").$(String.format("%.2f", outputThroughputMBps)).$(" MB/s").$();
                    LOG.info().$("==========================================").$();

                    // Analysis
                    if (cpuUtilization > 90) {
                        LOG.info().$("Analysis: CPU-bound (").$(String.format("%.1f", cpuUtilization)).$("% CPU utilization)").$();
                    } else if (cpuUtilization > 50) {
                        LOG.info().$("Analysis: Mixed CPU/IO (").$(String.format("%.1f", cpuUtilization)).$("% CPU utilization)").$();
                    } else {
                        LOG.info().$("Analysis: IO-bound (").$(String.format("%.1f", cpuUtilization)).$("% CPU utilization)").$();
                    }

                } finally {
                    closeStreamingParquetWriter(streamWriter);
                }
            }
        }
    }
}
