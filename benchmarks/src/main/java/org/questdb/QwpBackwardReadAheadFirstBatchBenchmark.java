/*+*****************************************************************************
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

package org.questdb;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cutlass.qwp.server.egress.QwpBackwardReadAhead;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Cost of reading the first batch of a descending QWP stream ahead over data in the page
 * cache: one stream navigates to the frames its first batch of 16,384 rows reaches, newest
 * first, and advises each in the slices the egress walks it in, as the egress does before it
 * sends the batch. Frames of a few rows stand for the partitions of a table that takes a few
 * rows an hour or a day, whose first batch reaches thousands of them. Each column is a file
 * that holds the rows of every frame, mapped and read into memory before the measurement, as
 * the reader pool keeps the partitions of a hot table; a frame publishes its rows of each. The
 * walk itself, which copies the rows, is not part of the measurement.
 * <p>
 * The stream advises its windows with {@code MADV_WILLNEED} on Linux only; elsewhere it reads
 * them itself, which this benchmark does not measure. Build (note {@code -am} so the
 * benchmark links the in-tree core) and run on Linux, with the files on a disk file system
 * (a few MB under {@code java.io.tmpdir}):
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests
 * java --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.QwpBackwardReadAheadFirstBatchBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
})
public class QwpBackwardReadAheadFirstBatchBenchmark {
    private static final long BATCH_ROWS = QwpBackwardReadAhead.INITIAL_WINDOW_ROWS;
    private static final int SLICE = 1_024;
    // the mapped column files, one per column, each holding the rows of every frame
    private final LongList columnAddresses = new LongList();
    private final ObjList<MappedFrame> frames = new ObjList<>();
    private final GenericRecordMetadata metadata = new GenericRecordMetadata();
    @Param({"5", "31"})
    public int columns;
    @Param({"1", "5", "100", "1440", "16384"})
    public int frameRows;
    private long columnSize;
    private File dir;
    private QwpBackwardReadAhead readAhead;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpBackwardReadAheadFirstBatchBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long firstBatch() {
        readAhead.of(metadata);
        long bytes = 0;
        long batchLeft = BATCH_ROWS;
        for (int f = 0; batchLeft > 0; f++) {
            readAhead.ofFrame(frames.getQuick(f), frameRows);
            for (long hi = frameRows; hi > 0 && batchLeft > 0; ) {
                final long slice = Math.min(Math.min(SLICE, hi), batchLeft);
                // as the egress does, the first batch tells the read-ahead the lowest row it takes
                bytes += readAhead.advise(hi - slice, Math.max(0, hi - batchLeft));
                hi -= slice;
                batchLeft -= slice;
            }
        }
        readAhead.clear();
        return bytes;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        for (int c = 0; c < columns; c++) {
            metadata.add(new TableColumnMetadata("c" + c, c == 0 ? ColumnType.TIMESTAMP : ColumnType.LONG, IndexType.NONE, 0, false, null));
        }
        final int frameCount = (int) ((BATCH_ROWS + frameRows - 1) / frameRows);
        columnSize = 8L * frameRows * frameCount;
        dir = java.nio.file.Files.createTempDirectory("qwp-read-ahead-first-batch").toFile();
        try (Path path = new Path()) {
            for (int c = 0; c < columns; c++) {
                path.of(dir.getAbsolutePath()).slash().put(c).$();
                final long fd = Files.openRW(path.$());
                try {
                    Files.truncate(fd, columnSize);
                    final long address = Files.mmap(fd, columnSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                    // write every page, so the column is in the page cache and mapped
                    for (long p = 0; p < columnSize; p += 8) {
                        Unsafe.putLong(address + p, p);
                    }
                    columnAddresses.add(address);
                } finally {
                    Files.close(fd);
                }
            }
        }
        for (int f = 0; f < frameCount; f++) {
            frames.add(new MappedFrame(columnAddresses, 8L * frameRows * f, frameRows));
        }
        readAhead = new QwpBackwardReadAhead(FilesFacadeImpl.INSTANCE);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        for (int c = 0, n = columnAddresses.size(); c < n; c++) {
            Files.munmap(columnAddresses.getQuick(c), columnSize, MemoryTag.MMAP_DEFAULT);
        }
        columnAddresses.clear();
        frames.clear();
        final File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                //noinspection ResultOfMethodCallIgnored
                file.delete();
            }
        }
        //noinspection ResultOfMethodCallIgnored
        dir.delete();
    }

    private static final class MappedFrame implements PageFrame {
        private final LongList columnAddresses;
        private final long offset;
        private final long rows;
        private final long size;

        MappedFrame(LongList columnAddresses, long offset, long rows) {
            this.columnAddresses = columnAddresses;
            this.offset = offset;
            this.rows = rows;
            this.size = 8 * rows;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return columnAddresses.size();
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return columnAddresses.getQuick(columnIndex) + offset;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return size;
        }

        @Override
        public int getParquetRowGroup() {
            return -1;
        }

        @Override
        public int getParquetRowGroupHi() {
            return -1;
        }

        @Override
        public int getParquetRowGroupLo() {
            return -1;
        }

        @Override
        public long getPartitionHi() {
            return rows;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }
    }
}
