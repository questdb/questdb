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
 * Cost of reading a descending QWP stream ahead over data in the page cache: one stream
 * navigates to every frame of about 7.2M rows of five 8-byte columns, newest first, and
 * advises each in the 1,024-row slices the egress walks it in. Every column of every frame
 * is a file of its own, mapped and read into memory before the measurement, as the reader
 * pool keeps the partitions of a hot table. The walk itself, which copies the rows, is not
 * part of the measurement.
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core) and run, with the files
 * on a disk file system (a few hundred MB under {@code java.io.tmpdir}):
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests
 * java --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.QwpBackwardReadAheadBenchmark
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
public class QwpBackwardReadAheadBenchmark {
    private static final int COLUMNS = 5;
    private static final int SLICE = 1_024;
    private static final long STREAM_ROWS = 7_200_000;
    private final ObjList<MappedFrame> frames = new ObjList<>();
    private final GenericRecordMetadata metadata = new GenericRecordMetadata();
    private File dir;
    @Param({"1440", "3600", "7200", "86400", "1000000"})
    public int frameRows;
    private QwpBackwardReadAhead readAhead;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpBackwardReadAheadBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long stream() {
        readAhead.of(metadata);
        long bytes = 0;
        for (int f = frames.size() - 1; f > -1; f--) {
            final MappedFrame frame = frames.getQuick(f);
            readAhead.ofFrame(frame, frameRows);
            for (long hi = frameRows; hi > 0; hi -= SLICE) {
                bytes += readAhead.advise(Math.max(0, hi - SLICE));
            }
        }
        readAhead.clear();
        return bytes;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        for (int c = 0; c < COLUMNS; c++) {
            metadata.add(new TableColumnMetadata("c" + c, c == 2 ? ColumnType.TIMESTAMP : ColumnType.LONG, IndexType.NONE, 0, false, null));
        }
        dir = java.nio.file.Files.createTempDirectory("qwp-read-ahead").toFile();
        final long columnSize = 8L * frameRows;
        try (Path path = new Path()) {
            for (int f = 0, n = (int) (STREAM_ROWS / frameRows); f < n; f++) {
                final MappedFrame frame = new MappedFrame(frameRows);
                frames.add(frame);
                for (int c = 0; c < COLUMNS; c++) {
                    path.of(dir.getAbsolutePath()).slash().put(f).put('.').put(c).$();
                    final long fd = Files.openRW(path.$());
                    try {
                        Files.truncate(fd, columnSize);
                        final long address = Files.mmap(fd, columnSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                        // write every page, so the column is in the page cache and mapped
                        for (long p = 0; p < columnSize; p += 8) {
                            Unsafe.putLong(address + p, p);
                        }
                        frame.addresses[c] = address;
                    } finally {
                        Files.close(fd);
                    }
                }
            }
        }
        readAhead = new QwpBackwardReadAhead(FilesFacadeImpl.INSTANCE);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        for (int f = 0, n = frames.size(); f < n; f++) {
            final MappedFrame frame = frames.getQuick(f);
            for (int c = 0; c < COLUMNS; c++) {
                Files.munmap(frame.addresses[c], frame.size, MemoryTag.MMAP_DEFAULT);
            }
        }
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
        final long[] addresses = new long[COLUMNS];
        final long rows;
        final long size;

        MappedFrame(long rows) {
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
            return COLUMNS;
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
            return addresses[columnIndex];
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
