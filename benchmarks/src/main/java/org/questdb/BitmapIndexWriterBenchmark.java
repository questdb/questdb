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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Measures what a bitmap index writer spends to open an index and to add keys, at the server's default key append page
 * of 512 KiB. {@code reopenAfterRollback} opens the index as a rollback with rows in flight leaves it, the key file not
 * cut since the last writer closed, so it ends up to a key append page past the last entry; {@code reopenAfterClose}
 * opens it after a clean close, which cuts the key file after the page of the last entry. Both then add a new key to
 * the index of 20_001 keys and close the writer as they found it. {@code reopenAfterRollbackPastSlack} opens an
 * index a rollback has just left, every time, and adds a key 12_288 keys past the last one, so that add() checks the
 * 384 KiB of entries it skips in the space the key file keeps past the last entry: with {@code isSlackCached}, a read
 * of the whole key file has left that space in the page cache. The add benchmarks add a batch of 10_000 keys to a new
 * index: dense new keys, sparse new keys that each skip a key of a key file that holds zeros past the last entry, and
 * values of existing keys. The score is the cost of one open or one add.
 * <p>
 * Build and run (note {@code -am} so the benchmark links the in-tree core). The indexes live in a directory under
 * {@code java.io.tmpdir}; add {@code -jvmArgsAppend -Djava.io.tmpdir=<dir>} to measure another file system:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *      --enable-native-access=ALL-UNNAMED -cp benchmarks/target/benchmarks.jar \
 *      org.questdb.BitmapIndexWriterBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class BitmapIndexWriterBenchmark {
    private static final int BATCH_KEYS = 10_000;
    private static final int BLOCK_VALUE_COUNT = 4;
    private static final int KEY_COUNT = 20_001;
    private static final String NAME = "x";
    // 384 KiB of entries: KEY_COUNT keys end at 640_096 in a key file of 1 MiB
    private static final int SLACK_SKIPPED_KEYS = 12_288;
    private int nextKey;
    private Path path;
    private String root;
    private BitmapIndexWriter writer;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BitmapIndexWriterBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_KEYS)
    public void addDenseNewKeys() {
        writer.of(path, NAME, COLUMN_NAME_TXN_NONE, BLOCK_VALUE_COUNT);
        for (int key = 0; key < BATCH_KEYS; key++) {
            writer.add(key, key);
        }
        writer.close();
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_KEYS)
    public void addSparseNewKeys() {
        // a new index whose key file keeps the key append page the writer mapped, holding zeros
        writer.of(path, NAME, COLUMN_NAME_TXN_NONE, BLOCK_VALUE_COUNT);
        writer.closeNoTruncate();
        writer.of(path, NAME, COLUMN_NAME_TXN_NONE);
        for (int i = 0; i < BATCH_KEYS; i++) {
            writer.add(2 * i + 1, i);
        }
        writer.close();
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_KEYS)
    public void addToExistingKeys() {
        writer.of(path, NAME, COLUMN_NAME_TXN_NONE, BLOCK_VALUE_COUNT);
        for (int i = 0; i < BATCH_KEYS; i++) {
            writer.add(i % 100, i);
        }
        writer.close();
    }

    @Benchmark
    public void reopenAfterClose() {
        writer.of(path, NAME, COLUMN_NAME_TXN_NONE);
        writer.add(nextKey, nextKey);
        writer.setMaxValue(nextKey++);
        writer.close();
    }

    @Benchmark
    public void reopenAfterRollback() {
        writer.of(path, NAME, COLUMN_NAME_TXN_NONE);
        writer.add(nextKey, nextKey);
        writer.setMaxValue(nextKey++);
        writer.closeNoTruncate();
    }

    @Benchmark
    public void reopenAfterRollbackPastSlack(RollbackSlack slack) {
        final BitmapIndexWriter slackWriter = slack.writer;
        slackWriter.of(slack.path, NAME, COLUMN_NAME_TXN_NONE);
        slackWriter.add(KEY_COUNT + SLACK_SKIPPED_KEYS, KEY_COUNT);
        slackWriter.setMaxValue(KEY_COUNT);
        slackWriter.closeNoTruncate();
    }

    @Setup(Level.Iteration)
    public void setUpIndex() {
        // KEY_COUNT keys of one value each, as a writer that closes cleanly leaves them
        writer.of(path, NAME, COLUMN_NAME_TXN_NONE, BLOCK_VALUE_COUNT);
        for (int key = 0; key < KEY_COUNT; key++) {
            writer.add(key, key);
        }
        writer.setMaxValue(KEY_COUNT - 1);
        writer.close();
        nextKey = KEY_COUNT;
    }

    @Setup(Level.Trial)
    public void setUpWriter() throws IOException {
        root = Files.createTempDirectory("bitmap-index-writer-bench").toString();
        final CairoConfiguration configuration = newConfiguration(root);
        path = new Path().of(root);
        writer = new BitmapIndexWriter(configuration);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        writer = Misc.free(writer);
        path = Misc.free(path);
        removeRecursively(root);
    }

    private static CairoConfiguration newConfiguration(String root) {
        return new DefaultCairoConfiguration(root) {
            @Override
            public long getDataIndexKeyAppendPageSize() {
                return 512 * 1024;
            }
        };
    }

    private static void removeRecursively(String root) throws IOException {
        try (Stream<java.nio.file.Path> files = Files.walk(new File(root).toPath())) {
            files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    // An index of its own for reopenAfterRollbackPastSlack(), built again before every invocation: an invocation adds
    // a key past the slack, which leaves the next one no slack to skip
    @State(Scope.Thread)
    public static class RollbackSlack {
        @Param({"false", "true"})
        public boolean isSlackCached;
        private FilesFacade ff;
        private Path path;
        private String root;
        private BitmapIndexWriter writer;

        @Setup(Level.Invocation)
        public void setUpIndex(Blackhole blackhole) {
            // KEY_COUNT keys of one value each, closed as a rollback with rows in flight closes them: the key file ends
            // a key append page past the start of the page it grew in last, 1 MiB here
            writer.of(path, NAME, COLUMN_NAME_TXN_NONE, BLOCK_VALUE_COUNT);
            for (int key = 0; key < KEY_COUNT; key++) {
                writer.add(key, key);
            }
            writer.setMaxValue(KEY_COUNT - 1);
            writer.closeNoTruncate();
            if (isSlackCached) {
                final int plen = path.size();
                final long fd = ff.openRO(BitmapIndexUtils.keyFileName(path, NAME, COLUMN_NAME_TXN_NONE));
                path.trimTo(plen);
                final long length = ff.length(fd);
                final long address = ff.mmap(fd, length, 0, io.questdb.std.Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                long sum = 0;
                for (long offset = 0; offset < length; offset += io.questdb.std.Files.PAGE_SIZE) {
                    sum += Unsafe.getLong(address + offset);
                }
                blackhole.consume(sum);
                ff.munmap(address, length, MemoryTag.MMAP_DEFAULT);
                ff.close(fd);
            }
        }

        @Setup(Level.Trial)
        public void setUpWriter() throws IOException {
            root = Files.createTempDirectory("bitmap-index-writer-slack-bench").toString();
            final CairoConfiguration configuration = newConfiguration(root);
            ff = configuration.getFilesFacade();
            path = new Path().of(root);
            writer = new BitmapIndexWriter(configuration);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            writer = Misc.free(writer);
            path = Misc.free(path);
            removeRecursively(root);
        }
    }
}
