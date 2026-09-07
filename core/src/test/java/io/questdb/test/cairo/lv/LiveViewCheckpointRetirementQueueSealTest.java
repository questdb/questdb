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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;

/**
 * The retirement queue IO the checkpoint seal charges to a publication.
 * <p>
 * A publication merges its zero-reference transitions into the durable queue and
 * needs the queue's current image exactly once: {@code persistRetirementQueue}
 * reads it, decides from that read whether the image advances the immediately
 * preceding generation, and hands both the image and that decision to the merge.
 * Reading it a second time inside the merge buys nothing - no writer can
 * interleave, because a live view's refresh latch serialises every publication,
 * sweep and reconciliation over its own checkpoint directory - and costs a file
 * open, an mmap and a full-image CRC32 per seal.
 * <p>
 * These cases pin that shape by counting the opens of {@code _retirements}
 * itself. The queue is written through a temporary sibling
 * ({@code _retirements.tmp}) and renamed, so an open of the final name is a read
 * and nothing else.
 */
public class LiveViewCheckpointRetirementQueueSealTest extends AbstractCairoTest {

    private static final long DEFINITION_TXN = 13;
    // On-disk layout of the package-private LiveViewCheckpointRetirementQueue
    // format, which a test cannot reach by constant: an 8-byte magic, a 4-byte
    // format version at offset 8, a 4-byte entry count at offset 12, an 8-byte
    // generation at offset 16 and an 8-byte live data segment count at offset
    // 24 - 32 bytes of header - then one 4-long entry per retirement and a
    // trailing 4-byte CRC32 over everything before it.
    private static final int COUNT_OFFSET = 12;
    private static final int ENTRY_STRIDE = 4;
    private static final int FORMAT_VERSION = 1;
    private static final int FORMAT_VERSION_OFFSET = 8;
    private static final int GENERATION_OFFSET = 16;
    private static final int HEADER_SIZE = 32;
    private static final long LIFECYCLE_IDENTITY = 501;
    private static final String LV_DIR = "lv_retirement_queue_seal";
    private static final long MAGIC = 0x4C56_5254_5155_0001L;

    @Before
    public void setUp() {
        super.setUp();
        try (Path dir = new Path(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(dir);
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    /**
     * The cold path: the first publication over a directory that holds no queue
     * yet must not open the file at all, because {@code read} gates its open on
     * {@code exists}.
     */
    @Test
    public void testFirstSealOpensNoRetirementQueue() throws Exception {
        final QueueOpenCounter ff = new QueueOpenCounter();
        assertMemoryLeak(ff, () -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                Assert.assertFalse(retirementQueueFile().exists());
                ff.reset();
                final long generation = seal(writer, functions, 1);
                Assert.assertEquals(
                        "an absent queue must cost no open at all",
                        0,
                        ff.openCount
                );
                Assert.assertTrue("the first seal must publish a queue", retirementQueueFile().exists());
                assertQueueHeader(generation);
            }
        });
    }

    /**
     * The rebuild path: a queue whose checksum no longer matches is rebuilt from
     * the freshly published segment directory. The caller's read is the one that
     * condemns it, so the seal still opens the file once, not twice.
     */
    @Test
    public void testSealOverACorruptQueueOpensItOnce() throws Exception {
        final QueueOpenCounter ff = new QueueOpenCounter();
        assertMemoryLeak(ff, () -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, 1);
                seal(writer, functions, 2);
                final File queueFile = retirementQueueFile();
                final long[] before = readQueueEntries();
                Assert.assertTrue(
                        "the queue must hold entries before the rebuild assertion below can say anything",
                        before.length > 0
                );
                // Flip a byte past the header, so magic, format, count and
                // generation all stay intact and only the checksum can catch it.
                flipByte(queueFile, HEADER_SIZE);

                ff.reset();
                final long generation = seal(writer, functions, 3);
                Assert.assertEquals(
                        "a condemned queue must be read by the caller and by nobody else",
                        1,
                        ff.openCount
                );
                // The rebuild seeded from the directory, so the image is whole
                // again and carries this generation.
                assertQueueHeader(generation);
                // The rebuild merges onto the SEED the directory scan built, never
                // onto the list read() emptied when it condemned the image. A merge
                // that took that emptied list as its base would persist this seal's
                // own additions alone; purge() matches on generation, so it would
                // never rebuild, and the dropped segments would never be unlinked.
                assertContainsEveryEntry(before, readQueueEntries());
            }
        });
    }

    /**
     * The steady path, and the one the seal pays on every cadence: a present,
     * current queue must cost one open.
     */
    @Test
    public void testSteadySealOpensTheRetirementQueueOnce() throws Exception {
        final QueueOpenCounter ff = new QueueOpenCounter();
        assertMemoryLeak(ff, () -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, 1);
                seal(writer, functions, 2);
                final long[] before = readQueueEntries();
                Assert.assertTrue(
                        "the queue must hold entries before the assertion below can say anything",
                        before.length > 0
                );

                ff.reset();
                final long generation = seal(writer, functions, 3);
                Assert.assertEquals(
                        "a steady-state seal must read _retirements exactly once: the caller's read"
                                + " already carries both the image and the generation test the merge needs",
                        1,
                        ff.openCount
                );
                assertQueueHeader(generation);

                // The merge is a union by segment id over the image the caller
                // read, so a seal that advances the queue can only add to it.
                // This is what a merge writing over its own input list would
                // break: it would persist the additions alone.
                final long[] after = readQueueEntries();
                Assert.assertTrue(
                        "a seal that advances the queue must carry every entry forward, but the image"
                                + " shrank from " + before.length / ENTRY_STRIDE + " to "
                                + after.length / ENTRY_STRIDE + " entries",
                        after.length >= before.length
                );
                assertContainsEveryEntry(before, after);
            }
        });
    }

    /**
     * Both the steady and the rebuild path in one directory, so a single case
     * pins that the caller's read alone drives both branches of the merge's
     * seed decision.
     */
    @Test
    public void testSteadyAndRebuiltSealsEachOpenTheQueueOnce() throws Exception {
        final QueueOpenCounter ff = new QueueOpenCounter();
        assertMemoryLeak(ff, () -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                long seq = 1;
                seal(writer, functions, seq++);
                for (int i = 0; i < 4; i++) {
                    ff.reset();
                    final long generation = seal(writer, functions, seq++);
                    Assert.assertEquals("seal " + i + " must read the queue once", 1, ff.openCount);
                    assertQueueHeader(generation);
                }
                // Truncated tail: shorter than header plus trailing checksum, so
                // the size guard condemns the file before anything is mapped and
                // the rebuilding seal opens it not even once.
                final long[] beforeTruncate = readQueueEntries();
                Assert.assertTrue(
                        "the queue must hold entries before the rebuild assertion below can say anything",
                        beforeTruncate.length > 0
                );
                truncateFile(retirementQueueFile(), 10);
                ff.reset();
                final long rebuilt = seal(writer, functions, seq++);
                Assert.assertEquals(
                        "a queue too short to hold a header must cost no open",
                        0,
                        ff.openCount
                );
                assertQueueHeader(rebuilt);
                // read() empties its output list on every failure, this size guard
                // included, so the rebuild's merge base has to be the directory seed.
                // Merging onto that emptied list would carry nothing forward.
                assertContainsEveryEntry(beforeTruncate, readQueueEntries());
                ff.reset();
                final long resumed = seal(writer, functions, seq);
                Assert.assertEquals("the seal after a rebuild must read the queue once", 1, ff.openCount);
                assertQueueHeader(resumed);
            }
        });
    }

    /**
     * Asserts that every entry of {@code before} survives into {@code after}
     * whole. A carried-forward entry keeps its file length, its retire generation
     * and its kind as well as its id: the purge unlinks by id but sizes and
     * classifies the work from the other three, so an entry that arrives with a
     * different payload is as wrong as one that never arrived.
     */
    private static void assertContainsEveryEntry(long[] before, long[] after) {
        for (int i = 0; i < before.length; i += ENTRY_STRIDE) {
            boolean found = false;
            for (int j = 0; j < after.length; j += ENTRY_STRIDE) {
                if (after[j] == before[i]) {
                    found = true;
                    Assert.assertEquals(
                            "the merged queue changed the file length of segment id " + before[i],
                            before[i + 1],
                            after[j + 1]
                    );
                    Assert.assertEquals(
                            "the merged queue changed the retire generation of segment id " + before[i],
                            before[i + 2],
                            after[j + 2]
                    );
                    Assert.assertEquals(
                            "the merged queue changed the kind of segment id " + before[i],
                            before[i + 3],
                            after[j + 3]
                    );
                    break;
                }
            }
            Assert.assertTrue("the merged queue dropped segment id " + before[i], found);
        }
    }

    private static void assertQueueHeader(long generation) throws IOException {
        final File file = retirementQueueFile();
        Assert.assertTrue("a seal must publish a retirement queue", file.exists());
        final ByteBuffer image = image(file);
        Assert.assertEquals("magic", MAGIC, image.getLong(0));
        Assert.assertEquals("format version", FORMAT_VERSION, image.getInt(FORMAT_VERSION_OFFSET));
        final int count = image.getInt(COUNT_OFFSET);
        Assert.assertTrue("entry count must not be negative", count >= 0);
        Assert.assertEquals(
                "the queue must carry the generation the seal published",
                generation,
                image.getLong(GENERATION_OFFSET)
        );
        Assert.assertEquals(
                "the image must be exactly header plus entries plus checksum",
                HEADER_SIZE + (long) count * ENTRY_STRIDE * Long.BYTES + Integer.BYTES,
                image.capacity()
        );
    }

    private static Path checkpointsDir(Path sink) {
        return sink.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static void flipByte(File file, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            final int value = raf.read();
            Assert.assertTrue("corruption offset must be inside the file", value >= 0);
            raf.seek(offset);
            raf.write(value ^ 0xFF);
        }
    }

    private static ByteBuffer image(File file) throws IOException {
        return ByteBuffer.wrap(Files.readAllBytes(file.toPath())).order(ByteOrder.nativeOrder());
    }

    private static long[] readQueueEntries() throws IOException {
        final ByteBuffer image = image(retirementQueueFile());
        final int count = image.getInt(COUNT_OFFSET);
        final long[] entries = new long[count * ENTRY_STRIDE];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = image.getLong(HEADER_SIZE + i * Long.BYTES);
        }
        return entries;
    }

    private static File retirementQueueFile() {
        try (Path path = new Path(); Path dir = new Path()) {
            return new File(LiveViewCheckpointLayout.retirementQueuePath(path, checkpointsDir(dir)).toString());
        }
    }

    private static long seal(
            LiveViewCheckpointTimelineStoreWriter writer,
            ObjList<WindowFunction> functions,
            long seq
    ) {
        try (Path dir = new Path()) {
            checkpointsDir(dir);
            return writer.append(
                    dir,
                    functions,
                    null,
                    DEFINITION_TXN,
                    0,
                    seq,
                    seq,
                    0,
                    LIFECYCLE_IDENTITY,
                    true,
                    seq * 1_000_000L,
                    seq,
                    seq * 1_000_000L,
                    Numbers.LONG_NULL,
                    null,
                    null
            ).getGeneration();
        }
    }

    private static void truncateFile(File file, long newLength) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(newLength);
        }
    }

    /**
     * Counts every open of the retirement queue's final name. The queue is
     * mapped read-write for both reading and writing, and the write goes to
     * {@code _retirements.tmp} first, so an open of {@code _retirements} is
     * always a read. Read-only opens count too, so the assertion survives a
     * future switch to a read-only mapping.
     */
    private static final class QueueOpenCounter extends TestFilesFacadeImpl {
        private int openCount;

        @Override
        public long openRO(LPSZ name) {
            count(name);
            return super.openRO(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            count(name);
            return super.openRW(name, opts);
        }

        private void count(LPSZ name) {
            if (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME)) {
                openCount++;
            }
        }

        private void reset() {
            openCount = 0;
        }
    }

    /**
     * A scalar (map-less) whole-state function, so a seal walks the production
     * publication path over a fixed, tiny state image.
     */
    private static final class ScalarStateStub extends BaseWindowFunction {

        private ScalarStateStub() {
            super(null);
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "retirement_queue_seal_stub()",
                            0,
                            "",
                            "ts asc",
                            "retirement-queue-seal-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.UNBOUNDED_CUMULATIVE_NO_RESET,
                            "",
                            "ts asc",
                            Long.MIN_VALUE,
                            0,
                            Long.MIN_VALUE,
                            ColumnType.TIMESTAMP,
                            false,
                            false,
                            false,
                            LiveViewCheckpointDependency.StructuralConvergence.EXACT,
                            LiveViewCheckpointDependency.NumericConvergence.EXACT
                    )
            );
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(0);
        }

        @Override
        public String getName() {
            return "retirement_queue_seal_stub";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            return Long.BYTES;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }
}
