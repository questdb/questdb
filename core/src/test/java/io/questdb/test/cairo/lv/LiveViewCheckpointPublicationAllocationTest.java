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

package io.questdb.test.cairo.lv;

import com.sun.management.ThreadMXBean;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;

/**
 * Java-heap allocation profiling for the checkpoint publication entry points.
 * <p>
 * The remediation's decisive gate is that a warmed-up publication creates no
 * garbage: it runs on shells the writer retains rather than on an object graph it
 * builds per call, and it must not depend on escape analysis to say so. These
 * cases measure {@code append} through the production entry point - the reader,
 * writer, store, root, directory, page reference, id list, timeline entry, result
 * and path it needs are all inside the measurement - and assert the per-seal
 * charge stays inside a bound far below what one per-call object graph costs.
 * <p>
 * The bound is deliberately not zero. {@code ThreadMXBean} samples the thread's
 * TLAB accounting rather than the operation's own allocations, and the file
 * system, logging and JIT layers under the seal contribute a small, cardinality-
 * independent baseline. What the assertion pins is the shape: the charge must not
 * grow with the key set, and it must not grow with the number of seals.
 */
public class LiveViewCheckpointPublicationAllocationTest extends AbstractCairoTest {

    private static final long DEFINITION_TXN = 11;
    private static final long LIFECYCLE_IDENTITY = 401;
    private static final ColumnTypes LONG_KEY_TYPES = new SingleColumnType(ColumnType.LONG);
    private static final String LV_DIR = "lv_publication_allocation";
    private static final int MEASURED_SEALS = 16;
    /**
     * Per-seal ceiling, roughly 1.4x what the retained-shell path measures. One
     * per-call publication object graph - the stores, readers, writers, roots,
     * directories, builders and their paths - costs several times this before a
     * single key is imaged, and the per-key graph this replaced cost more than
     * this per hundred keys, so either regression lands above the bound while the
     * steady state stays comfortably inside it.
     */
    private static final long PER_SEAL_ALLOCATION_LIMIT_BYTES = 6_144;
    private static final ColumnTypes STRING_KEY_TYPES = new SingleColumnType(ColumnType.STRING);
    private static final int WARMUP_SEALS = 16;
    private static final int WIDTH_CHURN_KEYS = 64;

    @Before
    public void setUp() {
        super.setUp();
        createCheckpointLayout();
    }

    @Test
    public void testASealOfOneKeyAllocatesNothingPerSealOnceWarm() throws Exception {
        assertSealAllocationIsBounded(1);
    }

    @Test
    public void testASealOfAThousandKeysAllocatesNothingPerSealOnceWarm() throws Exception {
        assertSealAllocationIsBounded(1_000);
    }

    @Test
    public void testASealJustInsideTheFrozenRetentionLimitAllocatesNothingPerSealOnceWarm() throws Exception {
        // The writer hands an outlier seal's frozen graph back when the seal ends. Each key is
        // one frozen key and one inline state array, which the limit counts together, so this
        // key set sits just inside it and its steady state must stay as garbage-free as a
        // small one's.
        assertSealAllocationIsBounded(LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES / 2 - 1_024);
    }

    @Test
    public void testASealJustOutsideTheFrozenRetentionLimitHandsItsFrozenGraphBack() throws Exception {
        // One frozen key and one inline state array per key puts this key set just past the
        // limit, although its arrays alone stay far inside it: the keys count too, since each
        // grows the frozen holders as a pooled key array once did.
        assertSealHandsItsFrozenGraphBack(LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES / 2 + 1_024, false);
    }

    @Test
    public void testAPageBackedSealJustInsideTheStatePageRefLimitAllocatesNothingPerSealOnceWarm() throws Exception {
        // An inline key takes a frozen key and an image array, so an inline seal hands its
        // graph back above half the array limit. A page-backed key takes the frozen key and
        // one state page reference but no image array, which kept such a seal warm up to the
        // reference limit while every key was a pooled array, and it must stay warm there.
        final int keyCount = LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_STATE_PAGE_REFS - 1_024;
        Assert.assertTrue(keyCount > LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES / 2);
        assertSealAllocationIsBounded(keyCount, true);
    }

    @Test
    public void testAPageBackedSealJustOutsideTheStatePageRefLimitHandsItsFrozenGraphBack() throws Exception {
        // Above the reference limit, which is also where its frozen keys alone pass the array
        // limit, the writer - one worker's, shared by every view it seals - must hand the
        // seal's frozen holders back rather than park them for the worker's lifetime.
        assertSealHandsItsFrozenGraphBack(LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_STATE_PAGE_REFS + 1_024, true);
    }

    @Test
    public void testASealWhoseKeyWidthsMoveEverySealAllocatesNothingPerKey() throws Exception {
        // Every seal here images keys of widths no earlier seal saw. A frozen key that is a
        // heap array comes from a pool that keeps one array per width, so such a seal would
        // allocate an array per key however warm the writer is; a key frozen into native
        // memory costs the heap nothing whatever its width.
        assertMemoryLeak(() -> {
            final StringSink sink = new StringSink();
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(STRING_KEY_TYPES, false);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                long seq = 1;
                int widthStep = 0;
                for (int i = 0; i < WARMUP_SEALS; i++) {
                    stub.replaceStringKeys(WIDTH_CHURN_KEYS, widthStep++ * WIDTH_CHURN_KEYS + 1, sink);
                    seal(writer, functions, seq++);
                }
                final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
                final long threadId = Thread.currentThread().threadId();
                long allocated = 0;
                try (Path dir = new Path()) {
                    checkpointsDir(dir);
                    for (int i = 0; i < MEASURED_SEALS; i++) {
                        // Outside the measurement: the stub's own map and strings are not the seal's.
                        stub.replaceStringKeys(WIDTH_CHURN_KEYS, widthStep++ * WIDTH_CHURN_KEYS + 1, sink);
                        final long before = threadMXBean.getThreadAllocatedBytes(threadId);
                        append(writer, dir, functions, seq++);
                        allocated += threadMXBean.getThreadAllocatedBytes(threadId) - before;
                    }
                }
                final long perSeal = allocated / MEASURED_SEALS;
                Assert.assertTrue(
                        "a warmed-up seal over " + WIDTH_CHURN_KEYS + " keys of widths no earlier seal saw"
                                + " allocated " + perSeal + " bytes on the Java heap (" + allocated + " over "
                                + MEASURED_SEALS + " seals); a frozen key must not be a heap array",
                        perSeal < PER_SEAL_ALLOCATION_LIMIT_BYTES
                );
            }
        });
    }

    @Test
    public void testSealAllocationDoesNotGrowWithSealCount() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(64);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                long seq = 1;
                for (int i = 0; i < WARMUP_SEALS; i++) {
                    seal(writer, functions, seq++);
                }
                final long first = measureSeals(writer, functions, seq);
                seq += MEASURED_SEALS;
                final long second = measureSeals(writer, functions, seq);
                Assert.assertTrue(
                        "the second measured run allocated " + second + " bytes against the first's " + first
                                + "; a warmed-up seal must not cost more the longer the writer lives",
                        second <= first + PER_SEAL_ALLOCATION_LIMIT_BYTES * MEASURED_SEALS
                );
            }
        });
    }

    private static void createCheckpointLayout() {
        try (Path dir = new Path(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(dir);
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    private static Path checkpointsDir(Path sink) {
        return sink.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private void assertSealAllocationIsBounded(int keyCount) throws Exception {
        assertSealAllocationIsBounded(keyCount, false);
    }

    private void assertSealAllocationIsBounded(int keyCount, boolean isPageBacked) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(keyCount, isPageBacked);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                long seq = 1;
                // The pools, the arenas and the maps all reach their high-water mark
                // here; what the measurement below sees is the steady state.
                for (int i = 0; i < WARMUP_SEALS; i++) {
                    seal(writer, functions, seq++);
                }
                final long allocated = measureSeals(writer, functions, seq);
                final long perSeal = allocated / MEASURED_SEALS;
                Assert.assertTrue(
                        "a warmed-up seal over " + keyCount + " keys allocated " + perSeal
                                + " bytes on the Java heap (" + allocated + " over " + MEASURED_SEALS
                                + " seals); a publication runs on retained shells and must not build"
                                + " an object graph per call",
                        perSeal < PER_SEAL_ALLOCATION_LIMIT_BYTES
                );
            }
        });
    }

    private void assertSealHandsItsFrozenGraphBack(int keyCount, boolean isPageBacked) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(keyCount, isPageBacked);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, 1);
                // A graph kept warm holds at least one partition holder per key.
                final int retained = writer.getRetainedFrozenObjectCountForTest();
                Assert.assertTrue(
                        "a seal of " + keyCount + " keys above the frozen retention limit must not park its"
                                + " frozen graph on the writer, retained=" + retained,
                        retained < keyCount
                );
            }
        });
    }

    private long measureSeals(
            LiveViewCheckpointTimelineStoreWriter writer,
            ObjList<WindowFunction> functions,
            long firstSeq
    ) {
        final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long threadId = Thread.currentThread().threadId();
        try (Path dir = new Path()) {
            checkpointsDir(dir);
            long seq = firstSeq;
            final long before = threadMXBean.getThreadAllocatedBytes(threadId);
            for (int i = 0; i < MEASURED_SEALS; i++) {
                append(writer, dir, functions, seq++);
            }
            return threadMXBean.getThreadAllocatedBytes(threadId) - before;
        }
    }

    private void append(
            LiveViewCheckpointTimelineStoreWriter writer,
            Path checkpointsDir,
            ObjList<WindowFunction> functions,
            long seq
    ) {
        writer.append(
                checkpointsDir,
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
                null
        );
    }

    private void seal(LiveViewCheckpointTimelineStoreWriter writer, ObjList<WindowFunction> functions, long seq) {
        try (Path dir = new Path()) {
            checkpointsDir(dir);
            append(writer, dir, functions, seq);
        }
    }

    /**
     * A partitioned function holding {@code keyCount} live keys, so a seal walks a
     * real key domain rather than a single entry. Its eight-byte state is inlined into
     * the leaf, or written to a data page when the stub is page-backed.
     */
    private static final class PartitionedStateStub extends BaseWindowFunction {
        private final boolean isPageBacked;
        private final ColumnTypes keyTypes;
        private final Map map;

        private PartitionedStateStub(int keyCount) {
            this(keyCount, false);
        }

        private PartitionedStateStub(int keyCount, boolean isPageBacked) {
            this(LONG_KEY_TYPES, isPageBacked);
            for (int i = 0; i < keyCount; i++) {
                final MapKey mapKey = map.withKey();
                mapKey.putLong(i);
                mapKey.createValue().putLong(0, i);
            }
        }

        private PartitionedStateStub(ColumnTypes keyTypes, boolean isPageBacked) {
            super(null);
            this.keyTypes = keyTypes;
            this.isPageBacked = isPageBacked;
            this.map = new OrderedMap(
                    1024,
                    keyTypes,
                    new SingleColumnType(ColumnType.LONG),
                    16,
                    0.7,
                    Integer.MAX_VALUE
            );
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "publication_allocation_stub()",
                            0,
                            "k",
                            "ts asc",
                            "publication-allocation-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.FIXED_ANCHOR_SEGMENT,
                            "k",
                            "ts asc",
                            0,
                            0,
                            0,
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
        public int checkpointStateFixedLength() {
            // Undeclared keeps the state page-backed; eight bytes inlines it into the leaf.
            return isPageBacked ? -1 : Long.BYTES;
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(value.getLong(0));
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return keyTypes;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            return 1;
        }

        @Override
        public String getName() {
            return "publication_allocation_stub";
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            map.clear();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            value.putLong(0, source.getLong(offset));
            return Long.BYTES;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }

        /**
         * Replaces the key set with {@code keyCount} STRING keys of {@code firstChars} to
         * {@code firstChars + keyCount - 1} characters, one width each.
         */
        private void replaceStringKeys(int keyCount, int firstChars, StringSink sink) {
            map.clear();
            for (int i = 0; i < keyCount; i++) {
                sink.clear();
                for (int c = 0, n = firstChars + i; c < n; c++) {
                    sink.put((char) ('a' + (c + i) % 26));
                }
                final MapKey mapKey = map.withKey();
                mapKey.putStr(sink);
                mapKey.createValue().putLong(0, i);
            }
        }
    }
}
