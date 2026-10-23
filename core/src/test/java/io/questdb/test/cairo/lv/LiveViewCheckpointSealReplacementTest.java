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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A cadence seal over a head whose timestamp group grew after it was sealed.
 * <p>
 * Such a head holds only the part of its group the runtime had consumed by then,
 * and a resume from it replays only the rows above its timestamp, so it would never
 * fold the rest. The batch minimum the seal receives records the growth: it sits at
 * or below the head's timestamp. The seal then publishes its boundary in the head's
 * place, in one generation, which keeps every root below the newest complete.
 * <p>
 * These cases drive the writer directly and check the published generation: the
 * timeline's entries, the superblock's logical state and retired-root totals, and
 * that every data and boundary segment counts exactly the surviving roots that name
 * it.
 */
public class LiveViewCheckpointSealReplacementTest extends AbstractCairoTest {

    private static final long DEFINITION_TXN = 17;
    private static final long LIFECYCLE_IDENTITY = 503;
    private static final String LV_DIR = "lv_seal_replacement";
    private long seq;

    @Before
    public void setUp() {
        super.setUp();
        seq = 0;
        try (Path dir = new Path(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(dir);
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    @Test
    public void testAFailedReplacementLeavesTheGenerationAsItWas() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, second(1), second(1));
                final long tiedId = seal(writer, functions, second(2), second(2)).getCheckpointId();
                final long generation = currentGeneration();

                writer.setTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                try {
                    seal(writer, functions, second(3), second(2));
                    Assert.fail("the seal must fail at the injected stage");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("test failure after live view checkpoint metadata publication"));
                }
                Assert.assertEquals(generation, currentGeneration());
                assertTimeline(second(1), second(2));
                assertRetiredCheckpointCount(0);
                assertCatalogueCountsSurvivingRoots();

                // The retry reconciles away what the failed attempt staged, and replaces the head.
                writer.setTestFailureStage(0);
                Assert.assertEquals(tiedId, seal(writer, functions, second(3), second(2)).getDroppedCheckpointId());
                assertTimeline(second(1), second(3));
                assertRetiredCheckpointCount(1);
                assertCatalogueCountsSurvivingRoots();
            }
        });
    }

    @Test
    public void testASealAboveTheHeadKeepsIt() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                Assert.assertEquals(Numbers.LONG_NULL, seal(writer, functions, second(1), second(1)).getDroppedCheckpointId());
                Assert.assertEquals(Numbers.LONG_NULL, seal(writer, functions, second(2), second(2)).getDroppedCheckpointId());
                // One microsecond above the head is enough: nothing the runtime fed since sits on it.
                Assert.assertEquals(Numbers.LONG_NULL, seal(writer, functions, second(3), second(2) + 1).getDroppedCheckpointId());
                assertTimeline(second(1), second(2), second(3));
                assertRetiredCheckpointCount(0);
                assertCatalogueCountsSurvivingRoots();
            }
        });
    }

    @Test
    public void testASealOverAGrownHeadReplacesIt() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, second(1), second(1));
                final long tiedId = seal(writer, functions, second(2), second(2)).getCheckpointId();
                // The runtime fed another row at 2s after that seal, then reached 3s.
                final LiveViewCheckpointTimelineStoreWriter.Result replaced = seal(writer, functions, second(3), second(2));
                Assert.assertEquals(tiedId, replaced.getDroppedCheckpointId());
                Assert.assertNotEquals(tiedId, replaced.getCheckpointId());
                assertTimeline(second(1), second(3));
                assertRetiredCheckpointCount(1);
                assertCatalogueCountsSurvivingRoots();
            }
        });
    }

    @Test
    public void testASealOverTheOnlyRootReplacesIt() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                final long firstId = seal(writer, functions, second(1), second(1)).getCheckpointId();
                Assert.assertEquals(firstId, seal(writer, functions, second(2), second(1)).getDroppedCheckpointId());
                assertTimeline(second(2));
                assertRetiredCheckpointCount(1);
                assertCatalogueCountsSurvivingRoots();
            }
        });
    }

    @Test
    public void testASealWithAnUnknownBatchMinimumKeepsTheHead() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, second(1), second(1));
                // The corrupt-root heal seals with no minimum, right after rebuilding the head
                // from the base table, so the head it seals over is complete.
                Assert.assertEquals(Numbers.LONG_NULL, seal(writer, functions, second(2), Numbers.LONG_NULL).getDroppedCheckpointId());
                assertTimeline(second(1), second(2));
                assertRetiredCheckpointCount(0);
                assertCatalogueCountsSurvivingRoots();
            }
        });
    }

    @Test
    public void testATieOnlySealIsStillSkipped() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, second(1), second(1));
                seal(writer, functions, second(2), second(2));
                final long generation = currentGeneration();
                try {
                    seal(writer, functions, second(2), second(2));
                    Assert.fail("a seal on the head's own timestamp must be skipped");
                } catch (LiveViewCheckpointTimelineStoreWriter.BoundaryNotAboveHeadException ignore) {
                }
                Assert.assertEquals(generation, currentGeneration());
                assertTimeline(second(1), second(2));
                assertRetiredCheckpointCount(0);
            }
        });
    }

    @Test
    public void testManySealsKeepEverySegmentCountingItsSurvivingRoots() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                final LongList expected = new LongList();
                int replacements = 0;
                for (int i = 1; i <= 150; i++) {
                    // Every third seal follows a tie on the head, and every seventh a repair
                    // whose replay lowered the minimum well below it. Either way the head goes,
                    // and only the head.
                    final long batchMinTs;
                    if (i > 1 && i % 7 == 0) {
                        batchMinTs = second(i - 5);
                    } else if (i > 1 && i % 3 == 0) {
                        batchMinTs = second(i - 1);
                    } else {
                        batchMinTs = second(i);
                    }
                    final long dropped = seal(writer, functions, second(i), batchMinTs).getDroppedCheckpointId();
                    if (batchMinTs < second(i)) {
                        Assert.assertNotEquals("seal " + i + " must replace the head", Numbers.LONG_NULL, dropped);
                        expected.removeIndex(expected.size() - 1);
                        replacements++;
                    } else {
                        Assert.assertEquals("seal " + i + " must keep the head", Numbers.LONG_NULL, dropped);
                    }
                    expected.add(second(i));
                }
                assertTimeline(expected);
                assertRetiredCheckpointCount(replacements);
                assertCatalogueCountsSurvivingRoots();
            }
        });
    }

    @Test
    public void testOnlyTheHeadGoesWhenTheBatchMinimumFellBelowIt() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                final ObjList<WindowFunction> functions = new ObjList<>();
                functions.add(stub);
                seal(writer, functions, second(1), second(1));
                final long headId = seal(writer, functions, second(2), second(2)).getCheckpointId();
                // A repair's replay fed rows from below the root at 1s. Only a row in-order on
                // the head's own timestamp can grow a root's group, so the roots below the head
                // stay; the head goes because nothing tells the seal what lowered the minimum.
                Assert.assertEquals(headId, seal(writer, functions, second(3), second(1) / 2).getDroppedCheckpointId());
                assertTimeline(second(1), second(3));
                assertRetiredCheckpointCount(1);
                assertCatalogueCountsSurvivingRoots();
            }
        });
    }

    private static Path checkpointsDir(Path sink) {
        return sink.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static long second(long s) {
        return s * 1_000_000L;
    }

    /**
     * Each data and boundary segment counts the surviving roots that name it, and no
     * surviving root names a segment the catalogue lacks. A replacement that released the
     * head's references twice, or not at all, or released one the new root reuses, breaks
     * this.
     */
    private void assertCatalogueCountsSurvivingRoots() {
        final Map<Long, Integer> expectedCounts = new HashMap<>();
        final Map<Long, Long> catalogued = new HashMap<>();
        try (
                Path dir = new Path();
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            checkpointsDir(dir);
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointSegmentDirectoryReader directory =
                            new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                timeline.of(dir);
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    root.of(dir, entry.rootRef);
                    final Set<Long> named = new HashSet<>();
                    for (int i = 0, n = root.getSegmentIdCount(); i < n; i++) {
                        named.add(root.getSegmentId(i));
                    }
                    for (long segmentId : named) {
                        expectedCounts.merge(segmentId, 1, Integer::sum);
                    }
                });
                directory.of(dir, pin.getSegmentDirectoryRootRef());
                directory.iterateAll(entry -> {
                    if (entry.kind != LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META) {
                        catalogued.put(entry.segmentId, entry.referenceCount);
                    }
                });
            }
        }
        for (Map.Entry<Long, Integer> e : expectedCounts.entrySet()) {
            Assert.assertEquals(
                    "segment " + e.getKey() + " must count every surviving root that names it",
                    Long.valueOf(e.getValue()),
                    catalogued.get(e.getKey())
            );
        }
        for (Map.Entry<Long, Long> e : catalogued.entrySet()) {
            if (!expectedCounts.containsKey(e.getKey())) {
                Assert.assertEquals("segment " + e.getKey() + " no surviving root names must count none", 0L, (long) e.getValue());
            }
        }
    }

    private void assertRetiredCheckpointCount(long expected) {
        try (
                Path dir = new Path();
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            checkpointsDir(dir);
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            Assert.assertEquals(expected, metaStore.getSuperblock().retiredCheckpointCount);
        }
    }

    /**
     * The published timeline holds exactly the roots at {@code expectedMaxTimestamps}, in
     * order, and the superblock's logical state total is theirs alone.
     */
    private void assertTimeline(long... expectedMaxTimestamps) {
        final LongList expected = new LongList();
        for (long ts : expectedMaxTimestamps) {
            expected.add(ts);
        }
        assertTimeline(expected);
    }

    private void assertTimeline(LongList expectedMaxTimestamps) {
        final LongList maxTimestamps = new LongList();
        final long[] logicalStateBytes = new long[1];
        try (
                Path dir = new Path();
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            checkpointsDir(dir);
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            timeline.of(dir);
            try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    maxTimestamps.add(entry.maxTimestamp);
                    logicalStateBytes[0] += entry.logicalStateBytes;
                });
            }
            Assert.assertEquals(logicalStateBytes[0], metaStore.getSuperblock().logicalStateBytes);
        }
        Assert.assertEquals(expectedMaxTimestamps, maxTimestamps);
    }

    private long currentGeneration() {
        try (
                Path dir = new Path();
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            checkpointsDir(dir);
            metaStore.of(dir);
            Assert.assertTrue("the generation must be readable", metaStore.isValid());
            return metaStore.getSuperblock().generation;
        }
    }

    private LiveViewCheckpointTimelineStoreWriter.Result seal(
            LiveViewCheckpointTimelineStoreWriter writer,
            ObjList<WindowFunction> functions,
            long maxTimestamp,
            long batchMinTs
    ) {
        seq++;
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
                    maxTimestamp,
                    seq,
                    batchMinTs,
                    Numbers.LONG_NULL,
                    null
            );
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
                            "seal_replacement_stub()",
                            0,
                            "",
                            "ts asc",
                            "seal-replacement-stub-v1"
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
            return "seal_replacement_stub";
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
