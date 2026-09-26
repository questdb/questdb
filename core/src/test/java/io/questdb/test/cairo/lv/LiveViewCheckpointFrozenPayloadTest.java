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
import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSealState;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.TreeMap;

/**
 * What a warm seal of an anchored window's fused state costs the Java heap, driven through
 * the production {@code append} with a SQL view's own compiled window and functions.
 * <p>
 * A seal images one payload per key: the fused window entry, and one member image per
 * runtime-only member of the group. Those payloads live in the freeze scratch's native
 * arena, which the seal frees when it ends, and the heap keeps only the handles and holders
 * the writer pools across seals. So a warm seal allocates nothing on the heap that scales
 * with its key set, however wide the payloads are: in particular a seal whose payloads
 * together pass the bytes a pooled heap image could keep must not hand its holders back and
 * re-image every key on the heap.
 * <p>
 * Each measured seal is complete - the window is told to owe one before every seal, and a
 * RANGE ring's state is frozen complete at every seal anyway - so every key is walked, imaged
 * and probed against the root below it. The seals go into a
 * timeline of their own, under a writer of their own, beside the one the refresh job keeps,
 * and every measurement starts a fresh one: a root that the seals of several key sets built
 * up spans a segment per set, and what reading it costs follows those segments rather than
 * the key set a seal images.
 */
public class LiveViewCheckpointFrozenPayloadTest extends AbstractLiveViewTest {

    // 'acct-1' as a checkpoint key encodes it: a SYMBOL key freezes as a STRING, a
    // little-endian length and then UTF-16LE characters.
    private static final String ACCT_1_KEY_HEX = "0600000061006300630074002d003100";
    private static final String DAILY_WINDOW = " WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')";
    private static final long DEFINITION_TXN = 23;
    // The last of the sixteen sums and both counts overflow the leaf.
    private static final int GROUPED_MEMBERS = 3;
    // The key count at which a grouped seal's frozen entries reach the retention limit: two
    // frozen keys, one fused payload and one image per member for each key.
    private static final int GROUPED_RETENTION_KEY_LIMIT =
            LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES / (2 + 1 + GROUPED_MEMBERS);
    private static final int GROUPED_SUMS = 16;
    private static final long LIFECYCLE_IDENTITY = 601;
    private static final int MEASURED_SEALS = 8;
    /**
     * Per-seal ceiling. The steady state is a few hundred bytes: the freeze walk's per-call
     * member lists and the publication's shells, none of which scale with the key set. A
     * payload imaged into a heap array per key costs this many bytes by the thirtieth key.
     */
    private static final long PER_SEAL_ALLOCATION_LIMIT_BYTES = 6_144;
    // The payload arena's page, which it grows by whole pages.
    private static final long PAYLOAD_ARENA_PAGE_BYTES = 4_096;
    // Keys whose records fill a page count no doubling from one page reaches, so an arena
    // sized in one allocation and one grown record by record end at different capacities.
    private static final int PRESIZED_CAPTURE_KEYS = 5_000;
    // The RANGE '1' HOUR frame of the ring view spans this many row steps.
    private static final int RING_FRAME_ROWS = 240;
    private static final long RING_ROW_STEP_MICROS = 15_000_000;
    // 2026-01-01T00:00:00Z.
    private static final long RING_START_MICROS = 1_767_225_600_000_000L;
    private static final String SEAL_DIR = "lv_frozen_payload";
    // Two full cycles of a sparse shared ring's chunk count: two pages, four, six.
    private static final int SHARED_RING_MEASURED_SEALS = 6;
    /**
     * Per-seal ceiling for a seal whose rings share chunks with the boundary below. Such a
     * root spans the data segments of the last few seals, and reading it and growing the
     * writer's pooled lists costs from about 3.5 KB to 16 KB depending on the seal, the same
     * at every key count. A reference array allocated per key costs more than this by the
     * 250th key.
     */
    private static final long SHARED_RING_PER_SEAL_ALLOCATION_LIMIT_BYTES = 32_768;
    // Rows per key a shared ring seal adds: far fewer than the frame holds, so the ring's
    // chunk cap forces a rebuild every third seal, and a key's chunk count moves every seal.
    private static final int SHARED_RING_ROWS_PER_SEAL = 8;
    // One full cycle, so the measured seals start where a warm writer's holders have seen
    // every chunk count the cycle takes.
    private static final int SHARED_RING_WARMUP_SEALS = 3;
    private static final int SYMBOL_CAPACITY = 65_536;
    private static final int WARMUP_SEALS = 4;
    // With the anchor value beside them they fill a leaf entry's whole inline budget, so the
    // fused payload is as wide as any gets.
    private static final int WIDE_FUSED_COMPONENTS =
            (LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES - Long.BYTES) / (Double.BYTES + Long.BYTES);
    private static final int WIDE_FUSED_PAYLOAD_BYTES = Long.BYTES + WIDE_FUSED_COMPONENTS * (Double.BYTES + Long.BYTES);
    // Past the four megabytes of images a freeze scratch used to keep pooled on the heap, and
    // well inside the key count at which it hands its holders back.
    private static final int WIDE_FUSED_KEY_COUNT = 4_194_304 / WIDE_FUSED_PAYLOAD_BYTES + 1_024;
    private int keyCount;
    private long ringRowCount;
    private int sealDirCount;
    private long seq;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        keyCount = 0;
        ringRowCount = 0;
        sealDirCount = 0;
        seq = 1;
    }

    @Test
    public void testAChainedRepairWritesAnImageThatReturnsToWhatThePublishedRootHolds() throws Exception {
        // An anchor resume re-versions every boundary above its anchor as a chain, and each
        // boundary's elision asks the chain whether the tree below already holds a key's
        // image. Here acct-1's DECIMAL sum - an inline image of its own function root, with no
        // count beside the sum - goes A, then B, then A again along the chain: the late row
        // moves it to B at the chain's first boundary, and the next row takes the same amount
        // back out. The tree the second boundary is built on holds B, staged by the chain, so
        // the second boundary must put A even though the published root below the chain
        // still holds exactly A. An answer from the published root would elide the put and
        // leave B standing in a boundary whose state is A.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE, d DECIMAL(38,2)) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, sum(amount) OVER w AS s, max(amount) OVER w AS m, "
                    + "sum(d) OVER w AS ds FROM tx" + DAILY_WINDOW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commit(job, decimalRow("2026-01-01T09:00", "acct-1", "1.0"));
                commit(job, decimalRow("2026-01-03T09:00", "acct-1", "16.0"));
                commit(job, decimalRow("2026-01-03T09:10", "acct-2", "32.0"));
                commit(job, decimalRow("2026-01-03T09:20", "acct-1", "-0.25"));
                commit(job, decimalRow("2026-01-03T09:30", "acct-2", "64.0"));
                final TreeMap<Long, String> before = functionScalarsOf(ACCT_1_KEY_HEX);
                commit(job, decimalRow("2026-01-03T09:05", "acct-1", "0.25"));
                final LiveViewInstance instance = viewInstance();
                Assert.assertTrue("the correction must resume from the anchor", instance.getO3ResumeReplayRows() > 0);
                Assert.assertTrue("the resume must splice its roots", instance.getCheckpointRepairRootsVersioned() > 0);
                assertNoRefreshFaults("lv");

                final TreeMap<Long, String> after = functionScalarsOf(ACCT_1_KEY_HEX);
                final long anchorTs = ts("2026-01-03T09:00:00.000000Z");
                final long firstChainedTs = ts("2026-01-03T09:10:00.000000Z");
                final long secondChainedTs = ts("2026-01-03T09:20:00.000000Z");
                final String published = after.get(anchorTs);
                Assert.assertNotNull("the boundary below the chain must hold acct-1", published);
                Assert.assertEquals("the boundary below the chain is not re-versioned", before.get(anchorTs), published);
                Assert.assertNotEquals(
                        "the chain's first boundary must hold the image the late row moved acct-1 to",
                        published,
                        after.get(firstChainedTs)
                );
                Assert.assertNotEquals(
                        "the late row must have changed what the chain's second boundary holds",
                        before.get(secondChainedTs),
                        after.get(secondChainedTs)
                );
                Assert.assertEquals(
                        "the chain's second boundary must hold the image acct-1 returned to, which is the published one",
                        published,
                        after.get(secondChainedTs)
                );
            }
        });
    }

    @Test
    public void testAFusedSealJustInsideTheFrozenRetentionLimitStaysWarm() throws Exception {
        // A fused seal freezes one key and one payload per key, which the limit counts
        // together, so this key set sits just inside it: its holders and handle lists stay
        // pooled, and a warm seal allocates nothing that scales with its keys.
        assertMemoryLeak(() -> {
            createView(1, false);
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                driveSeedToCompletion(job, "lv");
                final int keys = LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES / 2 - 1_024;
                addKeys(job, keys, 1);
                final long allocated = measureSeal(scope);
                Assert.assertTrue(
                        "a warm complete seal of " + keys + " keys inside the frozen retention limit allocated "
                                + allocated + " bytes on the Java heap",
                        allocated < PER_SEAL_ALLOCATION_LIMIT_BYTES
                );
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAFusedSealJustOutsideTheFrozenRetentionLimitHandsItsGraphBack() throws Exception {
        // One key and one payload per key put this key set just past the limit, although its
        // keys alone stay far inside it: the payloads count too, since each grows the frozen
        // handle lists as a pooled payload array once did. The writer hands the seal's graph
        // back rather than park it on the worker, so every such seal grows its handle lists
        // - keys, payloads and anchor values, a long each per key - again.
        assertMemoryLeak(() -> {
            createView(1, false);
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                driveSeedToCompletion(job, "lv");
                final int keys = LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES / 2 + 1_024;
                Assert.assertTrue(keys < LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_ENTRIES);
                addKeys(job, keys, 1);
                final long allocated = measureSeal(scope);
                Assert.assertTrue(
                        "a complete seal of " + keys + " keys outside the frozen retention limit allocated only "
                                + allocated + " bytes on the Java heap; it must not keep its frozen graph warm",
                        allocated > 3L * Long.BYTES * keys
                );
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAGroupedSealJustInsideTheFrozenRetentionLimitKeepsItsGraph() throws Exception {
        assertGroupedSealRetention(GROUPED_RETENTION_KEY_LIMIT - 512, true);
    }

    @Test
    public void testAGroupedSealJustOutsideTheFrozenRetentionLimitHandsItsGraphBack() throws Exception {
        assertGroupedSealRetention(GROUPED_RETENTION_KEY_LIMIT + 512, false);
    }

    @Test
    public void testAWarmCompleteGroupedMemberSealAllocatesNoHeapPerKey() throws Exception {
        // Sixteen sums overflow the leaf, so the last sum and both counts are runtime-only
        // members: each key freezes one fused payload and three member images.
        assertMemoryLeak(() -> {
            createView(true);
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                driveSeedToCompletion(job, "lv");
                addKeys(job, 1_024, GROUPED_SUMS);
                Assert.assertEquals("runtime-only members", GROUPED_MEMBERS, countRuntimeOnlyProjections());
                final long narrow = measureSeal(scope);
                addKeys(job, 8_192, GROUPED_SUMS);
                final long wide = measureSeal(scope);
                assertSealAllocation(1_024, narrow, 8_192, wide);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAWarmCompleteWideFusedSealAllocatesNoHeapPerKey() throws Exception {
        // The wider key set's payloads hold more bytes than a freeze scratch once kept pooled
        // on the heap, so a scratch that imaged them into heap arrays handed its whole frozen
        // graph back after every such seal and allocated it again at the next one.
        assertMemoryLeak(() -> {
            createView(false);
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                driveSeedToCompletion(job, "lv");
                addKeys(job, 1_024, WIDE_FUSED_COMPONENTS);
                Assert.assertEquals(
                        "the view must fuse every component into one payload",
                        WIDE_FUSED_PAYLOAD_BYTES,
                        window().getCheckpointWindowStatePlan().getTotalInlineStateBytes()
                );
                Assert.assertEquals("no runtime-only member", 0, countRuntimeOnlyProjections());
                final long narrow = measureSeal(scope);
                addKeys(job, WIDE_FUSED_KEY_COUNT, WIDE_FUSED_COMPONENTS);
                final long wide = measureSeal(scope);
                assertSealAllocation(1_024, narrow, WIDE_FUSED_KEY_COUNT, wide);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAWarmRingSealAllocatesNoHeapPerKey() throws Exception {
        // A RANGE frame keeps a ring of chunk pages plus a scalar per key. The seal opens each
        // key's entry in the root below, carries its chunks forward or rebuilds them, encodes
        // its scalar and hands its chunk references to the frozen entry, all through reference
        // objects and native scratch the ring builder reuses from key to key.
        assertMemoryLeak(() -> {
            createRingView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                driveSeedToCompletion(job, "lv");
                Assert.assertNull("a ring view keeps no anchor window", viewInstance().getAnchorWindow());
                addRingKeys(job, 1_024);
                final long narrow = measureSeal(scope);
                addRingKeys(job, 8_192);
                final long wide = measureSeal(scope);
                assertSealAllocation(1_024, narrow, 8_192, wide);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAWarmSharedRingSealAllocatesNoHeapPerKey() throws Exception {
        // A seal whose batch sits above the boundary below shares each key's ring chunks with
        // that boundary and adds one for the batch, until the ring reaches its chunk cap and
        // is rebuilt. A few rows per key per seal against a 240-row frame make that cap three
        // chunks, so every key's chunk count moves at every seal - two pages, four, six, then
        // two again - and the frozen holder that names a key's pages must take each new count
        // without allocating a reference array for it.
        assertMemoryLeak(() -> {
            createRingView();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                driveSeedToCompletion(job, "lv");
                final long[] narrow = new long[SHARED_RING_MEASURED_SEALS];
                final long narrowRefs = measureSharedRingSeals(job, scope, 1_024, narrow);
                final long[] wide = new long[SHARED_RING_MEASURED_SEALS];
                final long wideRefs = measureSharedRingSeals(job, scope, 4_096, wide);
                for (int i = 0; i < SHARED_RING_MEASURED_SEALS; i++) {
                    Assert.assertTrue(
                            "shared ring seal " + i + " of 1024 keys allocated " + narrow[i] + " bytes on the Java heap,"
                                    + " narrow=" + Arrays.toString(narrow) + ", wide=" + Arrays.toString(wide),
                            narrow[i] < SHARED_RING_PER_SEAL_ALLOCATION_LIMIT_BYTES
                    );
                    Assert.assertTrue(
                            "shared ring seal " + i + " of 4096 keys allocated " + wide[i] + " bytes on the Java heap"
                                    + " against " + narrow[i] + " at 1024 keys; a key whose chunk count moved must not"
                                    + " cost its frozen holder a reference array",
                            Math.abs(wide[i] - narrow[i]) < 4_096
                    );
                }
                // Every holder keeps the widest reference array it filled - three chunks of two
                // pages - which is also the proof that the seals shared their chunks.
                Assert.assertEquals("state page references the holders keep at 1024 keys", 1_024L * 6, narrowRefs);
                Assert.assertEquals("state page references the holders keep at 4096 keys", 4_096L * 6, wideRefs);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAnOpenCaptureSizesItsFusedPayloadArenaInOneAllocation() throws Exception {
        // The window walk sizes the capture scratch's payload arena for every key it visits
        // before it images the first, from its own key domain, so the open capture holds
        // exactly the pages its records need. An arena grown record by record would double
        // its way past them instead: 5,000 32-byte records fill 40 pages, and doubling from
        // one page stops at 64.
        assertMemoryLeak(() -> {
            createView(1, false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                addKeys(job, PRESIZED_CAPTURE_KEYS, 1);
                Assert.assertEquals("no runtime-only member", 0, countRuntimeOnlyProjections());
                final int payloadBytes = window().getCheckpointWindowStatePlan().getTotalInlineStateBytes();
                Assert.assertEquals(
                        "the capture must size its payload arena for its " + PRESIZED_CAPTURE_KEYS + " fused payloads"
                                + " in one allocation",
                        pageAlignedRecordBytes(PRESIZED_CAPTURE_KEYS, payloadBytes),
                        openCapturePayloadArenaBytes()
                );
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAnOpenCaptureSizesItsRingPayloadArenaInOneAllocation() throws Exception {
        // The ring arm sizes the arena for one scalar per key the walk may image before it
        // freezes the first.
        assertMemoryLeak(() -> {
            createRingView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                addRingKeys(job, PRESIZED_CAPTURE_KEYS);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(viewInstance());
                Assert.assertEquals(1, functions.size());
                final int scalarBytes = LiveViewCheckpointRangeRingStateReader.scalarStateBytes(
                        functions.getQuick(0).checkpointRingScalarWords()
                );
                Assert.assertEquals(
                        "the capture must size its payload arena for its " + PRESIZED_CAPTURE_KEYS + " ring scalars"
                                + " in one allocation",
                        pageAlignedRecordBytes(PRESIZED_CAPTURE_KEYS, scalarBytes),
                        openCapturePayloadArenaBytes()
                );
                assertNoRefreshFaults("lv");
            }
        });
    }

    /**
     * Seals a grouped view of {@code keys} keys once into a fresh timeline and checks whether
     * the writer kept the seal's frozen graph. Each key freezes its key twice - once for the
     * window's walk and once for the members' - one fused payload and one image per member,
     * and the limit counts all of them.
     */
    private void assertGroupedSealRetention(int keys, boolean isWarm) throws Exception {
        assertMemoryLeak(() -> {
            createView(true);
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                driveSeedToCompletion(job, "lv");
                addKeys(job, keys, GROUPED_SUMS);
                Assert.assertEquals("runtime-only members", GROUPED_MEMBERS, countRuntimeOnlyProjections());
                createSealLayout(dir);
                final LiveViewInstance instance = viewInstance();
                append(writer, dir, unwrapWindowFunctions(instance), instance.getAnchorWindow());
                // A graph kept warm holds a partition holder per member per key.
                final int retained = writer.getRetainedFrozenObjectCountForTest();
                if (isWarm) {
                    Assert.assertTrue(
                            "a grouped seal of " + keys + " keys inside the frozen retention limit must keep its"
                                    + " frozen graph, retained=" + retained,
                            retained >= GROUPED_MEMBERS * keys
                    );
                } else {
                    Assert.assertTrue(
                            "a grouped seal of " + keys + " keys outside the frozen retention limit must not park"
                                    + " its frozen graph on the writer, retained=" + retained,
                            retained < keys
                    );
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    private static void assertSealAllocation(int narrowKeys, long narrow, int wideKeys, long wide) {
        Assert.assertTrue(
                "a warm complete seal of " + narrowKeys + " keys allocated " + narrow + " bytes on the Java heap",
                narrow < PER_SEAL_ALLOCATION_LIMIT_BYTES
        );
        Assert.assertTrue(
                "a warm complete seal of " + wideKeys + " keys allocated " + wide + " bytes on the Java heap;"
                        + " a payload must not be imaged into a heap array",
                wide < PER_SEAL_ALLOCATION_LIMIT_BYTES
        );
        Assert.assertTrue(
                "a seal of " + wideKeys + " keys allocated " + wide + " bytes against " + narrow + " at "
                        + narrowKeys + " keys; nothing a seal puts on the heap may scale with its keys",
                Math.abs(wide - narrow) < 4_096
        );
    }

    /**
     * Grows the view's key domain to {@code targetKeyCount} with one commit of one row per
     * new key, later than every row before it, and refreshes the view over it.
     */
    private void addKeys(LiveViewRefreshJob job, int targetKeyCount, int sums) throws Exception {
        final StringBuilder values = new StringBuilder();
        for (int i = 1; i <= sums; i++) {
            values.append(", x::double");
        }
        execute("INSERT INTO tx SELECT ('2026-01-01T01:00:00.000000Z'::timestamp + (x + " + keyCount + ") * 1_000)::timestamp, "
                + "concat('acct-', x + " + keyCount + ")" + values + " FROM long_sequence(" + (targetKeyCount - keyCount) + ")");
        keyCount = targetKeyCount;
        driveRefreshToQuiescence(job);
        Assert.assertEquals(keyCount, window().getAnchorMapSize());
    }

    /**
     * Grows a ring view's key domain to {@code targetKeyCount} with one commit of one row per
     * new key, later than every row before it, and refreshes the view over it.
     */
    private void addRingKeys(LiveViewRefreshJob job, int targetKeyCount) throws Exception {
        execute("INSERT INTO tx SELECT ('2026-01-01T01:00:00.000000Z'::timestamp + (x + " + keyCount + ") * 1_000)::timestamp, "
                + "concat('acct-', x + " + keyCount + "), x::double FROM long_sequence(" + (targetKeyCount - keyCount) + ")");
        keyCount = targetKeyCount;
        driveRefreshToQuiescence(job);
    }

    /**
     * Adds {@code rows} row steps to a ring view, each one row for every one of the keys
     * {@code acct-0} to {@code acct-<keys - 1>}, and refreshes the view over them.
     */
    private void addRingRows(LiveViewRefreshJob job, int keys, int rows) throws Exception {
        execute("INSERT INTO tx SELECT (" + RING_START_MICROS + " + (" + ringRowCount + " + (x - 1) / " + keys + ") * "
                + RING_ROW_STEP_MICROS + ")::timestamp, concat('acct-', (x - 1) % " + keys + "), x::double"
                + " FROM long_sequence(" + ((long) keys * rows) + ")");
        ringRowCount += rows;
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private static String decimalRow(String minute, String account, String amount) {
        return "('" + minute + ":00.000000Z', '" + account + "', " + amount + ", " + amount + "::decimal(38,2))";
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    private void append(
            LiveViewCheckpointTimelineStoreWriter writer,
            Path dir,
            ObjList<WindowFunction> functions,
            LiveViewWindow window
    ) {
        append(writer, dir, functions, window, seq * 1_000_000L, seq * 1_000_000L);
    }

    /**
     * @param batchMinTs the lowest timestamp the view processed since the boundary below;
     *                   above that boundary's, it lets a ring share the boundary's chunks
     */
    private void append(
            LiveViewCheckpointTimelineStoreWriter writer,
            Path dir,
            ObjList<WindowFunction> functions,
            LiveViewWindow window,
            long maxTimestamp,
            long batchMinTs
    ) {
        writer.append(
                dir,
                functions,
                window,
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
        seq++;
    }

    private void commit(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO tx VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private int countRuntimeOnlyProjections() {
        final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
        Assert.assertNotNull("the group must be fused", plan);
        int count = 0;
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (!plan.isDurableProjection(i)) {
                count++;
            }
        }
        return count;
    }

    private void createRingView() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL CAPACITY " + SYMBOL_CAPACITY
                + ", q1 DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT created_at, account_id, "
                + "avg(q1) OVER (PARTITION BY account_id ORDER BY created_at "
                + "RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW) AS a FROM tx");
    }

    /**
     * Points {@code dir} at a checkpoint directory no seal has written to yet and creates
     * its layout.
     */
    private void createSealLayout(Path dir) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            dir.of(configuration.getDbRoot()).concat(SEAL_DIR).put(sealDirCount++).concat("_checkpoints");
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    /**
     * @param isGrouped true for sixteen sums and two counts, which overflow the leaf into
     *                  runtime-only members; false for as many sums as fill the leaf exactly
     */
    private void createView(boolean isGrouped) throws Exception {
        createView(isGrouped ? GROUPED_SUMS : WIDE_FUSED_COMPONENTS, isGrouped);
    }

    /**
     * @param sums       how many sums the view projects
     * @param withCounts whether it projects count(*) and count(account_id) beside them
     */
    private void createView(int sums, boolean withCounts) throws Exception {
        final StringBuilder columns = new StringBuilder();
        final StringBuilder projections = new StringBuilder();
        for (int i = 1; i <= sums; i++) {
            columns.append(", q").append(i).append(" DOUBLE");
            projections.append(", sum(q").append(i).append(") OVER w AS s").append(i);
        }
        if (withCounts) {
            projections.append(", count(*) OVER w AS n, count(account_id) OVER w AS c");
        }
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL CAPACITY " + SYMBOL_CAPACITY + columns + ") "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT created_at, account_id"
                + projections + " FROM tx" + DAILY_WINDOW);
    }

    /**
     * Seals the view's current key set into a fresh timeline under a fresh writer, warms both
     * up, then measures.
     *
     * @return the fewest heap bytes one warm complete seal of the view's current key set
     * allocated, over {@link #MEASURED_SEALS} seals
     */
    private long measureSeal(TestUtils.ThreadMetricsScope<ThreadMXBean> scope) {
        final LiveViewInstance instance = viewInstance();
        final LiveViewWindow window = instance.getAnchorWindow();
        final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
        try (
                LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                Path dir = new Path()
        ) {
            createSealLayout(dir);
            long min = Long.MAX_VALUE;
            for (int i = 0; i < WARMUP_SEALS + MEASURED_SEALS; i++) {
                // Outside the measurement: the window gives its incremental bookkeeping up, so
                // the seal below freezes every key rather than the ones touched since the last.
                // A view without an anchor window has only ring state, which every seal freezes
                // complete.
                if (window != null) {
                    try (LiveViewCheckpointSealState state = new LiveViewCheckpointSealState()) {
                        window.detachCheckpointSealState(state);
                    }
                }
                final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                append(writer, dir, functions, window);
                final long allocated = scope.getBean().getCurrentThreadAllocatedBytes() - before;
                if (i >= WARMUP_SEALS) {
                    min = Math.min(min, allocated);
                }
            }
            return min;
        }
    }

    /**
     * Gives the ring view's first {@code keys} keys a frame's worth of rows, seals them once
     * into a fresh timeline under a fresh writer, then seals batches of a few rows per key,
     * each above the boundary below it, so every key's ring shares that boundary's chunks.
     *
     * @param allocatedOut receives the heap bytes each measured seal allocated
     * @return the state page references the writer's partition holders keep afterwards
     */
    private long measureSharedRingSeals(
            LiveViewRefreshJob job,
            TestUtils.ThreadMetricsScope<ThreadMXBean> scope,
            int keys,
            long[] allocatedOut
    ) throws Exception {
        addRingRows(job, keys, RING_FRAME_ROWS);
        final ObjList<WindowFunction> functions = unwrapWindowFunctions(viewInstance());
        try (
                LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                Path dir = new Path()
        ) {
            createSealLayout(dir);
            // No boundary below, so every ring is rebuilt: one chunk per key.
            append(writer, dir, functions, null, ringRowTimestamp(ringRowCount - 1), Numbers.LONG_NULL);
            for (int i = 0; i < SHARED_RING_WARMUP_SEALS + SHARED_RING_MEASURED_SEALS; i++) {
                final long batchMinTs = ringRowTimestamp(ringRowCount);
                addRingRows(job, keys, SHARED_RING_ROWS_PER_SEAL);
                final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                append(writer, dir, functions, null, ringRowTimestamp(ringRowCount - 1), batchMinTs);
                final long allocated = scope.getBean().getCurrentThreadAllocatedBytes() - before;
                if (i >= SHARED_RING_WARMUP_SEALS) {
                    allocatedOut[i - SHARED_RING_WARMUP_SEALS] = allocated;
                }
            }
            return writer.getRetainedFrozenStatePageRefCountForTest();
        }
    }

    /**
     * @return per logical boundary of the view's published timeline, by its maximum
     * timestamp, the hex of the scalar image its one function root holds for the key whose
     * encoding is {@code keyHex}; a boundary that holds none has no mapping
     */
    private TreeMap<Long, String> functionScalarsOf(String keyHex) {
        final TreeMap<Long, String> scalars = new TreeMap<>();
        try (
                Path dir = new Path();
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            dir.of(configuration.getDbRoot()).concat(viewInstance().getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    root.of(dir, entry.rootRef);
                    root.getFunctionDirectoryRef(functionDirectoryRef);
                    functions.of(dir, functionDirectoryRef);
                    Assert.assertEquals("the view must keep one function root", 1, functions.size());
                    functions.getRootRef(0, functionRootRef);
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getPartitionMapRootRef(mapRootRef);
                    final long maxTimestamp = entry.maxTimestamp;
                    partitions.iterateAll(mapRootRef, partition -> {
                        if (keyHex.equals(hex(partition.copyKeyForTest()))) {
                            scalars.put(maxTimestamp, hex(partition.copyScalarStateForTest()));
                        }
                    });
                });
            }
        }
        return scalars;
    }

    /**
     * Seals the view's current key set once into a fresh timeline under a fresh writer, then
     * opens a repair capture over that one boundary and freezes it again, completely.
     *
     * @return the payload arena bytes the open capture holds
     */
    private long openCapturePayloadArenaBytes() {
        final LiveViewInstance instance = viewInstance();
        final LiveViewWindow window = instance.getAnchorWindow();
        final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
        final ObjList<LiveViewCheckpointTimelineEntry> boundaries = new ObjList<>();
        try (
                LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                Path dir = new Path()
        ) {
            createSealLayout(dir);
            append(writer, dir, functions, window);
            try (LiveViewCheckpointTimelineStoreWriter.RepairCapture capture = writer.beginRepair(dir, null, null, false)) {
                capture.collectBoundaries(0, Long.MAX_VALUE, boundaries);
                Assert.assertEquals(1, boundaries.size());
                Assert.assertEquals("the capture must start from an empty arena", 0, writer.getRetainedFrozenPayloadBytesForTest());
                capture.capture(boundaries.getQuick(0), functions, window, 1);
                return writer.getRetainedFrozenPayloadBytesForTest();
            }
        }
    }

    /**
     * @return the whole arena pages {@code records} records of {@code payloadBytes}-byte
     * payloads take: each is an int length and an int of padding, then the payload,
     * zero-padded to eight bytes
     */
    private static long pageAlignedRecordBytes(int records, int payloadBytes) {
        final long recordBytes = 2 * Integer.BYTES + ((payloadBytes + 7L) & ~7L);
        final long bytes = records * recordBytes;
        return (bytes + PAYLOAD_ARENA_PAGE_BYTES - 1) / PAYLOAD_ARENA_PAGE_BYTES * PAYLOAD_ARENA_PAGE_BYTES;
    }

    private static long ringRowTimestamp(long row) {
        return RING_START_MICROS + row * RING_ROW_STEP_MICROS;
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        return instance;
    }

    private LiveViewWindow window() {
        final LiveViewWindow window = viewInstance().getAnchorWindow();
        Assert.assertNotNull(window);
        return window;
    }
}
