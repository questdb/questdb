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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointRingSeal;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;

/**
 * White-box freeze/restore property fuzz for the five bounded-frame statistical window
 * functions a live view checkpoints: stddev/variance over ROWS and RANGE frames, covar/corr
 * over ROWS and RANGE frames, and ksum over RANGE frames.
 * <p>
 * Each case compiles one window function the way a live view does, feeds it a random row
 * stream (random timestamp gaps, some wider than the frame, NULL and infinite values, half-NULL
 * pairs, up to eight partitions plus a NULL key), and at random points freezes every partition
 * of that reference function and restores it into a second function. The second function then
 * consumes the same suffix and must emit the reference's values bit for bit. A restore takes
 * one of two forms at random:
 * <ul>
 *     <li>the durable form: the production ring seal and ring reader for a RANGE function,
 *     sharing chunks with the previous seal the way a cadence seal does, or one state page per
 *     partition for a ROWS function;</li>
 *     <li>the in-RAM form a repair's scratch overlay uses, {@link LiveViewFunctionSnapshot}.</li>
 * </ul>
 * Sometimes a second restore follows the first at the same point, and sometimes the target is
 * a freshly compiled function rather than the one the previous restore filled. Right after
 * every restore each partition's in-RAM image must match the reference's byte for byte, which
 * is the freeze, restore, freeze stability of the codec.
 * <p>
 * RANGE cases run with an initial ring capacity of 1, 2, 4 and 32 rows, so restore points land
 * on wrapped rings, on rings that just grew and left a slab on the free list, on empty rings,
 * and on lagged frames whose ring holds rows the frame has not reached yet.
 * <p>
 * Every class carries its running sums verbatim, so every case uses values of mixed magnitude,
 * whose incremental sums carry rounding a re-summation would not reproduce. Integer values
 * would hide a restore that re-summed the frame, as every summation order produces the same
 * doubles over them.
 */
public class LiveViewCheckpointStatFrameRestoreFuzzTest extends AbstractCairoTest {

    private static final String[] BIVARIATE_NAMES = {"covar_samp", "covar_pop", "corr"};
    // The {count, size, capacity, firstIdx} slots of a partition's map value in the RANGE
    // classes: BivarStatOverPartitionRangeFrameFunction keeps five sums ahead of them,
    // StdDevOverPartitionRangeFrameFunction and KSumOverPartitionRangeFrameFunction two.
    private static final int[] BIVARIATE_RANGE_SLOTS = {5, 7, 8, 9};
    private static final int FRAME_CURRENT = 0;
    private static final int FRAME_EXCLUDED = 2;
    private static final int FRAME_LAGGED = 1;
    private static final int FRAME_SPELLINGS = 3;
    private static final int[] INITIAL_RANGE_BUFFER_SIZES = {1, 2, 4, 32};
    private static final String[] KSUM_NAMES = {"ksum"};
    private static final String LV_DIR = "lv_stat_frame_restore_fuzz";
    // Rounds the ROWS tests run per frame spelling: a ROWS function ignores the initial range
    // buffer size, so these rounds only draw more data.
    private static final int ROWS_ROUNDS = 4;
    private static final int[] SINGLE_VALUE_RANGE_SLOTS = {2, 4, 5, 6};
    private static final String[] STDDEV_NAMES = {"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop"};
    private static final String[] SYMBOLS = {"a", "b", "c", "d", "e", "f", "g", "h"};
    // The NULL key's partition id, past every symbol's.
    private static final int NULL_KEY_ID = SYMBOLS.length;
    // Static: published segments are immutable, and every case of the class writes into the
    // same checkpoints directory.
    private static long nextDataSegmentId = 1;
    private static long nextMetaSegmentId = 1_000_000;

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.DATA_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testBivariateRangeRestoreFuzz() throws Exception {
        assertRangeFuzz("BivarStatOverPartitionRangeFrameFunction", BIVARIATE_NAMES, true, BIVARIATE_RANGE_SLOTS);
    }

    @Test
    public void testBivariateRowsRestoreFuzz() throws Exception {
        assertRowsFuzz("BivarStatOverPartitionRowsFrameFunction", BIVARIATE_NAMES, true);
    }

    @Test
    public void testKSumRangeRestoreFuzz() throws Exception {
        assertRangeFuzz("KSumOverPartitionRangeFrameFunction", KSUM_NAMES, false, SINGLE_VALUE_RANGE_SLOTS);
    }

    @Test
    public void testStdDevRangeRestoreFuzz() throws Exception {
        assertRangeFuzz("StdDevOverPartitionRangeFrameFunction", STDDEV_NAMES, false, SINGLE_VALUE_RANGE_SLOTS);
    }

    @Test
    public void testStdDevRowsRestoreFuzz() throws Exception {
        assertRowsFuzz("StdDevOverPartitionRowsFrameFunction", STDDEV_NAMES, false);
    }

    private static void assertSameImages(WindowFunction reference, WindowFunction restored, MemoryCARWImpl scratch, String label) {
        final Map referenceMap = reference.getPartitionMap();
        final Map restoredMap = restored.getPartitionMap();
        Assert.assertEquals(label + ": partition count after restore", referenceMap.size(), restoredMap.size());
        final int keyIndex = reference.getCheckpointKeyStartIndex();
        final LiveViewStatePageWriter writer = new LiveViewStatePageWriter();
        final MapRecordCursor cursor = referenceMap.getCursor();
        final MapRecord record = referenceMap.getRecord();
        while (cursor.hasNext()) {
            final CharSequence key = record.getStrA(keyIndex);
            final MapKey restoredKey = restoredMap.withKey();
            restoredKey.putStr(key);
            final MapValue restoredValue = restoredKey.findValue();
            Assert.assertNotNull(label + ": partition " + key + " missing after restore", restoredValue);
            scratch.jumpTo(0);
            final long referenceLength = writer.of(scratch).freeze(reference, record.getValue());
            final long referenceStart = writer.getPageStart();
            final long restoredLength = writer.of(scratch).freeze(restored, restoredValue);
            final long restoredStart = writer.getPageStart();
            Assert.assertEquals(label + ": image length of partition " + key, referenceLength, restoredLength);
            for (long i = 0; i < referenceLength; i++) {
                if (scratch.getByte(referenceStart + i) != scratch.getByte(restoredStart + i)) {
                    Assert.fail(label + ": image of partition " + key + " differs at byte " + i
                            + " of " + referenceLength);
                }
            }
        }
    }

    private static void assertSameOutput(String label, int row, double expected, double actual) {
        if (Double.isNaN(expected) && Double.isNaN(actual)) {
            return;
        }
        final long expectedBits = Double.doubleToRawLongBits(expected);
        final long actualBits = Double.doubleToRawLongBits(actual);
        if (expectedBits != actualBits) {
            Assert.fail(label + ": row " + row + " expected " + expected + " [bits=" + Long.toHexString(expectedBits)
                    + "] but the restored function emitted " + actual + " [bits=" + Long.toHexString(actualBits) + ']');
        }
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static MapValue createValue(Map map, CharSequence key) {
        final MapKey mapKey = map.withKey();
        mapKey.putStr(key);
        return mapKey.createValue();
    }

    private static String frame(boolean isRange, int spelling, int n, int m) {
        final String head = "PARTITION BY sym ORDER BY ts " + (isRange ? "RANGE" : "ROWS") + " BETWEEN ";
        final String lo = isRange ? "'" + n + "' SECOND PRECEDING" : n + " PRECEDING";
        return switch (spelling) {
            case FRAME_CURRENT -> head + lo + " AND CURRENT ROW";
            case FRAME_LAGGED -> head + lo + " AND " + (isRange ? "'" + m + "' SECOND PRECEDING" : m + " PRECEDING");
            case FRAME_EXCLUDED -> head + lo + " AND CURRENT ROW EXCLUDE CURRENT ROW";
            default -> throw new IllegalArgumentException("spelling=" + spelling);
        };
    }

    private static void openFunctions(WindowRecordCursorFactory factory, RecordCursor baseCursor) throws SqlException {
        // What the window cursor's first open does, minus the cursor: the lazy partition maps
        // open, and every function binds to the base cursor's symbol tables.
        final ObjList<Function> functions = factory.getFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (functions.getQuick(i) instanceof Reopenable reopenable) {
                reopenable.reopen();
            }
        }
        Function.init(functions, baseCursor, sqlExecutionContext, null);
    }

    private static int partitionId(CharSequence key) {
        if (key == null) {
            return NULL_KEY_ID;
        }
        for (int i = 0; i < SYMBOLS.length; i++) {
            if (Chars.equals(SYMBOLS[i], key)) {
                return i;
            }
        }
        throw new AssertionError("unexpected partition key " + key);
    }

    private static double randomValue(Rnd rnd, int nullPct) {
        final int draw = rnd.nextInt(100);
        if (draw < nullPct) {
            return Double.NaN;
        }
        if (draw < nullPct + 2) {
            // Never in a RANGE ring; a raw cell a ROWS ring keeps and never counts.
            return rnd.nextBoolean() ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
        if (draw < nullPct + 3) {
            return -0.0;
        }
        // Magnitudes from 1e-3 to 1e9 in one partition: the incremental sums keep rounding
        // residue that a fresh summation of the frame would not reproduce.
        return (rnd.nextDouble() - 0.5) * Math.pow(10, rnd.nextInt(13) - 3);
    }

    /**
     * The durable form of a ROWS function: one whole-state page per partition, read back into
     * a partition keyed by the same string, the way a timeline restore reads each partition
     * entry.
     */
    private static void restoreStatePages(WindowFunction reference, WindowFunction restored, MemoryCARWImpl page, String label) {
        restored.onCheckpointRestoreBegin();
        final Map map = reference.getPartitionMap();
        final Map restoredMap = restored.getPartitionMap();
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = map.getRecord();
        final int keyIndex = reference.getCheckpointKeyStartIndex();
        final LiveViewStatePageWriter pageWriter = new LiveViewStatePageWriter();
        final LiveViewStatePageReader pageReader = new LiveViewStatePageReader();
        while (cursor.hasNext()) {
            page.jumpTo(0);
            final long length = pageWriter.of(page).freeze(reference, record.getValue());
            pageReader.of(page, pageWriter.getPageStart(), length);
            final MapValue value = createValue(restoredMap, record.getStrA(keyIndex));
            Assert.assertEquals(label + ": state page not fully consumed", length, restored.restoreCheckpointState(pageReader, 0, value));
        }
    }

    private static WindowRecordCursorFactory windowFactory(RecordCursorFactory factory, String sql) {
        RecordCursorFactory root = factory;
        while (root instanceof QueryProgress) {
            root = root.getBaseFactory();
        }
        Assert.assertTrue(sql, root instanceof WindowRecordCursorFactory);
        final WindowRecordCursorFactory windowFactory = (WindowRecordCursorFactory) root;
        Assert.assertEquals(sql + ": expected exactly one window function", 1, windowFactory.getWindowFunctions().size());
        return windowFactory;
    }

    private void assertRangeFuzz(
            String className,
            String[] names,
            boolean isBivariate,
            int[] ringSlots
    ) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final RestoreStats stats = new RestoreStats();
            for (int bufferSize : INITIAL_RANGE_BUFFER_SIZES) {
                setProperty(PropertyKey.CAIRO_SQL_ANALYTIC_INITIAL_RANGE_BUFFER_SIZE, bufferSize);
                Assert.assertEquals(bufferSize, configuration.getSqlWindowInitialRangeBufferSize());
                for (int spelling = 0; spelling < FRAME_SPELLINGS; spelling++) {
                    runCase(rnd, className, names, isBivariate, true, spelling, ringSlots, stats);
                }
            }
            stats.assertExercised();
            // The ring shapes a restore point has to land on, or the RANGE codecs' rotation,
            // rebase and capacity handling went untested.
            Assert.assertTrue("no restore point met a wrapped ring", stats.wrappedRings > 0);
            Assert.assertTrue("no restore point met a full ring", stats.fullRings > 0);
            Assert.assertTrue("no restore point met a ring holding rows the frame has not reached", stats.laggedRings > 0);
            Assert.assertTrue("no restore point met an empty ring", stats.emptyRings > 0);
        });
    }

    private void assertRowsFuzz(String className, String[] names, boolean isBivariate) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final RestoreStats stats = new RestoreStats();
            for (int round = 0; round < ROWS_ROUNDS; round++) {
                for (int spelling = 0; spelling < FRAME_SPELLINGS; spelling++) {
                    runCase(rnd, className, names, isBivariate, false, spelling, null, stats);
                }
            }
            stats.assertExercised();
        });
    }

    private RecordCursorFactory compile(String sql) throws SqlException {
        sqlExecutionContext.setLiveViewCompile(true);
        try {
            return select(sql, sqlExecutionContext);
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private void runCase(
            Rnd rnd,
            String className,
            String[] names,
            boolean isBivariate,
            boolean isRange,
            int spelling,
            int[] ringSlots,
            RestoreStats stats
    ) throws Exception {
        final int n = 1 + rnd.nextInt(isRange ? 20 : 30);
        final int m = 1 + rnd.nextInt(n);
        final String name = names[rnd.nextInt(names.length)];
        final String sql = "SELECT ts, sym, " + name + (isBivariate ? "(x, y)" : "(x)")
                + " OVER (" + frame(isRange, spelling, n, m) + ") AS v FROM t";
        final int rowCount = 200 + rnd.nextInt(400);
        final int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        final boolean hasNullKey = rnd.nextInt(3) == 0;
        final int nullPct = 10 + rnd.nextInt(21);
        // Gaps are drawn against the frame's width: dense (many rows per frame, so the rings
        // grow and wrap), about one frame (rows enter and leave constantly) or wider than the
        // frame (rings empty out between rows).
        final long frameMicros = n * 1_000_000L;
        final int gapMode = rnd.nextInt(3);
        final long gapMax = gapMode == 0 ? Math.max(1, frameMicros / 50) : gapMode == 1 ? frameMicros : 3 * frameMicros;
        final String label = sql + " [initialRangeBuffer=" + configuration.getSqlWindowInitialRangeBufferSize()
                + ", rows=" + rowCount + ", symCount=" + symCount + ", nullKey=" + hasNullKey
                + ", gapMax=" + gapMax + ']';
        LOG.info().$("stat frame restore fuzz case: ").$(label).$();

        execute("DROP TABLE IF EXISTS t");
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        try (TableWriter writer = getWriter("t")) {
            long ts = 1_767_225_600_000_000L; // 2026-01-01T00:00:00Z
            for (int k = 0; k < rowCount; k++) {
                ts += rnd.nextInt(20) == 0 ? 4 * frameMicros : 1 + rnd.nextLong(gapMax);
                final TableWriter.Row row = writer.newRow(ts);
                final boolean isNullKey = hasNullKey && rnd.nextInt(8) == 0;
                if (!isNullKey) {
                    row.putSym(1, SYMBOLS[rnd.nextInt(symCount)]);
                }
                row.putDouble(2, randomValue(rnd, nullPct));
                row.putDouble(3, randomValue(rnd, nullPct));
                row.append();
            }
            writer.commit();
        }

        RecordCursorFactory referenceFactory = null;
        RecordCursorFactory restoredFactory = null;
        RecordCursor baseCursor = null;
        try (
                RingStore ringStore = new RingStore();
                MemoryCARWImpl snapshot = new MemoryCARWImpl(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARWImpl scratch = new MemoryCARWImpl(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
        ) {
            referenceFactory = compile(sql);
            final WindowRecordCursorFactory referenceWindow = windowFactory(referenceFactory, sql);
            final WindowFunction reference = referenceWindow.getWindowFunctions().getQuick(0);
            Assert.assertEquals(label, className, reference.getClass().getSimpleName());
            Assert.assertTrue(label, reference.supportsCheckpointState());
            Assert.assertEquals(label, isRange, reference.supportsCheckpointRingState());
            // A live view compile keys a SYMBOL partition by its resolved string.
            Assert.assertEquals(label, ColumnType.STRING, reference.getCheckpointKeyColumnTypes().getColumnType(0));

            restoredFactory = compile(sql);
            WindowFunction restored = windowFactory(restoredFactory, sql).getWindowFunctions().getQuick(0);

            final int timestampIndex = referenceWindow.getBaseFactory().getMetadata().getTimestampIndex();
            Assert.assertTrue(label, timestampIndex >= 0);
            baseCursor = referenceWindow.getBaseFactory().getCursor(sqlExecutionContext);
            openFunctions(referenceWindow, baseCursor);
            openFunctions(windowFactory(restoredFactory, sql), baseCursor);
            final Record record = baseCursor.getRecord();

            boolean isRestoredActive = false;
            int row = 0;
            while (baseCursor.hasNext()) {
                reference.computeNext(record);
                final double expected = reference.getDouble(null);
                if (isRestoredActive) {
                    restored.computeNext(record);
                    final double actual = restored.getDouble(null);
                    assertSameOutput(label, row, expected, actual);
                    stats.comparedRows++;
                    if (Double.isFinite(expected)) {
                        stats.comparedFiniteRows++;
                    }
                }
                final long rowTimestamp = record.getTimestamp(timestampIndex);
                row++;
                if (row == rowCount || (row > 3 && rnd.nextInt(25) != 0) || (row <= 3 && rnd.nextInt(3) != 0)) {
                    continue;
                }

                // A restore point: optionally swap in a function that never ran, then restore
                // every partition of the reference into it, sometimes twice in a row.
                if (ringSlots != null) {
                    stats.countRingShapes(reference.getPartitionMap(), ringSlots);
                }
                if (rnd.nextInt(4) == 0) {
                    restoredFactory = Misc.free(restoredFactory);
                    restoredFactory = compile(sql);
                    final WindowRecordCursorFactory restoredWindow = windowFactory(restoredFactory, sql);
                    restored = restoredWindow.getWindowFunctions().getQuick(0);
                    openFunctions(restoredWindow, baseCursor);
                    stats.freshTargets++;
                }
                final boolean isDurable = rnd.nextBoolean();
                final int restores = rnd.nextInt(4) == 0 ? 2 : 1;
                for (int r = 0; r < restores; r++) {
                    // The last restore is the one the suffix runs on; a first one, when there
                    // is one, takes the other form and leaves state the last must replace.
                    final boolean isDurableRestore = r == restores - 1 ? isDurable : !isDurable;
                    if (isDurableRestore) {
                        if (isRange) {
                            ringStore.restore(reference, restored, rowTimestamp);
                        } else {
                            restoreStatePages(reference, restored, snapshot, label);
                        }
                        stats.durableRestores++;
                    } else {
                        snapshot.jumpTo(0);
                        LiveViewFunctionSnapshot.write(snapshot, reference);
                        LiveViewFunctionSnapshot.restore(snapshot, 0, snapshot.getAppendOffset(), restored);
                        stats.inRamRestores++;
                    }
                }
                assertSameImages(reference, restored, scratch, label + " at row " + row);
                isRestoredActive = true;
                stats.restorePoints++;
            }
            Assert.assertEquals(label, rowCount, row);
        } finally {
            Misc.free(baseCursor);
            Misc.free(restoredFactory);
            Misc.free(referenceFactory);
        }
    }

    private static class RestoreStats {
        private long comparedFiniteRows;
        private long comparedRows;
        private long durableRestores;
        private long emptyRings;
        private long freshTargets;
        private long fullRings;
        private long inRamRestores;
        private long laggedRings;
        private long restorePoints;
        private long wrappedRings;

        private void assertExercised() {
            LOG.info().$("stat frame restore fuzz: restorePoints=").$(restorePoints)
                    .$(", durableRestores=").$(durableRestores)
                    .$(", inRamRestores=").$(inRamRestores)
                    .$(", freshTargets=").$(freshTargets)
                    .$(", comparedRows=").$(comparedRows)
                    .$(", comparedFiniteRows=").$(comparedFiniteRows)
                    .$(", wrappedRings=").$(wrappedRings)
                    .$(", fullRings=").$(fullRings)
                    .$(", laggedRings=").$(laggedRings)
                    .$(", emptyRings=").$(emptyRings).$();
            Assert.assertTrue("no durable restore ran", durableRestores > 0);
            Assert.assertTrue("no in-RAM restore ran", inRamRestores > 0);
            Assert.assertTrue("no finite value was compared after a restore", comparedFiniteRows > 0);
        }

        /**
         * Classifies every partition ring of a RANGE function at a restore point, reading the
         * {count, size, capacity, firstIdx} slots of its map value.
         */
        private void countRingShapes(Map map, int[] slots) {
            final MapRecordCursor cursor = map.getCursor();
            final MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                final MapValue value = record.getValue();
                final long count = value.getLong(slots[0]);
                final long size = value.getLong(slots[1]);
                final long capacity = value.getLong(slots[2]);
                final long firstIdx = value.getLong(slots[3]);
                if (size == 0) {
                    emptyRings++;
                }
                if (count < size) {
                    laggedRings++;
                }
                if (size > 0 && size == capacity) {
                    fullRings++;
                }
                if (firstIdx + size > capacity) {
                    wrappedRings++;
                }
            }
        }
    }

    /**
     * The durable form of a RANGE function: every partition ring sealed through the production
     * ring seal into one data segment per restore point, then read back through the production
     * ring reader. Each seal passes the partition's entry from the previous seal as the
     * boundary to share chunks with, the way a cadence seal does; that is sound here because
     * the stream is strictly ascending, so every row above the previous boundary is new.
     */
    private static final class RingStore implements Closeable {
        private final SegmentCatalogue catalogue = new SegmentCatalogue();
        private final ObjList<LiveViewCheckpointPartitionMapEntry> currentEntries = new ObjList<>();
        private final Path dir = new Path();
        private final boolean[] hasPrevious = new boolean[NULL_KEY_ID + 1];
        private final LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys();
        private final ObjList<LiveViewCheckpointPartitionMapEntry> previousEntries = new ObjList<>();
        private final LiveViewCheckpointRangeRingStateReader ringReader = new LiveViewCheckpointRangeRingStateReader(configuration);
        private final LiveViewCheckpointRingSeal ringSeal = new LiveViewCheckpointRingSeal(configuration, null);
        private long previousBoundaryTimestamp = Long.MIN_VALUE;

        private RingStore() {
            for (int i = 0; i <= NULL_KEY_ID; i++) {
                previousEntries.add(new LiveViewCheckpointPartitionMapEntry());
                currentEntries.add(new LiveViewCheckpointPartitionMapEntry());
            }
        }

        @Override
        public void close() {
            Misc.free(ringSeal);
            Misc.free(ringReader);
            Misc.free(key);
            Misc.free(catalogue);
            Misc.free(dir);
            Misc.freeObjList(previousEntries);
            Misc.freeObjList(currentEntries);
        }

        private void restore(WindowFunction reference, WindowFunction restored, long boundaryTimestamp) {
            final Map map = reference.getPartitionMap();
            final MapRecordCursor cursor = map.getCursor();
            final MapRecord record = map.getRecord();
            final int keyIndex = reference.getCheckpointKeyStartIndex();
            final long segmentId = nextDataSegmentId++;
            try (LiveViewCheckpointDataSegmentWriter segmentWriter = new LiveViewCheckpointDataSegmentWriter(configuration)) {
                segmentWriter.of(checkpointsDir(dir), segmentId);
                try {
                    while (cursor.hasNext()) {
                        final int id = partitionId(record.getStrA(keyIndex));
                        key.of(new byte[]{(byte) id});
                        ringSeal.seal(
                                segmentWriter,
                                reference,
                                record.getValue(),
                                key.address(),
                                key.length(),
                                hasPrevious[id] ? previousEntries.getQuick(id) : null,
                                previousBoundaryTimestamp,
                                currentEntries.getQuick(id)
                        );
                    }
                    // A seal whose partitions all shared their chunks writes no payload.
                    if (segmentWriter.isEmpty()) {
                        segmentWriter.discard();
                    } else {
                        catalogue.addSegment(segmentId, segmentWriter.commit());
                    }
                } catch (Throwable th) {
                    segmentWriter.discard();
                    throw th;
                }
            }

            restored.onCheckpointRestoreBegin();
            final Map restoredMap = restored.getPartitionMap();
            cursor.toTop();
            while (cursor.hasNext()) {
                final CharSequence partitionKey = record.getStrA(keyIndex);
                final int id = partitionId(partitionKey);
                ringReader.of(checkpointsDir(dir), catalogue.reader, currentEntries.getQuick(id));
                restored.restoreCheckpointRingState(ringReader, createValue(restoredMap, partitionKey));
                hasPrevious[id] = true;
            }

            // The entries just written are the next seal's previous boundary.
            for (int i = 0; i <= NULL_KEY_ID; i++) {
                final LiveViewCheckpointPartitionMapEntry swap = previousEntries.getQuick(i);
                previousEntries.setQuick(i, currentEntries.getQuick(i));
                currentEntries.setQuick(i, swap);
            }
            previousBoundaryTimestamp = boundaryTimestamp;
        }
    }

    /**
     * Publishes each data segment into a fresh copy-on-write directory generation and keeps a
     * reader bound to the newest root, which is what a ring read validates its chunk
     * references against.
     */
    private static final class SegmentCatalogue implements Closeable {
        private final LiveViewCheckpointSegmentDirectoryReader reader =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        private final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();

        @Override
        public void close() {
            reader.close();
        }

        private void addSegment(long segmentId, long fileLength) {
            try (LiveViewCheckpointSegmentDirectoryWriter writer = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.begin(root);
                writer.addSegment(segmentId, fileLength, 1);
                writer.publish(nextMetaSegmentId++, 1, root);
                reader.of(checkpointsDir(dir), root);
            }
        }
    }
}
