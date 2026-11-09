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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.LongChain;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongPredicate;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.cairo.sql.RecordCursorFactory.SCAN_DIRECTION_BACKWARD;

/**
 * Builds from a table's page frames as the fused operator's owner does, or its frame and partition
 * tasks do, and freezes over the frames, so that probes read real payload columns at the kept row
 * ids. The caller keeps the frames open until every probe closes, then closes the build and the
 * frames. The chains of such a build can be checked against the ones that the serial hash join's
 * {@link LongChain} makes over the same scan; see {@link #longChains}.
 */
final class FrameBuilds {
    /**
     * Table t, whose INT key k (column 0) and SYMBOL key s (column 2) repeat within every page frame
     * of 20 rows, across frames and across six daily partitions of 100 rows, the first two of them
     * Parquet, NULL included; v (column 1) numbers the rows from one. Table p holds every text of s
     * in another dictionary order, plus one that t lacks, for probes that translate into t's keys.
     */
    static final String[] SCAN_ORDER_TABLES = {
            "CREATE TABLE t (k INT, v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
            """
            INSERT INTO t
            SELECT CASE WHEN x % 13 = 0 THEN NULL ELSE (x % 6)::INT END, x,
                   CASE WHEN x % 17 = 0 THEN NULL ELSE 's' || (x % 5) END,
                   timestamp_sequence('2020-01-01', 864_000_000L)
            FROM long_sequence(600)
            """,
            "ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'",
            "CREATE TABLE p (s SYMBOL)",
            "INSERT INTO p VALUES ('s0'), ('s9'), ('s4'), ('s3'), ('s2'), ('s1')"
    };
    // The page frame rows of SCAN_ORDER_TABLES' t: every key repeats within a native frame.
    static final int SCAN_ORDER_FRAME_ROWS = 20;
    // Stands for the partitionFrame() overloads that take no row order and walk each frame from its first row.
    private static final int NO_ORDER = -1;

    private FrameBuilds() {
    }

    /**
     * Walks the chain that the probe's last lookup found, and checks that it lists exactly the given
     * values of payload column 0, in their order, each one also through its handle; null expects a
     * miss.
     */
    static void assertChain(String label, FrozenHashJoinBuild.Probe probe, @Nullable List<Long> expected) {
        if (expected != null) {
            for (int i = 0, n = expected.size(); i < n; i++) {
                Assert.assertTrue(label + ", match " + i, probe.hasNext());
                final long handle = probe.next();
                Assert.assertEquals(label + ", match " + i, expected.get(i).longValue(), probe.getRecord().getLong(0));
                probe.recordAt(handle);
                Assert.assertEquals(label + ", match " + i, expected.get(i).longValue(), probe.getRecord().getLong(0));
            }
        }
        Assert.assertFalse(label, probe.hasNext());
    }

    /**
     * Checks that an INT-keyed build holds exactly the expected chains, each listing the values of
     * payload column 0 in the order that a probe walks them: a probe looks up NULL and every key from
     * {@code keyLo} to {@code keyHi}, inclusive, which have to cover the expected keys, and misses
     * each key without a chain.
     */
    static void assertIntChains(FrozenHashJoinBuild.IntKeyed frozen, Map<Integer, List<Long>> expected, int keyLo, int keyHi) {
        assertCounts(frozen, expected);
        int matchedKeyCount = 0;
        try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
            for (long k = keyLo - 1L; k <= keyHi; k++) {
                final int key = k < keyLo ? Numbers.INT_NULL : (int) k;
                final List<Long> chain = expected.get(key);
                if (chain != null) {
                    matchedKeyCount++;
                }
                probe.find(key);
                assertChain("key " + key, probe, chain);
            }
        }
        Assert.assertEquals(expected.size(), matchedKeyCount);
    }

    /**
     * Checks that a staged-key build holds exactly the expected chains, by key text, each listing the
     * values of payload column 0 in the order that a probe walks them. A probe stages the key of the
     * first row of each key that the frames hold, kept or not; a key whose rows the build dropped
     * misses.
     */
    static void assertRecordChains(
            CairoConfiguration configuration,
            FrozenHashJoinBuild.RecordKeyed frozen,
            RecordSink probeKeySink,
            HashJoinBuildFrames frames,
            Function<Record, String> keyOf,
            Map<String, List<Long>> expected
    ) {
        assertCounts(frozen, expected);
        final Set<String> probed = new HashSet<>();
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(probeKeySink)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                for (long row = 0, n = frames.getFrameRowCount(frameIndex); row < n; row++) {
                    record.setRowIndex(row);
                    final String key = keyOf.apply(record);
                    if (probed.add(key)) {
                        probe.find(record);
                        assertChain("key " + key, probe, expected.get(key));
                    }
                }
            }
        }
        Assert.assertTrue(probed.containsAll(expected.keySet()));
    }

    /**
     * Checks that an INT-keyed build of a SYMBOL key column holds exactly the expected chains, by
     * symbol text, NULL included. A probe looks every key of the probe dictionary up, and NULL,
     * through the translation into the build dictionary that the operator's probes take, so the
     * probe dictionary has to hold every text of the build's.
     */
    static void assertTranslatedSymbolChains(
            FrozenHashJoinBuild.IntKeyed frozen,
            Map<String, List<Long>> expected,
            StaticSymbolTable probeDictionary,
            StaticSymbolTable buildDictionary,
            @Nullable MemoryTracker memoryTracker
    ) {
        assertCounts(frozen, expected);
        int matchedKeyCount = 0;
        try (SymbolKeyTranslator translator = new SymbolKeyTranslator();
             SymbolKeyTranslator.View view = new SymbolKeyTranslator.View();
             FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
            translator.of(probeDictionary.getSymbolCount(), memoryTracker, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            view.of(translator, probeDictionary, buildDictionary);
            for (int probeKey = -1, n = probeDictionary.getSymbolCount(); probeKey < n; probeKey++) {
                final int key = probeKey < 0 ? SymbolTable.VALUE_IS_NULL : probeKey;
                final CharSequence text = probeDictionary.valueOf(key);
                final String label = text != null ? text.toString() : null;
                final List<Long> chain = expected.get(label);
                if (chain != null) {
                    matchedKeyCount++;
                }
                probe.find(view.translate(key));
                assertChain("symbol " + label, probe, chain);
            }
        }
        Assert.assertEquals(expected.size(), matchedKeyCount);
    }

    static FrozenHashJoinBuild.IntKeyed buildInt(
            CairoConfiguration configuration,
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordCursorFactory factory,
            int keyColumn,
            SqlExecutionContext context
    ) throws SqlException {
        frames.open(factory, context);
        return buildIntOverOpenFrames(configuration, build, frames, keyColumn, context);
    }

    /**
     * Builds as {@link #buildInt} does, from frames whose cursor the caller opened: this walks
     * their frames, then builds from them.
     */
    static FrozenHashJoinBuild.IntKeyed buildIntOverOpenFrames(
            CairoConfiguration configuration,
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            int keyColumn,
            SqlExecutionContext context
    ) {
        frames.loadFrames();
        build.open(context.getMemoryTracker(), context.getCircuitBreaker());
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                build.appendFrame(record, keyColumn, frames.getFrameRowCount(frameIndex), ORDER_ASC);
            }
        }
        return build.freeze(frames);
    }

    /**
     * Builds from the open frames as the fused operator's parallel build does, running every frame
     * task and then every partition task on this thread: the frames in order, the partitions in
     * reverse order, so that no partition relies on an earlier one having been built. {@code keep}
     * picks the rows a frame keeps, by row id, as the build filters would; null keeps every row and
     * starts the build as the operator starts one without filters, whose frames keep every row. On
     * failure the build is closed, as the operator closes it once the failed round has drained.
     */
    static FrozenHashJoinBuild.IntKeyed buildIntPartitioned(
            CairoConfiguration configuration,
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            int keyColumn,
            long rowsPerPartition,
            long keyCountHint,
            @Nullable LongPredicate keep,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        return buildIntPartitioned(configuration, build, frames, keyColumn, rowsPerPartition, keyCountHint, keep, NO_ORDER,
                memoryTracker, circuitBreaker);
    }

    /**
     * Builds as {@link #buildIntPartitioned(CairoConfiguration, IntHashJoinBuild, HashJoinBuildFrames, int, long, long,
     * LongPredicate, MemoryTracker, SqlExecutionCircuitBreaker)} does, every frame task walking its frame's rows, or the
     * rows it keeps, in the given row order, as the operator's frame tasks walk the rows of a scan in that order;
     * {@link #NO_ORDER} partitions the frames through the overloads that take no row order.
     */
    static FrozenHashJoinBuild.IntKeyed buildIntPartitioned(
            CairoConfiguration configuration,
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            int keyColumn,
            long rowsPerPartition,
            long keyCountHint,
            @Nullable LongPredicate keep,
            int order,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        build.open(memoryTracker, circuitBreaker);
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             DirectLongList rows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
            if (keep == null) {
                build.beginUnfilteredPartitioning(frameRowCounts(frames), rowsPerPartition);
            } else {
                build.beginPartitioning(frames.getFrameCount(), frames.getRowCount(), rowsPerPartition);
            }
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                final long rowCount = frames.getFrameRowCount(frameIndex);
                if (keep == null) {
                    if (order == NO_ORDER) {
                        build.partitionFrame(frameIndex, record, keyColumn, rowCount);
                    } else {
                        build.partitionFrame(frameIndex, record, keyColumn, rowCount, order);
                    }
                } else {
                    keptRows(frameIndex, rowCount, keep, rows);
                    if (order == NO_ORDER) {
                        build.partitionFrame(frameIndex, record, keyColumn, rows);
                    } else {
                        build.partitionFrame(frameIndex, record, keyColumn, rows, order);
                    }
                }
            }
            final int partitionCount = build.planPartitions(rowsPerPartition, keyCountHint);
            for (int partition = partitionCount - 1; partition >= 0; partition--) {
                build.buildPartition(partition, circuitBreaker);
            }
            return build.freezePartitioned(frames);
        } catch (Throwable th) {
            build.close();
            throw th;
        }
    }

    /**
     * The staged-key twin of {@link #buildIntPartitioned}: every frame task stages its keys through
     * the given sink and one stager of the build's, which this thread keeps for the frame tasks and
     * closes once they are done, as the operator closes its worker slots' own.
     */
    static FrozenHashJoinBuild.RecordKeyed buildMapPartitioned(
            CairoConfiguration configuration,
            MapHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordSink keySink,
            long rowsPerPartition,
            long keyCountHint,
            @Nullable LongPredicate keep,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        return buildMapPartitioned(configuration, build, frames, keySink, rowsPerPartition, keyCountHint, keep, NO_ORDER,
                memoryTracker, circuitBreaker);
    }

    /**
     * The staged-key twin of {@link #buildIntPartitioned(CairoConfiguration, IntHashJoinBuild, HashJoinBuildFrames, int,
     * long, long, LongPredicate, int, MemoryTracker, SqlExecutionCircuitBreaker)}.
     */
    static FrozenHashJoinBuild.RecordKeyed buildMapPartitioned(
            CairoConfiguration configuration,
            MapHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordSink keySink,
            long rowsPerPartition,
            long keyCountHint,
            @Nullable LongPredicate keep,
            int order,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        build.open(memoryTracker, circuitBreaker);
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             DirectLongList rows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
             MapHashJoinBuild.KeyStager stager = build.newKeyStager()) {
            if (keep == null) {
                build.beginUnfilteredPartitioning(frameRowCounts(frames), rowsPerPartition);
            } else {
                build.beginPartitioning(frames.getFrameCount(), frames.getRowCount(), rowsPerPartition);
            }
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                final long rowCount = frames.getFrameRowCount(frameIndex);
                if (keep == null) {
                    if (order == NO_ORDER) {
                        build.partitionFrame(frameIndex, record, keySink, stager, rowCount);
                    } else {
                        build.partitionFrame(frameIndex, record, keySink, stager, rowCount, order);
                    }
                } else {
                    keptRows(frameIndex, rowCount, keep, rows);
                    if (order == NO_ORDER) {
                        build.partitionFrame(frameIndex, record, keySink, stager, rows);
                    } else {
                        build.partitionFrame(frameIndex, record, keySink, stager, rows, order);
                    }
                }
            }
            stager.close();
            final int partitionCount = build.planPartitions(rowsPerPartition, keyCountHint);
            for (int partition = partitionCount - 1; partition >= 0; partition--) {
                build.buildPartition(partition, circuitBreaker);
            }
            return build.freezePartitioned(frames);
        } catch (Throwable th) {
            build.close();
            throw th;
        }
    }

    static FrozenHashJoinBuild.RecordKeyed buildMap(
            CairoConfiguration configuration,
            MapHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordCursorFactory factory,
            RecordSink keySink,
            SqlExecutionContext context
    ) throws SqlException {
        frames.of(factory, context);
        build.open(context.getMemoryTracker(), context.getCircuitBreaker());
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                build.appendFrame(record, keySink, frames.getFrameRowCount(frameIndex), ORDER_ASC);
            }
        }
        return build.freeze(frames);
    }

    /**
     * Builds from the walked frames as the fused operator's owner does, a frame at a time in the
     * frames' order: {@code keep} picks the rows a frame keeps, by row id, as the build filters
     * would, in a row list that runs from the frame's first row, and null appends every row. Each
     * frame's rows, or the rows it keeps, go in in the given row order. On failure the build is
     * closed, as the build closes itself.
     */
    static FrozenHashJoinBuild.IntKeyed buildIntSerial(
            CairoConfiguration configuration,
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            int keyColumn,
            @Nullable LongPredicate keep,
            int order,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        build.open(memoryTracker, circuitBreaker);
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             DirectLongList rows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                final long rowCount = frames.getFrameRowCount(frameIndex);
                if (keep == null) {
                    build.appendFrame(record, keyColumn, rowCount, order);
                } else {
                    keptRows(frameIndex, rowCount, keep, rows);
                    build.appendFrame(record, keyColumn, rows, order);
                }
            }
            return build.freeze(frames);
        } catch (Throwable th) {
            build.close();
            throw th;
        }
    }

    /**
     * The staged-key twin of {@link #buildIntSerial}.
     */
    static FrozenHashJoinBuild.RecordKeyed buildMapSerial(
            CairoConfiguration configuration,
            MapHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordSink keySink,
            @Nullable LongPredicate keep,
            int order,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        build.open(memoryTracker, circuitBreaker);
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             DirectLongList rows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                final long rowCount = frames.getFrameRowCount(frameIndex);
                if (keep == null) {
                    build.appendFrame(record, keySink, rowCount, order);
                } else {
                    keptRows(frameIndex, rowCount, keep, rows);
                    build.appendFrame(record, keySink, rows, order);
                }
            }
            return build.freeze(frames);
        } catch (Throwable th) {
            build.close();
            throw th;
        }
    }

    /**
     * Counts the key rows of a frozen parallel build as the operator's count round does, a task per
     * partition, running every task on this thread, in reverse order, so that no partition relies on
     * an earlier one having been counted, and then ends the count. On failure the build is closed, as
     * the operator closes it once the failed round has drained.
     */
    static void countIntPartitions(IntHashJoinBuild build, SqlExecutionCircuitBreaker circuitBreaker) {
        countIntPartitions(build, 1, circuitBreaker);
    }

    /**
     * Counts as {@link #countIntPartitions(IntHashJoinBuild, SqlExecutionCircuitBreaker)} does, a task
     * per this many consecutive partitions, which must divide the partition count.
     */
    static void countIntPartitions(IntHashJoinBuild build, int partitionsPerTask, SqlExecutionCircuitBreaker circuitBreaker) {
        try {
            Assert.assertEquals(0, build.getPartitionCount() % partitionsPerTask);
            for (int partitionLo = build.getPartitionCount() - partitionsPerTask; partitionLo >= 0; partitionLo -= partitionsPerTask) {
                build.countPartitionKeyRows(partitionLo, partitionLo + partitionsPerTask, circuitBreaker);
            }
            build.endPartitionedKeyRowCount();
        } catch (Throwable th) {
            build.close();
            throw th;
        }
    }

    /**
     * The staged-key twin of {@link #countIntPartitions(IntHashJoinBuild, SqlExecutionCircuitBreaker)}.
     */
    static void countMapPartitions(MapHashJoinBuild build, SqlExecutionCircuitBreaker circuitBreaker) {
        countMapPartitions(build, 1, circuitBreaker);
    }

    /**
     * The staged-key twin of {@link #countIntPartitions(IntHashJoinBuild, int, SqlExecutionCircuitBreaker)}.
     */
    static void countMapPartitions(MapHashJoinBuild build, int partitionsPerTask, SqlExecutionCircuitBreaker circuitBreaker) {
        try {
            Assert.assertEquals(0, build.getPartitionCount() % partitionsPerTask);
            for (int partitionLo = build.getPartitionCount() - partitionsPerTask; partitionLo >= 0; partitionLo -= partitionsPerTask) {
                build.countPartitionKeyRows(partitionLo, partitionLo + partitionsPerTask, circuitBreaker);
            }
            build.endPartitionedKeyRowCount();
        } catch (Throwable th) {
            build.close();
            throw th;
        }
    }

    /**
     * The row count of every open frame, in frame order, as the operator hands a build that keeps every row.
     */
    static LongList frameRowCounts(HashJoinBuildFrames frames) {
        final LongList rowCounts = new LongList();
        for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
            rowCounts.add(frames.getFrameRowCount(frameIndex));
        }
        return rowCounts;
    }

    /**
     * Picks the rows of the walked frames whose payload column holds a value that {@code keepValue}
     * accepts, by row id, as a build filter on that column would pick them.
     */
    static LongPredicate keepByValue(CairoConfiguration configuration, HashJoinBuildFrames frames, int column, LongPredicate keepValue) {
        final Set<Long> kept = new HashSet<>();
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                for (long row = 0, n = frames.getFrameRowCount(frameIndex); row < n; row++) {
                    record.setRowIndex(row);
                    if (keepValue.test(record.getLong(column))) {
                        kept.add(Rows.toRowID(frameIndex, row));
                    }
                }
            }
        }
        return kept::contains;
    }

    /**
     * Each key's payload values in the order that the serial hash join's {@link LongChain} lists the
     * key's rows for the factory's record cursor, the reference for a build over the frames of the
     * same scan. {@code populateRowIDHashMap()} of {@code HashJoinLightRecordCursorFactory} puts the
     * row id of every row the cursor yields at the head of its key's chain, and the join walks the
     * chain from its head, reading each row back through {@code recordAt()}; so does this. The cursor
     * keeps the rows whose payload value {@code keepValue} accepts, as a filter under the join would;
     * null keeps every row. A NULL key is a key of its own, as the join's sinks make it.
     */
    static <K> Map<K, List<Long>> longChains(
            RecordCursorFactory factory,
            Function<Record, K> keyOf,
            int payloadColumn,
            @Nullable LongPredicate keepValue,
            SqlExecutionContext context
    ) throws SqlException {
        final Map<K, Integer> heads = new HashMap<>();
        final Map<K, List<Long>> chains = new HashMap<>();
        try (RecordCursor cursor = factory.getCursor(context);
             LongChain chain = new LongChain(1_024, Integer.MAX_VALUE)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (keepValue == null || keepValue.test(record.getLong(payloadColumn))) {
                    final K key = keyOf.apply(record);
                    final Integer head = heads.get(key);
                    heads.put(key, chain.put(record.getRowId(), head != null ? head : -1));
                }
            }
            final Record rowRecord = cursor.getRecordB();
            for (Map.Entry<K, Integer> entry : heads.entrySet()) {
                final List<Long> values = new ArrayList<>();
                final LongChain.Cursor rows = chain.getCursor(entry.getValue());
                while (rows.hasNext()) {
                    cursor.recordAt(rowRecord, rows.next());
                    values.add(rowRecord.getLong(payloadColumn));
                }
                chains.put(entry.getKey(), values);
            }
        }
        return chains;
    }

    /**
     * The page frame order of the factory's scan, as an operator over it derives it from the
     * factory's scan direction.
     */
    static int scanOrder(RecordCursorFactory factory) {
        return factory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
    }

    /**
     * A copy of the row heap of an open build, either kind: every row's link, or once counted its
     * chain length, and its row id when it has one. Two builds over the same frames with the same
     * partitions lay their rows out alike, so their copies compare byte for byte.
     */
    static byte[] heapBytes(Object build) {
        try {
            final Object heap = field(build, "heap");
            final long address = (long) field(field(heap, "rows"), "address");
            final long size = (long) field(heap, "rowBytes");
            final byte[] bytes = new byte[(int) size];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = Unsafe.getByte(address + i);
            }
            return bytes;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertCounts(FrozenHashJoinBuild frozen, Map<?, List<Long>> expected) {
        long rowCount = 0;
        for (List<Long> chain : expected.values()) {
            rowCount += chain.size();
        }
        Assert.assertEquals(rowCount, frozen.getRowCount());
        Assert.assertEquals(expected.size(), frozen.getKeyCount());
    }

    // The rows of the frame that keep picks, by row id, from the frame's first row, as a filter lists them.
    private static void keptRows(int frameIndex, long rowCount, LongPredicate keep, DirectLongList rows) {
        rows.clear();
        for (long row = 0; row < rowCount; row++) {
            if (keep.test(Rows.toRowID(frameIndex, row))) {
                rows.add(row);
            }
        }
    }

    private static Object field(Object owner, String name) throws ReflectiveOperationException {
        final Field field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    /**
     * Cancels a parallel build at the first breaker check that runs as it starts partitioning, inside
     * {@code HashJoinPartitions.begin()}. A check that runs while the chunk table is allocated but the
     * bucket table is not runs inside the chunk table's clear, before it zeroes a byte, so the table
     * holds whatever malloc left in its block. The breaker stands in for those bytes with an entry
     * that names the given block, which the build does not own, and zeroes the other entries.
     */
    static final class PartitioningStartBreaker extends CountingSqlExecutionCircuitBreaker {
        private final Object build;
        private final long foreignBlock;
        private final long foreignBlockSize;
        private final StackWalker walker = StackWalker.getInstance();
        private boolean isTripped;

        PartitioningStartBreaker(Object build, long foreignBlock, long foreignBlockSize) {
            super(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            this.build = build;
            this.foreignBlock = foreignBlock;
            this.foreignBlockSize = foreignBlockSize;
        }

        boolean isTripped() {
            return isTripped;
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            if (isTripped || !walker.walk(frames -> frames.anyMatch(frame ->
                    frame.getClassName().endsWith(".HashJoinPartitions") && frame.getMethodName().equals("begin")))) {
                return;
            }
            isTripped = true;
            try {
                final Object partitions = field(build, "partitions");
                final Object chunks = field(partitions, "chunks");
                final long table = (long) field(chunks, "address");
                if (table != 0 && (long) field(field(partitions, "bucketStarts"), "address") == 0) {
                    Vect.memset(table, (long) field(chunks, "capacity"), 0);
                    Unsafe.putLong(table, foreignBlock);
                    Unsafe.putLong(table + Long.BYTES, foreignBlockSize);
                }
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
            throw CairoException.queryCancelled(1);
        }
    }
}
