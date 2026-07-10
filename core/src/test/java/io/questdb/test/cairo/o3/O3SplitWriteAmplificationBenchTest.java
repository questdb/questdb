/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Write-amplification bench for the multi-source lagging-writers ingestion pattern
 * (see PARTITION_SPLIT_POLICY.md). Not a regression test: it prints, per scenario,
 * total rows ingested vs rows physically written (including squash copies), the
 * resulting piece count, and wall time. A light row-count assertion keeps it honest.
 * <p>
 * Workload: a base table of {@code bench.partitions} day partitions holding
 * {@code bench.rowsPerPartition} rows each (last partition half full, so the tail is
 * live), then {@code bench.writers} WAL writers run for {@code bench.steps} virtual
 * seconds. Each writer commits on its own cadence with its own batch size and writes
 * chronologically at its own fixed offset in the past; the first two are near-realtime
 * with timestamp jitter, deeper ones lag by 1 minute, 1 hour, 1 day (then +1 day each).
 * The WAL queue is drained after every virtual step, mimicking continuous apply.
 * <p>
 * Scale knobs (system properties): bench.partitions=3, bench.rowsPerPartition=1000000,
 * bench.steps=120, bench.stepSeconds=1, bench.writers=5.
 */
public class O3SplitWriteAmplificationBenchTest extends AbstractCairoTest {

    private static final long BASE_START = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final int PARTITIONS = Integer.getInteger("bench.partitions", 3);
    private static final long ROWS_PER_PARTITION = Long.getLong("bench.rowsPerPartition", 1_000_000L);
    private static final long STEP_MICROS = Long.getLong("bench.stepSeconds", 1) * Micros.SECOND_MICROS;
    private static final int STEPS = Integer.getInteger("bench.steps", 120);
    private static final String[] SYMBOLS = {"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"};
    private static final int WRITERS = Integer.getInteger("bench.writers", 5);
    private int scenarioIndex;

    @Test
    public void testMultiWriterPatternWriteAmplification() throws Exception {
        assertMemoryLeak(() -> {
            System.out.printf(
                    "%nbench config: partitions=%d, rowsPerPartition=%d, steps=%d, stepSeconds=%d, writers=%d%n",
                    PARTITIONS, ROWS_PER_PARTITION, STEPS, STEP_MICROS / Micros.SECOND_MICROS, WRITERS
            );
            for (WriterSpec spec : buildWriterSpecs()) {
                System.out.printf(
                        "  writer %-10s offset=%-9s rowsPerCommit=%-6d commitEvery=%ds jitter=%ds%n",
                        spec.name,
                        spec.offsetMicros / Micros.SECOND_MICROS + "s",
                        spec.rowsPerCommit,
                        spec.cadenceSteps * STEP_MICROS / Micros.SECOND_MICROS,
                        spec.jitterMicros / Micros.SECOND_MICROS
                );
            }
            System.out.printf(
                    "%n%-34s %12s %14s %6s %10s %7s %8s %9s%n",
                    "scenario", "ingested", "physical", "amp", "max_pieces", "pieces", "logical", "time_ms"
            );
            runScenario("splits-off (shipped defaults)", false, 1, 20);
            runScenario("hardlink-on, mid cap 1 (defaults)", true, 1, 20);
            runScenario("hardlink-on, mid cap 5", true, 5, 20);
            runScenario("hardlink-on, mid+last cap 100", true, 100, 100);
            runScenario("policy: mid 9/fold 3, last free", true, 9, 40, 4 * 1024 * 1024, 3);
            runScenario("policy: mid 9/3, last 100, endfold", true, 9, 100, 4 * 1024 * 1024, 3);
            runScenario("policy: same, min.size 1MB", true, 9, 100, 1024 * 1024, 3);
            runScenario("policy: min 1MB, last capped 40", true, 9, 40, 1024 * 1024, 3);
        });
    }

    /**
     * Catch-up variant: the same writer schedule commits all its WAL transactions
     * up front, then a single drain applies the whole backlog. Commit sizing is
     * bounded to ~9k rows (maxUncommittedRows 9000, lag multiplier 1.0) so the
     * backlog applies in about as many storage commits as the interleaved bench's
     * 120 drains, and no commit is forced by the per-drain window end: the real
     * commitToTimestamp/lag machinery governs.
     */
    @Test
    public void testCatchUpWriteAmplification() throws Exception {
        assertMemoryLeak(() -> {
            System.out.printf(
                    "%ncatch-up bench: partitions=%d, rowsPerPartition=%d, steps=%d, writers=%d%n",
                    PARTITIONS, ROWS_PER_PARTITION, STEPS, WRITERS
            );
            System.out.printf(
                    "%n%-34s %12s %14s %6s %8s %7s %8s %9s%n",
                    "scenario", "ingested", "physical", "amp", "commits", "pieces", "logical", "time_ms"
            );
            runCatchUpScenario("CU splits-off", false, 1, 20, 4 * 1024 * 1024);
            runCatchUpScenario("CU hardlink defaults", true, 1, 20, 4 * 1024 * 1024);
            runCatchUpScenario("CU hardlink mid 9, last 40", true, 9, 40, 4 * 1024 * 1024);
            runCatchUpScenario("CU hardlink cap 100", true, 100, 100, 4 * 1024 * 1024);
            runCatchUpScenario("CU hardlink cap 100, min 1MB", true, 100, 100, 1024 * 1024);
        });
    }

    private static WriterSpec[] buildWriterSpecs() {
        long maxOffset = (long) ((PARTITIONS - 1) * Micros.DAY_MICROS);
        WriterSpec[] specs = new WriterSpec[WRITERS];
        for (int i = 0; i < WRITERS; i++) {
            specs[i] = switch (i) {
                case 0 -> new WriterSpec("realtime", 0, 2 * Micros.SECOND_MICROS, 1_000, 1);
                case 1 -> new WriterSpec("rt-5s", 5 * Micros.SECOND_MICROS, 5 * Micros.SECOND_MICROS, 500, 1);
                case 2 -> new WriterSpec("lag-1m", Micros.MINUTE_MICROS, 0, 2_000, 2);
                case 3 -> new WriterSpec("lag-1h", Micros.HOUR_MICROS, 0, 10_000, 5);
                // 45k x 12 commits = 540k into the past partition, matching the 540k the
                // four same-day writers produce - a 50/50 today/past split.
                default -> new WriterSpec(
                        "lag-" + (i - 3) + "d",
                        Math.min((i - 3) * Micros.DAY_MICROS, maxOffset),
                        0,
                        45_000,
                        10
                );
            };
        }
        return specs;
    }

    private static long emitBatch(WalWriter walWriter, WriterSpec spec, long virtualNow, Rnd rnd) {
        long end = virtualNow - spec.offsetMicros;
        if (spec.jitterMicros > 0) {
            end += rnd.nextLong(2 * spec.jitterMicros + 1) - spec.jitterMicros;
        }
        if (end <= spec.lastTs) {
            end = spec.lastTs + spec.rowsPerCommit;
        }
        long delta = Math.max(1, (end - spec.lastTs) / spec.rowsPerCommit);
        for (int i = 1; i <= spec.rowsPerCommit; i++) {
            TableWriter.Row row = walWriter.newRow(spec.lastTs + i * delta);
            row.putSym(1, SYMBOLS[rnd.nextInt(SYMBOLS.length)]);
            row.putDouble(2, rnd.nextDouble());
            row.putLong(3, rnd.nextLong(100_000));
            row.append();
        }
        spec.lastTs += spec.rowsPerCommit * delta;
        walWriter.commit();
        return spec.rowsPerCommit;
    }

    private void runScenario(String name, boolean isHardlinkSplitEnabled, int midMaxSplits, int lastMaxSplits) throws Exception {
        runScenario(name, isHardlinkSplitEnabled, midMaxSplits, lastMaxSplits, 4 * 1024 * 1024, 0);
    }

    /**
     * foldTo > 0 activates an external policy controller emulating the planned
     * decoupled admission-cap/fold-target behavior: when any logical partition
     * approaches the admission cap, both caps are pulsed down to foldTo for one
     * drain (housekeep folds the oldest pieces - those behind the laggard's
     * floor), then restored. After the run both caps drop to 1 and two more
     * commits let housekeep consolidate whatever remains.
     */
    private void runScenario(String name, boolean isHardlinkSplitEnabled, int midMaxSplits, int lastMaxSplits, long splitMinSize, int foldTo) throws Exception {
        // Pin values the test-env defaults diverge on (1000 max uncommitted rows, lag
        // txn cap 20). Split min size default is 4MB, not the production 50MB: the 50MB
        // floor blocks all classic tail-piece splits at this bench scale (see git history).
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 0); // TEMP: force min.size 0 (splitMinSize arg ignored)
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, String.valueOf(isHardlinkSplitEnabled));
        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, midMaxSplits);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, lastMaxSplits);

        String tbl = "bench" + scenarioIndex++;
        execute("CREATE TABLE " + tbl + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        long tsStep = Micros.DAY_MICROS / ROWS_PER_PARTITION;
        long baseRows = PARTITIONS * ROWS_PER_PARTITION - ROWS_PER_PARTITION / 2;
        execute("INSERT INTO " + tbl + " SELECT timestamp_sequence(" + BASE_START + ", " + tsStep + ")," +
                " rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7'), rnd_double(), rnd_long(0, 100000, 0)" +
                " FROM long_sequence(" + baseRows + ")");
        drainWalQueue();

        long physicalBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        long startNanos = System.nanoTime();
        long virtualNow = BASE_START + baseRows * tsStep;
        long ingested = 0;

        TableToken token = engine.verifyTableName(tbl);
        long maxPieces = 0;
        WriterSpec[] specs = buildWriterSpecs();
        WalWriter[] writers = new WalWriter[specs.length];
        Rnd rnd = new Rnd();
        try {
            for (int w = 0; w < specs.length; w++) {
                writers[w] = engine.getWalWriter(token);
                specs[w].lastTs = virtualNow - specs[w].offsetMicros - specs[w].cadenceSteps * STEP_MICROS;
            }
            boolean isFoldPulseActive = false;
            for (int step = 1; step <= STEPS; step++) {
                virtualNow += STEP_MICROS;
                for (int w = 0; w < specs.length; w++) {
                    if (step % specs[w].cadenceSteps == 0) {
                        ingested += emitBatch(writers[w], specs[w], virtualNow, rnd);
                    }
                }
                drainWalQueue();
                long[] sample = samplePieces(token);
                maxPieces = Math.max(maxPieces, sample[0]);
                if (foldTo > 0) {
                    if (isFoldPulseActive) {
                        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, midMaxSplits);
                        isFoldPulseActive = false;
                    } else if (sample[1] >= midMaxSplits - 2 && (step + 1) % 10 != 0) {
                        // a mid group is close to the admission cap: fold it behind the
                        // laggard's floor on the next drain (skip if the deep laggard commits
                        // next step, so the pulse never blocks its split admission). The last
                        // logical partition is deliberately never pulse-folded: all its pieces
                        // sit inside the 1h writer's band, and folding hot pieces only buys
                        // re-merges of the fold target.
                        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, foldTo);
                        isFoldPulseActive = true;
                    }
                }
            }
            if (foldTo > 0) {
                // end of ingestion: consolidate everything; two commit+drain rounds because
                // a fold can unblock the next one (fresh targets appear as pieces vanish)
                setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1);
                setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1);
                for (int i = 0; i < 2; i++) {
                    virtualNow += STEP_MICROS;
                    ingested += emitBatch(writers[0], specs[0], virtualNow, rnd);
                    drainWalQueue();
                }
            }
        } finally {
            for (WalWriter walWriter : writers) {
                if (walWriter != null) {
                    walWriter.close();
                }
            }
        }
        drainWalQueue();

        long physical = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalBefore;
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        maxPieces = Math.max(maxPieces, samplePieces(token)[0]);
        try (TableReader reader = engine.getReader(token)) {
            TxReader tx = reader.getTxFile();
            int pieces = tx.getPartitionCount();
            int logical = 0;
            long prevLogicalTs = Long.MIN_VALUE;
            for (int i = 0; i < pieces; i++) {
                long logicalTs = tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(i));
                if (logicalTs != prevLogicalTs) {
                    logical++;
                    prevLogicalTs = logicalTs;
                }
            }
            System.out.printf(
                    "%-34s %12d %14d %6.1f %10d %7d %8d %9d%n",
                    name, ingested, physical, (double) physical / ingested, maxPieces, pieces, logical, elapsedMs
            );
            Assert.assertEquals(baseRows + ingested, reader.size());
        }
    }

    private void runCatchUpScenario(String name, boolean isHardlinkSplitEnabled, int midMaxSplits, int lastMaxSplits, long splitMinSize) throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 0); // TEMP: force min.size 0 (splitMinSize arg ignored)
        // bound each storage commit to ~9k rows: with ~1.08M backlog rows this yields
        // ~120 commits, matching the interleaved bench's drain count
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 9_000);
        setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, "1.0");
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        setProperty(PropertyKey.CAIRO_PARTITION_TOP_WAL_ENABLED, String.valueOf(isHardlinkSplitEnabled));
        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, midMaxSplits);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, lastMaxSplits);

        String tbl = "bench" + scenarioIndex++;
        execute("CREATE TABLE " + tbl + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        long tsStep = Micros.DAY_MICROS / ROWS_PER_PARTITION;
        long baseRows = PARTITIONS * ROWS_PER_PARTITION - ROWS_PER_PARTITION / 2;
        execute("INSERT INTO " + tbl + " SELECT timestamp_sequence(" + BASE_START + ", " + tsStep + ")," +
                " rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7'), rnd_double(), rnd_long(0, 100000, 0)" +
                " FROM long_sequence(" + baseRows + ")");
        drainWalQueue();

        long physicalBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        long commitsBefore = engine.getMetrics().tableWriterMetrics().getCommitCount();
        long startNanos = System.nanoTime();
        long virtualNow = BASE_START + baseRows * tsStep;
        long ingested = 0;

        TableToken token = engine.verifyTableName(tbl);
        WriterSpec[] specs = buildWriterSpecs();
        WalWriter[] writers = new WalWriter[specs.length];
        Rnd rnd = new Rnd();
        try {
            for (int w = 0; w < specs.length; w++) {
                writers[w] = engine.getWalWriter(token);
                specs[w].lastTs = virtualNow - specs[w].offsetMicros - specs[w].cadenceSteps * STEP_MICROS;
            }
            // phase 1: commit the full schedule to WAL, no apply
            for (int step = 1; step <= STEPS; step++) {
                virtualNow += STEP_MICROS;
                for (int w = 0; w < specs.length; w++) {
                    if (step % specs[w].cadenceSteps == 0) {
                        ingested += emitBatch(writers[w], specs[w], virtualNow, rnd);
                    }
                }
            }
        } finally {
            for (WalWriter walWriter : writers) {
                if (walWriter != null) {
                    walWriter.close();
                }
            }
        }
        // phase 2: catch up
        drainWalQueue();

        long physical = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalBefore;
        long commits = engine.getMetrics().tableWriterMetrics().getCommitCount() - commitsBefore;
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        try (TableReader reader = engine.getReader(token)) {
            TxReader tx = reader.getTxFile();
            int pieces = tx.getPartitionCount();
            int logical = 0;
            long prevLogicalTs = Long.MIN_VALUE;
            for (int i = 0; i < pieces; i++) {
                long logicalTs = tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(i));
                if (logicalTs != prevLogicalTs) {
                    logical++;
                    prevLogicalTs = logicalTs;
                }
            }
            System.out.printf(
                    "%-34s %12d %14d %6.1f %8d %7d %8d %9d%n",
                    name, ingested, physical, (double) physical / ingested, commits, pieces, logical, elapsedMs
            );
            Assert.assertEquals(baseRows + ingested, reader.size());
        }
    }

    private long[] samplePieces(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            TxReader tx = reader.getTxFile();
            int pieces = tx.getPartitionCount();
            long lastLogicalTs = pieces > 0
                    ? tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(pieces - 1))
                    : Long.MIN_VALUE;
            int maxMidGroup = 0;
            int group = 0;
            long prevLogicalTs = Long.MIN_VALUE;
            for (int i = 0; i < pieces; i++) {
                long logicalTs = tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(i));
                if (logicalTs != prevLogicalTs) {
                    prevLogicalTs = logicalTs;
                    group = 1;
                } else {
                    group++;
                }
                if (logicalTs != lastLogicalTs) {
                    maxMidGroup = Math.max(maxMidGroup, group);
                }
            }
            return new long[]{pieces, maxMidGroup};
        }
    }

    private static class WriterSpec {
        final int cadenceSteps;
        final long jitterMicros;
        final String name;
        final long offsetMicros;
        final int rowsPerCommit;
        long lastTs;

        WriterSpec(String name, long offsetMicros, long jitterMicros, int rowsPerCommit, int cadenceSteps) {
            this.name = name;
            this.offsetMicros = offsetMicros;
            this.jitterMicros = jitterMicros;
            this.rowsPerCommit = rowsPerCommit;
            this.cadenceSteps = cadenceSteps;
        }
    }
}
