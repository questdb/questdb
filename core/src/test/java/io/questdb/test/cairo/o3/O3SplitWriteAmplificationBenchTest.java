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
 * Write-amplification and end-of-run dead-space bench for the multi-source lagging-writers ingestion
 * pattern. Ported from the enterprise {@code feat-partition-top-split} branch's
 * {@code O3SplitWriteAmplificationBenchTest}, whose SPLIT design (one {@code _txn} record per piece)
 * predates this branch's composite-partition design (one {@code _txn} record per DIRECTORY, pieces tracked
 * inside {@code PartitionGeometry} - see {@code cmposite_partition_like_parquet_partition.md}). Dead-row
 * accounting is rewritten against {@link TableReader#getPartitionPhysicalRowCount} (there is no
 * folder-level dead/live table on this branch, unlike the ported-from branch's
 * {@code TxReader.getFolderDeadRows}/{@code getFolderLiveRows}). Not ported: piece/split-sibling counting,
 * and {@code TableWriter.benchSquashFullThreshold} / {@code benchForceBlockApply} /
 * {@code benchForceOneByOne} - test-only static hooks the old branch added to {@code TableWriter} itself,
 * with no equivalent here. Scenarios that depended on them (the squash-strategy A/B toggle, the forced
 * block-apply path) are dropped rather than faked.
 * <p>
 * Not a regression test: it prints, per scenario, two numbers - write amplification (rows physically
 * written, squash and compaction copies included, per row ingested) and dead space at the end of the run
 * (rows the column files hold that no longer belong to any live row, as a share of all rows the files
 * hold). A light row-count assertion keeps it honest.
 * <p>
 * Workload: a base table of {@code bench.partitions} day partitions holding {@code bench.rowsPerPartition}
 * rows each (last partition half full, so the tail is live), then {@code bench.writers} WAL writers run for
 * {@code bench.steps} virtual seconds. Each writer commits on its own cadence with its own batch size and
 * writes chronologically at its own fixed offset in the past; the first two are near-realtime with
 * timestamp jitter, deeper ones lag by 1 minute, 1 hour, 1 day (then +1 day each). The WAL queue is drained
 * after every virtual step, mimicking continuous apply.
 * <p>
 * Scale knobs (system properties): bench.partitions=3, bench.rowsPerPartition=1000000, bench.steps=120,
 * bench.stepSeconds=1, bench.writers=5.
 */
public class O3SplitWriteAmplificationBenchTest extends AbstractCairoTest {

    private static final long BASE_START = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final int PARTITIONS = Integer.getInteger("bench.partitions", 3);
    private static final long ROWS_PER_PARTITION = Long.getLong("bench.rowsPerPartition", 1_000_000L);
    private static final long STEP_MICROS = Long.getLong("bench.stepSeconds", 1) * Micros.SECOND_MICROS;
    private static final int STEPS = Integer.getInteger("bench.steps", 120);
    private static final String[] SYMBOLS = {"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"};
    private static final int WRITERS = Integer.getInteger("bench.writers", 5);
    // Set around a scenario call to turn partition compaction on for it. Compaction copies feed the
    // same physicallyWrittenRows counter the amp column is built from, so it RAISES amp on purpose -
    // reclaiming dead space is the other half of that trade.
    private boolean compactionEnabled;
    // Cooling-off to give compaction, in microseconds. The bench compresses 120 VIRTUAL seconds of
    // ingestion into a few real ones while the cooling-off timer runs on the WALL clock, so at the 10m
    // default compaction fires about twice per run whatever the workload does. "hot" sets it to 0 to
    // show the other end: what continuous compaction costs and reclaims.
    private String compactionCooldown = "10m";
    // cairo.partition.compaction.max.pieces for the next scenario. The piece-count rule fires when a
    // folder holds more geometry pieces than this.
    private int compactionMaxPieces = 1000;
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
            printHeader();
            runScenario("split 10/10");
            compactionEnabled = true;
            runScenario("split 10/10 + compaction");
            compactionCooldown = "0";
            runScenario("split 10/10 + compaction (hot)");
            // Piece-count sweep. Cooling-off stays at 0 so the piece-count RULE is what binds
            // rather than the timer: at the 10m default compaction fires about twice per run
            // whatever the setting says, and the sweep would measure the clock instead.
            for (int maxPieces : new int[]{50, 20, 10}) {
                compactionMaxPieces = maxPieces;
                runScenario("compaction, max.pieces=" + maxPieces);
            }
            resetCompactionSettings();
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
            printHeader();
            runCatchUpScenario("split 10/10");
            compactionEnabled = true;
            runCatchUpScenario("split 10/10 + compaction");
            compactionCooldown = "0";
            runCatchUpScenario("split 10/10 + compaction (hot)");
            for (int maxPieces : new int[]{50, 20, 10}) {
                compactionMaxPieces = maxPieces;
                runCatchUpScenario("compaction, max.pieces=" + maxPieces);
            }
            resetCompactionSettings();
        });
    }

    /**
     * Random-order variant: same base data and roughly the same commit count / ingest volume as
     * the other benches, but every inserted row lands at a uniformly random timestamp across the
     * whole existing data span - zero chronological locality. Maximal O3 stress: each commit merges
     * into arbitrary partitions.
     */
    @Test
    public void testRandomOrderWriteAmplification() throws Exception {
        assertMemoryLeak(() -> {
            System.out.printf(
                    "%nrandom-order bench: partitions=%d, rowsPerPartition=%d, steps=%d%n",
                    PARTITIONS, ROWS_PER_PARTITION, STEPS
            );
            printHeader();
            runRandomScenario("split 10/10");
            compactionEnabled = true;
            runRandomScenario("split 10/10 + compaction");
            compactionCooldown = "0";
            runRandomScenario("split 10/10 + compaction (hot)");
            for (int maxPieces : new int[]{50, 20, 10}) {
                compactionMaxPieces = maxPieces;
                runRandomScenario("compaction, max.pieces=" + maxPieces);
            }
            resetCompactionSettings();
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

    /**
     * Dead rows across every directory the table holds, right now: {@code physical - live}, summed. A
     * folder's physical extent is {@link TableReader#getPartitionPhysicalRowCount} ({@code E} in
     * {@code PARTITION_COMPACTION.md} vocabulary, dead space included); its live rows are
     * {@link TxReader#getPartitionSize}. [0]=dead rows, [1]=live rows.
     */
    private static long[] deadSpace(TableReader reader) {
        final TxReader tx = reader.getTxFile();
        final int folders = tx.getPartitionCount();
        long dead = 0;
        long live = 0;
        for (int i = 0; i < folders; i++) {
            final long liveRows = tx.getPartitionSize(i);
            final long physicalRows = reader.getPartitionPhysicalRowCount(i);
            live += liveRows;
            dead += Math.max(0, physicalRows - liveRows);
        }
        return new long[]{dead, live};
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

    private static void printHeader() {
        System.out.printf(
                "%n%-34s %12s %14s %6s %12s %9s %9s%n",
                "scenario", "ingested", "physical", "amp", "dead_rows", "dead%", "time_ms"
        );
    }

    private static void printSummary(String name, long ingested, long physical, long baseRows, long elapsedMs, TableToken token) throws Exception {
        try (TableReader reader = engine.getReader(token)) {
            final long[] dead = deadSpace(reader);
            final long deadRows = dead[0];
            final long liveRows = dead[1];
            final double deadPct = deadRows + liveRows > 0 ? 100.0 * deadRows / (deadRows + liveRows) : 0;
            System.out.printf(
                    "%-34s %12d %14d %6.1f %12d %8.1f%% %9d%n",
                    name, ingested, physical, (double) physical / ingested, deadRows, deadPct, elapsedMs
            );
            Assert.assertEquals(baseRows + ingested, reader.size());
        }
    }

    /**
     * Applies the compaction knobs for the next scenario. There is no single on/off switch on this
     * branch (unlike the ported-from branch's {@code cairo.partition.compaction.enabled}) - compaction
     * is threshold-driven per rule (waste ratio, piece count, age, table pressure - see
     * {@code PARTITION_COMPACTION.md}). "Disabled" here means every rule's threshold is pushed out of
     * reach rather than a real off switch.
     */
    private void applyCompactionSettings() {
        if (compactionEnabled) {
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MAX_PIECES, compactionMaxPieces);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_COOLDOWN, compactionCooldown);
        } else {
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MAX_PIECES, Integer.MAX_VALUE);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, Integer.MAX_VALUE);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_PERCENT, 100);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_STOP_PERCENT, 100);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, Long.MAX_VALUE);
        }
    }

    private void resetCompactionSettings() {
        compactionMaxPieces = 1000;
        compactionCooldown = "10m";
        compactionEnabled = false;
    }

    private void runCatchUpScenario(String name) throws Exception {
        // bound each storage commit to ~9k rows: with ~1.08M backlog rows this yields
        // ~120 commits, matching the interleaved bench's drain count
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 9_000);
        setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, "1.0");
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 10);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 10);
        applyCompactionSettings();

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
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        printSummary(name, ingested, physical, baseRows, elapsedMs, token);
    }

    private void runRandomScenario(String name) throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 10);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 10);
        applyCompactionSettings();

        String tbl = "bench" + scenarioIndex++;
        execute("CREATE TABLE " + tbl + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        long tsStep = Micros.DAY_MICROS / ROWS_PER_PARTITION;
        long baseRows = PARTITIONS * ROWS_PER_PARTITION - ROWS_PER_PARTITION / 2;
        execute("INSERT INTO " + tbl + " SELECT timestamp_sequence(" + BASE_START + ", " + tsStep + ")," +
                " rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7'), rnd_double(), rnd_long(0, 100000, 0)" +
                " FROM long_sequence(" + baseRows + ")");
        drainWalQueue();

        // each commit is a COMPACT ascending burst of rowsPerStep rows at a RANDOM position across the
        // whole data span - random which partition/offset, but each transaction covers a small ts range
        final long rangeMicros = baseRows * tsStep;
        final int rowsPerStep = (int) (1_080_000L / STEPS); // ~1.08M total, matching the other benches
        final long burstSpan = rowsPerStep * tsStep;
        long physicalBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        long startNanos = System.nanoTime();
        long ingested = 0;
        TableToken token = engine.verifyTableName(tbl);
        Rnd rnd = new Rnd();
        WalWriter w = engine.getWalWriter(token);
        try {
            for (int step = 1; step <= STEPS; step++) {
                long windowStart = BASE_START + rnd.nextLong(rangeMicros - burstSpan);
                for (int i = 0; i < rowsPerStep; i++) {
                    TableWriter.Row row = w.newRow(windowStart + i * tsStep);
                    row.putSym(1, SYMBOLS[rnd.nextInt(SYMBOLS.length)]);
                    row.putDouble(2, rnd.nextDouble());
                    row.putLong(3, rnd.nextLong(100_000));
                    row.append();
                }
                w.commit();
                ingested += rowsPerStep;
                drainWalQueue();
            }
        } finally {
            w.close();
        }
        drainWalQueue();

        long physical = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalBefore;
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        printSummary(name, ingested, physical, baseRows, elapsedMs, token);
    }

    private void runScenario(String name) throws Exception {
        // Split min size default is 4MB, not the production 50MB: the 50MB floor blocks all classic
        // tail-piece splits at this bench scale.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 0);
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 10);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 10);
        applyCompactionSettings();

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
        WriterSpec[] specs = buildWriterSpecs();
        WalWriter[] writers = new WalWriter[specs.length];
        Rnd rnd = new Rnd();
        try {
            for (int w = 0; w < specs.length; w++) {
                writers[w] = engine.getWalWriter(token);
                specs[w].lastTs = virtualNow - specs[w].offsetMicros - specs[w].cadenceSteps * STEP_MICROS;
            }
            for (int step = 1; step <= STEPS; step++) {
                virtualNow += STEP_MICROS;
                for (int w = 0; w < specs.length; w++) {
                    if (step % specs[w].cadenceSteps == 0) {
                        ingested += emitBatch(writers[w], specs[w], virtualNow, rnd);
                    }
                }
                drainWalQueue();
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
        printSummary(name, ingested, physical, baseRows, elapsedMs, token);
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
