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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Query latency of the parquet-form covering index against the native one.
 * <p>
 * Decode counts, row-group skips and syscalls are all measured elsewhere;
 * latency is the one number that says whether the feature is worth having,
 * and it was the one nobody had.
 * <p>
 * <b>Not a gate.</b> A timing assertion in CI is a flake generator, and this
 * branch has already produced one performance claim that a repeat run
 * inverted. Nothing here asserts a ratio -- it prints a table and leaves the
 * judgement to a person.
 * <p>
 * The plan asked for {@code @Ignore} plus "run manually with -Dtest=...".
 * Those contradict: JUnit honours {@code @Ignore} even when the class is
 * named explicitly, so an {@code @Ignore}d benchmark cannot be run at all.
 * It is gated on a system property instead, which keeps it out of every
 * normal run while leaving it runnable:
 * <pre>
 * mvn -pl core -Pbuild-rust-library -Dtest='ParquetCoveringIndexBenchTest' \
 *     -DfailIfNoSpecifiedTests=false -Dquestdb.bench=true test
 * </pre>
 */
public class ParquetCoveringIndexBenchTest extends AbstractCairoTest {

    private static final String BENCH_PROPERTY = "questdb.bench";
    private static final String HOT_KEY = "s1";
    private static final String INDEXED_PARTITION = "2024-01-01";
    private static final int ITERATIONS = 20;
    private static final long ROW_COUNT = 400_000;
    /**
     * The hot key repeats every {@code SYM_CARDINALITY} rows; the cold key is
     * placed on a handful of rows only, so the sparse-key shape is dominated
     * by pruning rather than by gathering.
     */
    private static final int SYM_CARDINALITY = 16;
    private static final int WARMUP = 5;

    @Test
    public void testCoveringReadLatencyAgainstTheNativeIndex() throws Exception {
        Assume.assumeTrue(
                "benchmark, not a gate: enable with -D" + BENCH_PROPERTY + "=true",
                Boolean.getBoolean(BENCH_PROPERTY)
        );
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            createArm("native_bench");
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            createArm("parquet_bench");

            // A two-arm bake-off that silently runs the same arm twice agrees
            // with itself perfectly. Assert the forms differ before timing
            // anything, and report the fixture so the ratios below mean
            // something to someone who did not run them.
            assertArmsDifferAndDescribeFixture();

            final String[][] shapes = {
                    {"count, hot key (metadata only)",
                            "select count() from %s where sym = '" + HOT_KEY + "'"},
                    {"covered gather, hot key",
                            "select price, qty from %s where sym = '" + HOT_KEY
                                    + "' and ts in '" + INDEXED_PARTITION + "'"},
                    {"covered gather, cold key (pruning)",
                            "select price, qty from %s where sym = 'cold'"
                                    + " and ts in '" + INDEXED_PARTITION + "'"},
            };

            System.out.println();
            System.out.printf("%-38s %14s %14s %8s%n", "shape", "native (us)", "parquet (us)", "ratio");
            System.out.printf("%-38s %14s %14s %8s%n", "-".repeat(38), "-".repeat(14), "-".repeat(14), "-".repeat(8));
            for (String[] shape : shapes) {
                final long nativeMedian = medianMicros(String.format(shape[1], "native_bench"));
                final long parquetMedian = medianMicros(String.format(shape[1], "parquet_bench"));
                // Guard the division rather than print Infinity for a shape
                // that measured as zero on a fast machine.
                final String ratio = nativeMedian == 0
                        ? "n/a"
                        : String.format("%.2fx", (double) parquetMedian / (double) nativeMedian);
                System.out.printf("%-38s %14d %14d %8s%n", shape[0], nativeMedian, parquetMedian, ratio);
            }
            System.out.println();
            System.out.println("ratio < 1.00x means the parquet form is faster."
                    + " Run this three times before quoting a number:"
                    + " ratios that move more than 10% between runs are noise, not a result.");
        });
    }

    /**
     * Asserts the two arms really are sealed differently, then prints what the
     * fixture actually is. A ratio without a fixture description is not a
     * result anyone can act on.
     */
    private void assertArmsDifferAndDescribeFixture() {
        try (
                TableReader nativeReader = engine.getReader(engine.verifyTableName("native_bench"));
                TableReader parquetReader = engine.getReader(engine.verifyTableName("parquet_bench"))
        ) {
            final int nativeCol = nativeReader.getMetadata().getColumnIndex("sym");
            final int parquetCol = parquetReader.getMetadata().getColumnIndex("sym");
            // The form cache is filled when a partition is OPENED and readers
            // open lazily: read it first and an unopened partition reports
            // native, which would let a same-form bake-off pass this check.
            nativeReader.openPartition(0);
            parquetReader.openPartition(0);

            Assert.assertEquals(
                    "the native arm must be sealed natively, or both arms are the same arm",
                    PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE,
                    nativeReader.getPartitionIndexForm(0, nativeCol)
            );
            Assert.assertEquals(
                    "the parquet arm must be sealed to the parquet form, or both arms are the same arm",
                    PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                    parquetReader.getPartitionIndexForm(0, parquetCol)
            );

            System.out.println();
            System.out.println("fixture:");
            System.out.println("  rows in indexed partition : " + ROW_COUNT);
            System.out.println("  distinct keys             : " + SYM_CARDINALITY
                    + " (hot='" + HOT_KEY + "' every " + SYM_CARDINALITY + " rows, 'cold' on 3 rows)");
            System.out.println("  covered columns           : price DOUBLE, qty LONG");
            System.out.println("  _im size (parquet arm)    : "
                    + parquetReader.getPartitionIndexImFileSize(0, parquetCol) + " bytes");
            System.out.println("  index txn (parquet arm)   : "
                    + parquetReader.getPartitionIndexTxn(0, parquetCol));
        }
    }

    private void createArm(String table) throws Exception {
        execute("CREATE TABLE " + table + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + table + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x % " + SYM_CARDINALITY + ")," +
                " x::DOUBLE," +
                " x * 3" +
                " FROM long_sequence(" + ROW_COUNT + ")");
        // A genuinely sparse key: three rows in a partition of ROW_COUNT, so
        // the third shape measures pruning rather than gathering.
        execute("INSERT INTO " + table + " VALUES" +
                " ('" + INDEXED_PARTITION + "T12:00:00.000001Z', 'cold', 1.0, 1)," +
                " ('" + INDEXED_PARTITION + "T12:00:00.000002Z', 'cold', 2.0, 2)," +
                " ('" + INDEXED_PARTITION + "T12:00:00.000003Z', 'cold', 3.0, 3)");
        drainWalQueue();
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * Median rather than mean: one GC pause in twenty iterations moves a mean
     * far more than it moves the number a user would actually experience.
     */
    private long medianMicros(String sql) throws Exception {
        final long[] samples = new long[ITERATIONS];
        for (int i = 0; i < WARMUP + ITERATIONS; i++) {
            final long startNanos = System.nanoTime();
            drain(sql);
            final long elapsedNanos = System.nanoTime() - startNanos;
            if (i >= WARMUP) {
                samples[i - WARMUP] = elapsedNanos / 1000;
            }
        }
        java.util.Arrays.sort(samples);
        return samples[ITERATIONS / 2];
    }

    /**
     * Consumes every row and touches every projected column. Timing a cursor
     * that is never drained measures planning, not reading, and a projection
     * that is never read can be optimised away.
     */
    private void drain(String sql) throws Exception {
        long sideEffect = 0;
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            final int columnCount = factory.getMetadata().getColumnCount();
            while (cursor.hasNext()) {
                for (int col = 0; col < columnCount; col++) {
                    sideEffect += record.getLong(col);
                }
            }
        }
        if (sideEffect == Numbers.LONG_NULL) {
            // Unreachable in practice; exists so the accumulation above cannot
            // be treated as dead.
            System.out.print("");
        }
    }
}
