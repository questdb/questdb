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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the Parquet
 * decode buffers ({@link io.questdb.griffin.engine.table.parquet.RowGroupBuffers})
 * wired through the Rust/Parquet opt-ins. The decoded column data is the
 * unbounded allocation: it scales with the row group being materialized, so a
 * runaway scan over wide values trips the limit at the offending decode malloc
 * on the Rust side.
 * <p>
 * Two query paths are covered:
 * <ul>
 *     <li>{@code read_parquet()} via {@code ReadParquetRecordCursor}, which owns
 *     its own {@code RowGroupBuffers} and binds the tracker in {@code of()};</li>
 *     <li>a scan over a {@code CONVERT PARTITION TO PARQUET} partition via the
 *     page-frame pool ({@code PageFrameMemoryPool}), which propagates the
 *     tracker to each decode buffer on reopen.</li>
 * </ul>
 * The breach surfaces as a {@link CairoException} carrying
 * {@code isOutOfMemory() == true} and the Rust-side per-query message
 * ({@code "query memory limit exceeded"}). Note that the Rust template differs
 * from the Java {@code Unsafe.malloc} template - it carries no
 * {@code workload=QUERY} field - so these tests assert only the shared
 * {@code "query memory limit exceeded"} text.
 * <p>
 * The encoder row group size and the read_parquet parallelism flag are set in
 * {@link #beforeClass()}; the per-query limit is applied per test in
 * {@link #setUp()}, and the provider reads it live on each tracker acquisition.
 * A large row group size keeps each converted partition in a single row group
 * so one decode crosses the limit deterministically; the read_parquet path is
 * pinned to the synchronous cursor.
 */
public class ParquetMemoryTrackerTest extends AbstractCairoTest {

    // 50k rows of ~256-byte varchar decode to ~12 MiB per row group, well over
    // the 512 KiB per-query limit.
    private static final int LARGE_ROWS = 50_000;

    @BeforeClass
    public static void beforeClass() {
        // One row group per converted partition so a single decode crosses the limit.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000_000);
        // Pin read_parquet() to the synchronous ReadParquetRecordCursor.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "false");
    }

    @Override
    public void setUp() {
        super.setUp();
        // 512 KiB: a single ~12 MiB row-group decode crosses it, while small inputs
        // stay under. Applied per test; the provider reads it live on each acquisition.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
        // read_parquet() resolves relative paths under the input root.
        inputRoot = root;
    }

    @Test
    public void testParquetPartitionScanFailsOnLargeData() throws Exception {
        // Full scan of a parquet partition decodes the row group through the
        // page-frame pool; the wide varchar column blows past the per-query limit.
        // The wide data lives in an older (sealed) partition: QuestDB never
        // converts the active partition, so a later, tiny partition seals the
        // first one before the conversion.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE p_big (s VARCHAR, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO p_big " +
                            "SELECT rnd_varchar(200, 256, 0), (x * 1_000_000L)::timestamp, x " +
                            "FROM long_sequence(" + LARGE_ROWS + ")"
            );
            execute("INSERT INTO p_big VALUES ('z', '1970-01-02T00:00:00.000000Z', -1)");
            execute("ALTER TABLE p_big CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            assertBreach("SELECT * FROM p_big WHERE ts < '1970-01-02'");
        });
    }

    @Test
    public void testParquetPartitionScanSucceedsOnSmallData() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE p_small (s VARCHAR, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO p_small VALUES" +
                            "  ('aaa', '1970-01-01T00:00:00.000000Z', 1)," +
                            "  ('bbb', '1970-01-01T00:00:01.000000Z', 2)," +
                            "  ('ccc', '1970-01-01T00:00:02.000000Z', 3)," +
                            "  ('zzz', '1970-01-02T00:00:00.000000Z', 4)"
            );
            execute("ALTER TABLE p_small CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            // Scan only the (now parquet) first partition through the page-frame pool.
            assertQuery("SELECT * FROM p_small WHERE ts < '1970-01-02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("s\tts\tv\n" +
                            "aaa\t1970-01-01T00:00:00.000000Z\t1\n" +
                            "bbb\t1970-01-01T00:00:01.000000Z\t2\n" +
                            "ccc\t1970-01-01T00:00:02.000000Z\t3\n");
        });
    }

    @Test
    public void testReadParquetFailsOnLargeFile() throws Exception {
        // read_parquet() decodes the full row group into ReadParquetRecordCursor's
        // own RowGroupBuffers; the wide varchar column crosses the per-query limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x_big AS (SELECT rnd_varchar(200, 256, 0) s, x v FROM long_sequence(" + LARGE_ROWS + "))");
            encodeToParquet("x_big", "x_big.parquet");
            assertBreach("SELECT * FROM read_parquet('x_big.parquet')");
        });
    }

    @Test
    public void testReadParquetSucceedsOnSmallFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x_small AS (SELECT ('s' || x)::varchar s, x v FROM long_sequence(3))");
            encodeToParquet("x_small", "x_small.parquet");
            assertQuery("SELECT * FROM read_parquet('x_small.parquet')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("s\tv\n" +
                            "s1\t1\n" +
                            "s2\t2\n" +
                            "s3\t3\n");
        });
    }

    @Test
    public void testRepeatedReadParquetRunsReleaseAllocations() throws Exception {
        // Repeat read_parquet() many times: each getCursor reopens the decode
        // buffers under the active tracker and close() frees them. assertMemoryLeak
        // around the loop is the load-bearing check - a malloc/free asymmetry shows
        // up as a residual native allocation count at the end of the test.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x_loop AS (SELECT ('val_' || x)::varchar s, x v FROM long_sequence(1000))");
            encodeToParquet("x_loop", "x_loop.parquet");
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT * FROM read_parquet('x_loop.parquet')", sqlExecutionContext
                 ).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(1000, rows);
                    }
                }
            }
        });
    }

    private static void assertBreach(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // drain until the decode breaches the per-query limit
                }
                Assert.fail("expected per-query memory breach");
            } catch (CairoException e) {
                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
            }
        }
    }

    private static void encodeToParquet(String tableName, String fileName) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(fileName);
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(io.questdb.std.Files.exists(path.$()));
        }
    }
}
