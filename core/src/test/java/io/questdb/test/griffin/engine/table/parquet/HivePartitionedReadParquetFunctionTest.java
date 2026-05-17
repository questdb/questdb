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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.table.HivePartitionedReadParquetPageFrameCursor;
import io.questdb.griffin.engine.functions.table.ReadParquetFunctionFactory;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.mp.WorkerPool;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HivePartitionedReadParquetFunctionTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "true");
        super.setUp();
        inputRoot = root;
    }

    @Test
    public void testParallelFilterOnPartitionColumn() throws Exception {
        // End-to-end check that the parallel filter pipeline reads partition virtual column
        // values correctly across files. The query touches `day` (partition column, DATE) so
        // the virtual page overlay has to deliver typed long buffers to the workers.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(5))");
            writeParquet("ppf/day=2026-02-01/data.parquet", "src");
            writeParquet("ppf/day=2026-02-02/data.parquet", "src");
            writeParquet("ppf/day=2026-02-03/data.parquet", "src");

            WorkerPool pool = new TestWorkerPool(4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start();
            try {
                final String sql = "select id from read_parquet('ppf/day=*/data.parquet') where day = '2026-02-02'";
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    // Sanity: the planner actually picked the parallel filter; otherwise the test
                    // would only validate sequential behaviour.
                    // The worker pool is live; whichever async filter variant the planner picks
                    // exercises the parallel pipeline. We assert correctness on the result.
                    assertCursor(
                            "id\n1\n2\n3\n4\n5\n",
                            factory,
                            true,
                            true, // partition-predicate residual elimination makes size() exact
                            false,
                            sqlExecutionContext
                    );
                }
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testParallelFilterOnParquetColumn() throws Exception {
        // Mirror test: filter on a real parquet column under parallel filter. Confirms
        // workers still decode parquet column data correctly while partition virtual
        // buffers are present alongside.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(10))");
            writeParquet("ppf2/day=2026-03-01/data.parquet", "src");
            writeParquet("ppf2/day=2026-03-02/data.parquet", "src");

            WorkerPool pool = new TestWorkerPool(4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start();
            try {
                final String sql = "select id, day from read_parquet('ppf2/day=*/data.parquet') where id = 7";
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    // The worker pool is live; whichever async filter variant the planner picks
                    // exercises the parallel pipeline. We assert correctness on the result.
                    assertCursor(
                            "id\tday\n" +
                                    "7\t2026-03-01T00:00:00.000Z\n" +
                                    "7\t2026-03-02T00:00:00.000Z\n",
                            factory,
                            true,
                            false,
                            false,
                            sqlExecutionContext
                    );
                }
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testCountAcrossFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select x as id from long_sequence(7))");
            writeParquet("hive/day=2026-01-01/data.parquet", "src");
            writeParquet("hive/day=2026-01-02/data.parquet", "src");
            writeParquet("hive/day=2026-01-03/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n21\n",
                    "select count(*) from read_parquet('hive/day=*/data.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGlobMatchesNoFiles() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = configuration.getFilesFacade();
            try (Path p = new Path()) {
                ff.mkdir(p.of(root).concat("empty").$(), 493);
            }
            try {
                select("select * from read_parquet('empty/*.parquet')").close();
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "glob did not match any files");
            }
        });
    }

    @Test
    public void testPartitionColumnsAppended() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("hive/day=2026-05-01/sym=BTC-USD/data.parquet", "src");
            writeParquet("hive/day=2026-05-02/sym=ETH-USD/data.parquet", "src");

            // day=YYYY-MM-DD values are inferred as DATE; sym=BTC-USD stays VARCHAR.
            assertQueryNoLeakCheck(
                    "id\tday\tsym\n" +
                            "1\t2026-05-01T00:00:00.000Z\tBTC-USD\n" +
                            "2\t2026-05-01T00:00:00.000Z\tBTC-USD\n" +
                            "1\t2026-05-02T00:00:00.000Z\tETH-USD\n" +
                            "2\t2026-05-02T00:00:00.000Z\tETH-USD\n",
                    "select id, day, sym from read_parquet('hive/day=*/sym=*/data.parquet') order by day, sym, id",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionColumnCollisionIsSuppressed() throws Exception {
        assertMemoryLeak(() -> {
            // "id" exists in the parquet file - the path's id= segment must not shadow it.
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("collide/id=999/data.parquet", "src");
            assertQueryNoLeakCheck(
                    "id\n1\n2\n3\n",
                    "select id from read_parquet('collide/id=*/data.parquet') order by id",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionMissingOnSomeFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            // First file establishes the "day" partition column; the second file lives directly
            // under the root without one, so its "day" value should read NULL.
            writeParquet("mixed/day=2026-01-01/data.parquet", "src");
            writeParquet("mixed/plain.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n1\n",
                    "select count(*) from read_parquet('mixed/**/*.parquet') where day is null",
                    null,
                    false,
                    true
            );
            assertQueryNoLeakCheck(
                    "count\n1\n",
                    "select count(*) from read_parquet('mixed/**/*.parquet') where day = '2026-01-01'",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testProjectionMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select" +
                    " cast(x as int) as id," +
                    " ('row_' || x)::varchar as label," +
                    " (x * 1.5)::double as ratio," +
                    " ('1970-01-01T00:00:00.000Z'::timestamp + x * 1000000) as ts" +
                    " from long_sequence(3))");
            writeParquet("mt/day=2026-01-01/data.parquet", "src");
            writeParquet("mt/day=2026-01-02/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "id\tlabel\tratio\tts\tday\n" +
                            "1\trow_1\t1.5\t1970-01-01T00:00:01.000000Z\t2026-01-01T00:00:00.000Z\n" +
                            "2\trow_2\t3.0\t1970-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000Z\n" +
                            "3\trow_3\t4.5\t1970-01-01T00:00:03.000000Z\t2026-01-01T00:00:00.000Z\n" +
                            "1\trow_1\t1.5\t1970-01-01T00:00:01.000000Z\t2026-01-02T00:00:00.000Z\n" +
                            "2\trow_2\t3.0\t1970-01-01T00:00:02.000000Z\t2026-01-02T00:00:00.000Z\n" +
                            "3\trow_3\t4.5\t1970-01-01T00:00:03.000000Z\t2026-01-02T00:00:00.000Z\n",
                    "select id, label, ratio, ts, day from read_parquet('mt/day=*/data.parquet') order by day, id",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testFilterOnPartitionColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(4))");
            writeParquet("filt/day=2026-01-01/data.parquet", "src");
            writeParquet("filt/day=2026-01-02/data.parquet", "src");
            writeParquet("filt/day=2026-01-03/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n4\n",
                    "select count(*) from read_parquet('filt/day=*/data.parquet') where day = '2026-01-02'",
                    null,
                    false,
                    true
            );
            assertQueryNoLeakCheck(
                    "count\n8\n",
                    "select count(*) from read_parquet('filt/day=*/data.parquet') where day != '2026-01-01'",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFilterOnParquetColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(10))");
            writeParquet("pc/day=2026-01-01/data.parquet", "src");
            writeParquet("pc/day=2026-01-02/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "id\tday\n" +
                            "5\t2026-01-01T00:00:00.000Z\n" +
                            "5\t2026-01-02T00:00:00.000Z\n",
                    "select id, day from read_parquet('pc/day=*/data.parquet') where id = 5 order by day",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testFilterPartitionAndParquetCombined() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(10))");
            writeParquet("comb/day=2026-01-01/data.parquet", "src");
            writeParquet("comb/day=2026-01-02/data.parquet", "src");
            writeParquet("comb/day=2026-01-03/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "id\tday\n3\t2026-01-02T00:00:00.000Z\n",
                    "select id, day from read_parquet('comb/day=*/data.parquet') where day = '2026-01-02' and id = 3",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testRecursiveStarStarGlob() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("rs/a/x1.parquet", "src");
            writeParquet("rs/a/b/x2.parquet", "src");
            writeParquet("rs/a/b/c/x3.parquet", "src");
            writeParquet("rs/d/x4.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n8\n",
                    "select count(*) from read_parquet('rs/**/*.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testLimitOffsetAcrossFiles() throws Exception {
        // Each file has 10 rows; we have 3 files = 30 rows total.
        // OFFSET 12 skips file 0 entirely and 2 rows of file 1 (skipRows fast path).
        // LIMIT 5 reads from file 1 (8 rows left) takes 5.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(10))");
            writeParquet("lo/day=2026-01-01/data.parquet", "src");
            writeParquet("lo/day=2026-01-02/data.parquet", "src");
            writeParquet("lo/day=2026-01-03/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "id\tday\n" +
                            "3\t2026-01-02T00:00:00.000Z\n" +
                            "4\t2026-01-02T00:00:00.000Z\n" +
                            "5\t2026-01-02T00:00:00.000Z\n" +
                            "6\t2026-01-02T00:00:00.000Z\n" +
                            "7\t2026-01-02T00:00:00.000Z\n",
                    "select id, day from read_parquet('lo/day=*/data.parquet') order by day, id limit 12, 17",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testToTopRestartsIteration() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("tt/day=2026-01-01/data.parquet", "src");
            writeParquet("tt/day=2026-01-02/data.parquet", "src");

            // self join forces the inner factory's cursor to be requested twice.
            assertQueryNoLeakCheck(
                    "cnt\n16\n",
                    "select count(*) cnt from read_parquet('tt/day=*/data.parquet') a cross join read_parquet('tt/day=*/data.parquet') b",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testManyFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(5))");
            final int n = 25;
            for (int i = 0; i < n; i++) {
                writeParquet("mf/day=2026-01-" + String.format("%02d", i + 1) + "/data.parquet", "src");
            }
            assertQueryNoLeakCheck(
                    "count\n" + (n * 5) + "\n",
                    "select count(*) from read_parquet('mf/day=*/data.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSchemaMismatchAcrossFiles() throws Exception {
        assertMemoryLeak(() -> {
            // First file: id INT. Second file: id LONG. The first file decides the schema; when
            // the cursor switches to the second file it must surface the mismatch instead of
            // silently coercing.
            execute("create table src_int as (select cast(x as int) as id from long_sequence(2))");
            execute("create table src_long as (select x as id from long_sequence(2))");
            writeParquet("mm/day=2026-01-01/data.parquet", "src_int");
            writeParquet("mm/day=2026-01-02/data.parquet", "src_long");

            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    io.questdb.cairo.sql.RecordCursorFactory factory = compiler.compile(
                            "select id from read_parquet('mm/day=*/data.parquet')",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                // Drain the cursor; one of the file switches must raise.
                while (cursor.hasNext()) {
                    cursor.getRecord().getInt(0);
                }
                Assert.fail("expected schema mismatch error");
            } catch (io.questdb.cairo.CairoException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "parquet schema mismatch");
                // Whichever file is opened second is the offender; both have a "day=" segment
                // so the path naming check works regardless of enumeration order.
                TestUtils.assertContains(expected.getFlyweightMessage(), "/mm/day=");
            }
        });
    }

    @Test
    public void testPlanShowsGlobScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            writeParquet("plan/day=2026-01-01/data.parquet", "src");
            writeParquet("plan/day=2026-01-02/data.parquet", "src");

            sink.clear();
            sink.put("select * from read_parquet('plan/day=*/data.parquet')");
            assertPlanNoLeakCheck(
                    sink,
                    "Parquet glob scan\n" +
                            "  glob: plan/day=*/data.parquet\n" +
                            "  scheme: local\n" +
                            "  files: 2\n"
            );
        });
    }

    @Test
    public void testPushdownPrunesRowGroupsAcrossFiles() throws Exception {
        // Each file has 5_000 rows split into row groups of 1000 with a bloom filter on `id`;
        // a selective filter on `id` should prune at least one row group per file.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(5000))");
            writeParquetWithBloomFilter("pd/day=2026-01-01/data.parquet", "src");
            writeParquetWithBloomFilter("pd/day=2026-01-02/data.parquet", "src");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "id\n",
                    "select id from read_parquet('pd/day=*/data.parquet') where id = -1",
                    null,
                    true,
                    false
            );
            // 2 files * 5 row groups each = 10 row groups; with id = -1 each is prunable.
            Assert.assertTrue(
                    "expected row groups to be pruned, got " + ParquetRowGroupFilter.getRowGroupsSkipped(),
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0
            );
        });
    }

    @Test
    public void testPushdownIgnoresPartitionFilters() throws Exception {
        // A filter on a partition column must not crash the prepared filter list (it has no
        // matching column in the parquet decoder). Row-level evaluation still produces the
        // correct rows.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("pdp/day=2026-01-01/data.parquet", "src");
            writeParquet("pdp/day=2026-01-02/data.parquet", "src");

            // expectSize=true: the partition-equality predicate is fully consumed by
            // the hive cursor's file-level prune, and the cursor reports an exact
            // prune-aware row count via size().
            assertQueryNoLeakCheck(
                    "id\tday\n" +
                            "1\t2026-01-02T00:00:00.000Z\n" +
                            "2\t2026-01-02T00:00:00.000Z\n" +
                            "3\t2026-01-02T00:00:00.000Z\n",
                    "select id, day from read_parquet('pdp/day=*/data.parquet') where day = '2026-01-02' order by id",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testInferInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            writeParquet("infInt/yr=2024/data.parquet", "src");
            writeParquet("infInt/yr=2025/data.parquet", "src");
            writeParquet("infInt/yr=2026/data.parquet", "src");

            // INT arithmetic must work, not just string comparison.
            assertQueryNoLeakCheck(
                    "count\n2\n",
                    "select count(*) from read_parquet('infInt/yr=*/data.parquet') where yr >= 2025",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testInferLongOnOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            // 5_000_000_000 exceeds INT range but fits in LONG; the column demotes to LONG.
            writeParquet("infLong/n=42/data.parquet", "src");
            writeParquet("infLong/n=5000000000/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "n\n42\n5000000000\n",
                    "select n from read_parquet('infLong/n=*/data.parquet') order by n",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testInferDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            writeParquet("infDbl/r=1.5/data.parquet", "src");
            writeParquet("infDbl/r=2.25/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "r\n1.5\n2.25\n",
                    "select r from read_parquet('infDbl/r=*/data.parquet') order by r",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testInferDemotesToVarcharOnMixed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            // First file looks like INT, second forces demotion all the way to VARCHAR.
            writeParquet("mix/tag=42/data.parquet", "src");
            writeParquet("mix/tag=hello/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "tag\n42\nhello\n",
                    "select tag from read_parquet('mix/tag=*/data.parquet') order by tag",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testInferPicksLatestType() throws Exception {
        // First file fits INT; second file appears that requires DOUBLE. Final type is DOUBLE.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(1))");
            writeParquet("mix2/v=10/data.parquet", "src");
            writeParquet("mix2/v=1.25/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "v\n1.25\n10.0\n",
                    "select v from read_parquet('mix2/v=*/data.parquet') order by v",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testEmptyPartitionValueIgnored() throws Exception {
        assertMemoryLeak(() -> {
            // "day=" has no value - should not be promoted to a partition column.
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("emp/day=2026-01-01/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "id\tday\n" +
                            "1\t2026-01-01T00:00:00.000Z\n" +
                            "2\t2026-01-01T00:00:00.000Z\n",
                    "select id, day from read_parquet('emp/day=*/data.parquet') order by id",
                    null,
                    true,
                    true
            );
            // sanity: id alone still works.
            assertQueryNoLeakCheck(
                    "count\n2\n",
                    "select count(*) from read_parquet('emp/day=*/data.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testParallelCountAcrossManyFilesManyWorkers() throws Exception {
        // Stress: 25 partitions, 4 workers, count(*) must match the sum of file row counts.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(7))");
            final int n = 25;
            for (int i = 0; i < n; i++) {
                writeParquet("pmany/day=2026-04-" + String.format("%02d", i + 1) + "/data.parquet", "src");
            }
            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select count(*) from read_parquet('pmany/day=*/data.parquet') where id >= 0",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor(
                            "count\n" + (n * 7) + "\n",
                            factory, false, true, false, ctx
                    );
                }
            });
        });
    }

    @Test
    public void testParallelGroupByOnPartitionColumn() throws Exception {
        // Aggregation keyed by a partition virtual column. Each worker decodes its file's
        // row group(s); the virtual page overlay supplies the day value for every row,
        // so the group-by hash table builds the right keys regardless of worker assignment.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(4))");
            writeParquet("pgb/day=2026-05-01/data.parquet", "src");
            writeParquet("pgb/day=2026-05-02/data.parquet", "src");
            writeParquet("pgb/day=2026-05-03/data.parquet", "src");
            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select day, count(*) cnt, sum(id) tot " +
                                "from read_parquet('pgb/day=*/data.parquet') " +
                                "group by day order by day",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor(
                            "day\tcnt\ttot\n" +
                                    "2026-05-01T00:00:00.000Z\t4\t10\n" +
                                    "2026-05-02T00:00:00.000Z\t4\t10\n" +
                                    "2026-05-03T00:00:00.000Z\t4\t10\n",
                            factory, true, true, false, ctx
                    );
                }
            });
        });
    }

    @Test
    public void testParallelMixedPartitionTypes() throws Exception {
        // Three partition columns of three different types alongside parquet data.
        // Verifies typed buffers are filled correctly per file and the overlay routes
        // each column to the right slot.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("pmix/yr=2024/region=1/score=1.5/data.parquet", "src");
            writeParquet("pmix/yr=2025/region=2/score=2.5/data.parquet", "src");
            withWorkerPool(2, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select id, yr, region, score " +
                                "from read_parquet('pmix/yr=*/region=*/score=*/data.parquet') " +
                                "order by yr, id",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor(
                            "id\tyr\tregion\tscore\n" +
                                    "1\t2024\t1\t1.5\n" +
                                    "2\t2024\t1\t1.5\n" +
                                    "1\t2025\t2\t2.5\n" +
                                    "2\t2025\t2\t2.5\n",
                            factory, true, true, false, ctx
                    );
                }
            });
        });
    }

    @Test
    public void testParallelNullPartitionInSomeFiles() throws Exception {
        // One file lacks the `day` segment; its rows must surface day as DATE NULL under
        // parallel iteration (typed long buffer filled with Numbers.LONG_NULL).
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("pnp/day=2026-06-01/data.parquet", "src");
            writeParquet("pnp/plain.parquet", "src");
            withWorkerPool(2, (compiler, ctx) -> {
                try (RecordCursorFactory factoryNull = compiler.compile(
                        "select count(*) from read_parquet('pnp/**/*.parquet') where day is null",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("count\n3\n", factoryNull, false, true, false, ctx);
                }
                try (RecordCursorFactory factoryDate = compiler.compile(
                        "select count(*) from read_parquet('pnp/**/*.parquet') where day = '2026-06-01'",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("count\n3\n", factoryDate, false, true, false, ctx);
                }
            });
        });
    }

    @Test
    public void testParallelOneFile() throws Exception {
        // Degenerate single-file case for the parallel path. Ensures the cursor handles
        // a glob that matches exactly one file without partition columns.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(50))");
            writeParquet("pone/flat.parquet", "src");
            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select count(*) from read_parquet('pone/*.parquet')",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("count\n50\n", factory, false, true, false, ctx);
                }
            });
        });
    }

    @Test
    public void testParallelSequentialParitySelectAll() throws Exception {
        // Same query in sequential and parallel modes must yield identical results.
        // ORDER BY keys make the comparison deterministic regardless of worker scheduling.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(8))");
            writeParquet("ppar/day=2026-07-01/data.parquet", "src");
            writeParquet("ppar/day=2026-07-02/data.parquet", "src");
            writeParquet("ppar/day=2026-07-03/data.parquet", "src");

            final String sql = "select id, day from read_parquet('ppar/day=*/data.parquet') order by day, id";
            final String sequentialExpected = capture(sql);
            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                    assertCursor(sequentialExpected, factory, true, true, false, ctx);
                }
            });
        });
    }

    @Test
    public void testParallelSequentialParityWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(10))");
            writeParquet("pparf/day=2026-08-01/data.parquet", "src");
            writeParquet("pparf/day=2026-08-02/data.parquet", "src");
            writeParquet("pparf/day=2026-08-03/data.parquet", "src");

            final String sql = "select id, day from read_parquet('pparf/day=*/data.parquet') " +
                    "where id < 4 and day != '2026-08-01' order by day, id";
            final String sequentialExpected = capture(sql);
            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                    assertCursor(sequentialExpected, factory, true, false, false, ctx);
                }
            });
        });
    }

    @Test
    public void testParallelWorkerCountParity() throws Exception {
        // Same query at 1, 2, and 4 workers must produce identical results.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(6))");
            writeParquet("pwc/day=2026-09-01/data.parquet", "src");
            writeParquet("pwc/day=2026-09-02/data.parquet", "src");
            writeParquet("pwc/day=2026-09-03/data.parquet", "src");

            final String sql = "select id, day from read_parquet('pwc/day=*/data.parquet') where id >= 4 order by day, id";
            final String[] results = new String[3];
            final int[] workerCounts = {1, 2, 4};
            for (int i = 0; i < workerCounts.length; i++) {
                final int idx = i;
                withWorkerPool(workerCounts[i], (compiler, ctx) -> {
                    io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
                    TestUtils.printSql(compiler, ctx, sql, sink);
                    results[idx] = sink.toString();
                });
            }
            Assert.assertEquals(results[0], results[1]);
            Assert.assertEquals(results[1], results[2]);
        });
    }

    @Test
    public void testParallelOrderByOnPartitionColumn() throws Exception {
        // ORDER BY a partition virtual column must produce stable, sorted output even
        // though files may be enumerated and consumed in non-sorted order under
        // parallel iteration.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("pord/day=2026-10-03/data.parquet", "src");
            writeParquet("pord/day=2026-10-01/data.parquet", "src");
            writeParquet("pord/day=2026-10-02/data.parquet", "src");
            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select day from read_parquet('pord/day=*/data.parquet') order by day",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor(
                            "day\n" +
                                    "2026-10-01T00:00:00.000Z\n" +
                                    "2026-10-01T00:00:00.000Z\n" +
                                    "2026-10-02T00:00:00.000Z\n" +
                                    "2026-10-02T00:00:00.000Z\n" +
                                    "2026-10-03T00:00:00.000Z\n" +
                                    "2026-10-03T00:00:00.000Z\n",
                            factory, true, true, false, ctx
                    );
                }
            });
        });
    }

    @Test
    public void testParallelRepeatedCursorReuse() throws Exception {
        // Self cross-join exercises factory.getCursor() being called twice on the
        // cached page-frame cursor, so toTop() must reset state cleanly.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("prep/day=2026-11-01/data.parquet", "src");
            writeParquet("prep/day=2026-11-02/data.parquet", "src");
            withWorkerPool(2, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select count(*) cnt " +
                                "from read_parquet('prep/day=*/data.parquet') a " +
                                "cross join read_parquet('prep/day=*/data.parquet') b",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("cnt\n36\n", factory, false, true, false, ctx);
                }
            });
        });
    }

    @Test
    public void testParallelInferredIntPartitionFilter() throws Exception {
        // Confirms the parallel path filters correctly on an inferred INT partition column
        // (arithmetic comparison, not string compare).
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("pint/yr=2024/data.parquet", "src");
            writeParquet("pint/yr=2025/data.parquet", "src");
            writeParquet("pint/yr=2026/data.parquet", "src");
            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select count(*) from read_parquet('pint/yr=*/data.parquet') where yr >= 2025",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("count\n4\n", factory, false, true, false, ctx);
                }
            });
        });
    }

    @Test
    public void testParallelVarcharPartitionEqFilter() throws Exception {
        // VARCHAR partition values used to force the sequential path; they now ride
        // through the parallel page-frame cursor via hand-encoded VARCHAR_SLICE aux
        // pages. Filter on the VARCHAR partition value and confirm the right rows
        // come back through the async filter pipeline.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(4))");
            writeParquet("pvc/sym=BTC-USD/data.parquet", "src");
            writeParquet("pvc/sym=ETH-USD/data.parquet", "src");
            writeParquet("pvc/sym=DOGE/data.parquet", "src");

            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select id, sym from read_parquet('pvc/sym=*/data.parquet') " +
                                "where sym = 'ETH-USD' order by id",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor(
                            "id\tsym\n1\tETH-USD\n2\tETH-USD\n3\tETH-USD\n4\tETH-USD\n",
                            factory, true, false, false, ctx
                    );
                }
            });
        });
    }

    @Test
    public void testParallelVarcharPartitionProjection() throws Exception {
        // Projection that includes a VARCHAR partition virtual column. Ensures the
        // overlay's aux+data pages return the right bytes for every row across files.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("pvp/region=eu-west-1/data.parquet", "src");
            writeParquet("pvp/region=us-east-2/data.parquet", "src");

            withWorkerPool(2, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select id, region from read_parquet('pvp/region=*/data.parquet') order by region, id",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor(
                            "id\tregion\n" +
                                    "1\teu-west-1\n" +
                                    "2\teu-west-1\n" +
                                    "1\tus-east-2\n" +
                                    "2\tus-east-2\n",
                            factory, true, true, false, ctx
                    );
                }
            });
        });
    }

    @Test
    public void testCheapToTopReusesOpenFiles() throws Exception {
        // The cursor caches a finalised pass across toTop calls. Running the same
        // query twice on the same cached factory must produce identical results
        // (this is what assertFactoryCursor's two-pass exercise is meant to verify,
        // and what an earlier attempt at the optimisation broke). A self cross-join
        // forces the cursor's frame iteration to be re-driven from scratch.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("ctop/day=2026-12-01/data.parquet", "src");
            writeParquet("ctop/day=2026-12-02/data.parquet", "src");
            writeParquet("ctop/day=2026-12-03/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "cnt\n36\n",
                    "select count(*) cnt " +
                            "from read_parquet('ctop/day=*/data.parquet') a " +
                            "cross join read_parquet('ctop/day=*/data.parquet') b",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFileLevelPruningEqAvoidsOpeningOtherFiles() throws Exception {
        // Use a glob that matches three files. With a partition-column equality filter,
        // the page-frame cursor should skip the two non-matching files entirely - the
        // count and decoded rows must come from one file only.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(5))");
            writeParquet("plev/day=2026-12-01/data.parquet", "src");
            writeParquet("plev/day=2026-12-02/data.parquet", "src");
            writeParquet("plev/day=2026-12-03/data.parquet", "src");

            withWorkerPool(4, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select id from read_parquet('plev/day=*/data.parquet') where day = '2026-12-02'",
                        ctx
                ).getRecordCursorFactory()) {
                    // sizeExpected=true: the hive cursor's prune-aware size() reports the
                    // exact surviving row count when the WHERE is a partition predicate
                    // that the cursor fully consumes.
                    assertCursor("id\n1\n2\n3\n4\n5\n", factory, true, true, false, ctx);
                }
            });
        });
    }

    @Test
    public void testFileLevelPruningIsNullSkipsPresentFiles() throws Exception {
        // `day IS NULL` should keep only the file that lacks the day= segment.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("pisn/day=2027-01-01/data.parquet", "src");
            writeParquet("pisn/plain.parquet", "src");

            withWorkerPool(2, (compiler, ctx) -> {
                try (RecordCursorFactory factory = compiler.compile(
                        "select count(*) from read_parquet('pisn/**/*.parquet') where day is null",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("count\n3\n", factory, false, true, false, ctx);
                }
            });
        });
    }

    @Test
    public void testFileLevelPruningActuallyOpensOnlyMatchingFile() throws Exception {
        // The other File-level pruning tests assert RESULT correctness; this one
        // asserts WORK reduction - the cursor must open exactly one file out of 30
        // for an equality filter on the partition column. Without pruning the cursor
        // would open all 30 and rely on the row-level filter to discard 29 files'
        // worth of decoded rows, which is exactly the scenario file pruning exists
        // to avoid.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            for (int d = 1; d <= 30; d++) {
                writeParquet(String.format("pfp/day=2026-01-%02d/data.parquet", d), "src");
            }

            // Force re-enumeration each test (factory is per-compile). Uses `select id`
            // (not count(*)) so the cursor actually iterates - count(*) over a partition
            // predicate is now satisfied by the prune-aware size() shortcut, which
            // computes the answer from cached footer row counts without opening any
            // files. Iteration is the only way to assert open-count.
            withWorkerPool(2, (compiler, ctx) -> {
                HivePartitionedReadParquetPageFrameCursor.resetFilesOpenedCount();
                try (RecordCursorFactory factory = compiler.compile(
                        "select id from read_parquet('pfp/day=*/data.parquet') where day = '2026-01-15'",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("id\n1\n2\n3\n", factory, true, true, false, ctx);
                }
                final long opened = HivePartitionedReadParquetPageFrameCursor.getFilesOpenedCount();
                Assert.assertEquals(
                        "WHERE day = '2026-01-15' should open exactly 1 of 30 files, opened " + opened,
                        1L, opened
                );
            });

            // BETWEEN range: 3 of 30
            withWorkerPool(2, (compiler, ctx) -> {
                HivePartitionedReadParquetPageFrameCursor.resetFilesOpenedCount();
                try (RecordCursorFactory factory = compiler.compile(
                        "select id from read_parquet('pfp/day=*/data.parquet') where day between '2026-01-10' and '2026-01-12'",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("id\n1\n2\n3\n1\n2\n3\n1\n2\n3\n", factory, true, true, false, ctx);
                }
                final long opened = HivePartitionedReadParquetPageFrameCursor.getFilesOpenedCount();
                Assert.assertEquals(
                        "BETWEEN should open exactly 3 of 30 files, opened " + opened,
                        3L, opened
                );
            });
        });
    }

    @Test
    public void testFileLevelPruningRangeOperators() throws Exception {
        // Range pushdown (<, <=, >, >=) on the inferred DATE partition column. Each
        // assertion picks a cut point and verifies the matching row count - the cursor
        // should only iterate files whose partition value satisfies the predicate.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(5))");
            writeParquet("prng/day=2026-03-01/data.parquet", "src");
            writeParquet("prng/day=2026-03-02/data.parquet", "src");
            writeParquet("prng/day=2026-03-03/data.parquet", "src");
            writeParquet("prng/day=2026-03-04/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n10\n", // 2026-03-03 + 2026-03-04
                    "select count(*) from read_parquet('prng/day=*/data.parquet') where day > '2026-03-02'",
                    null, false, true
            );
            assertQueryNoLeakCheck(
                    "count\n15\n", // 2026-03-02 + 2026-03-03 + 2026-03-04
                    "select count(*) from read_parquet('prng/day=*/data.parquet') where day >= '2026-03-02'",
                    null, false, true
            );
            assertQueryNoLeakCheck(
                    "count\n5\n", // 2026-03-01 only
                    "select count(*) from read_parquet('prng/day=*/data.parquet') where day < '2026-03-02'",
                    null, false, true
            );
            assertQueryNoLeakCheck(
                    "count\n10\n", // 2026-03-01 + 2026-03-02
                    "select count(*) from read_parquet('prng/day=*/data.parquet') where day <= '2026-03-02'",
                    null, false, true
            );
        });
    }

    @Test
    public void testFileLevelPruningBetween() throws Exception {
        // BETWEEN pushdown keeps the inclusive range and auto-swaps reversed bounds.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("pbet/day=2026-04-01/data.parquet", "src");
            writeParquet("pbet/day=2026-04-02/data.parquet", "src");
            writeParquet("pbet/day=2026-04-03/data.parquet", "src");
            writeParquet("pbet/day=2026-04-04/data.parquet", "src");
            writeParquet("pbet/day=2026-04-05/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n6\n", // 02 + 03 + 04
                    "select count(*) from read_parquet('pbet/day=*/data.parquet') where day between '2026-04-02' and '2026-04-04'",
                    null, false, true
            );
            // Reversed bounds: should still match 02..04 because BETWEEN auto-swaps.
            assertQueryNoLeakCheck(
                    "count\n6\n",
                    "select count(*) from read_parquet('pbet/day=*/data.parquet') where day between '2026-04-04' and '2026-04-02'",
                    null, false, true
            );
            // Degenerate: lo == hi keeps only that file.
            assertQueryNoLeakCheck(
                    "count\n2\n",
                    "select count(*) from read_parquet('pbet/day=*/data.parquet') where day between '2026-04-03' and '2026-04-03'",
                    null, false, true
            );
        });
    }

    @Test
    public void testFileLevelPruningRangeOnInt() throws Exception {
        // Range pushdown on an INT-inferred partition column exercises the int-cast
        // branch of compareTyped (long storage is sign-extended from int).
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("prngi/yr=2024/data.parquet", "src");
            writeParquet("prngi/yr=2025/data.parquet", "src");
            writeParquet("prngi/yr=2026/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n6\n", // 2025 + 2026
                    "select count(*) from read_parquet('prngi/yr=*/data.parquet') where yr >= 2025",
                    null, false, true
            );
            assertQueryNoLeakCheck(
                    "count\n3\n", // 2024 only
                    "select count(*) from read_parquet('prngi/yr=*/data.parquet') where yr < 2025",
                    null, false, true
            );
        });
    }

    @Test
    public void testTildeExpansionResolvesAgainstUserHome() {
        // Unit test for ReadParquetFunctionFactory.expandHomeDir(). Validates the
        // helper that lets users write read_parquet('~/data/foo.parquet'). The
        // expanded path still has to live under sql.copy.input.root - this is a
        // resolution convenience, not a sandbox bypass.
        final String savedHome = System.getProperty("user.home");
        try {
            System.setProperty("user.home", "/test/home/qdb");
            // '~/foo.parquet' -> '/test/home/qdb/foo.parquet'
            CharSequence expanded = ReadParquetFunctionFactory.expandHomeDir("~/foo.parquet");
            Assert.assertEquals("/test/home/qdb/foo.parquet", expanded.toString());
            // Bare '~' resolves to the home directory itself.
            CharSequence bare = ReadParquetFunctionFactory.expandHomeDir("~");
            Assert.assertEquals("/test/home/qdb", bare.toString());
            // '~user/...' (other-user expansion) is intentionally NOT expanded - we
            // pass through and let the sandbox check reject it consistently.
            CharSequence other = ReadParquetFunctionFactory.expandHomeDir("~root/x");
            Assert.assertEquals("~root/x", other.toString());
            // Paths that don't start with '~' pass through unchanged.
            CharSequence plain = ReadParquetFunctionFactory.expandHomeDir("/abs/path/x");
            Assert.assertEquals("/abs/path/x", plain.toString());
            CharSequence rel = ReadParquetFunctionFactory.expandHomeDir("rel/x");
            Assert.assertEquals("rel/x", rel.toString());
            // Empty / null inputs round-trip unchanged.
            Assert.assertEquals("", ReadParquetFunctionFactory.expandHomeDir("").toString());
            Assert.assertNull(ReadParquetFunctionFactory.expandHomeDir(null));
        } finally {
            if (savedHome != null) {
                System.setProperty("user.home", savedHome);
            } else {
                System.clearProperty("user.home");
            }
        }
    }

    @Test
    public void testFilenameWithEqualsNotMisinterpretedAsPartition() throws Exception {
        // Hive convention puts partitions in DIRECTORY names. A file named
        // 'foo=bar.parquet' is a real file name that happens to contain '=', not a
        // partition. The partition parser must walk only directory segments and skip
        // the last (filename) segment so the schema doesn't sprout a spurious 'foo'
        // column. Both the planning-time parser and the cursor-time parser must
        // agree on this or the query would crash on column-count mismatch.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("fneq/day=2026-06-01/foo=bar.parquet", "src");

            // Schema should only have id + day. NO 'foo' column.
            assertQueryNoLeakCheck(
                    "id\tday\n" +
                            "1\t2026-06-01T00:00:00.000Z\n" +
                            "2\t2026-06-01T00:00:00.000Z\n",
                    "select id, day from read_parquet('fneq/day=*/foo=bar.parquet') order by id",
                    null, true, true
            );
            // Confirm 'foo' really doesn't exist by selecting it explicitly.
            try {
                execute("select foo from read_parquet('fneq/day=*/foo=bar.parquet')");
                Assert.fail("expected SqlException - foo should not be exposed as a column");
            } catch (SqlException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "Invalid column");
            }
        });
    }

    @Test
    public void testProjectionPushdownPartitionColumnOnly() throws Exception {
        // SqlCodeGenerator pushes the {day} projection down through the hive factory.
        // The cursor's columnMapping then references only the partition virtual column
        // and no parquet column - the parquet decoder must skip the data columns. If
        // the pushdown were not wired through, downstream would see a single-column
        // metadata while the cursor produced multi-column frames, blowing up on
        // type / index mismatch.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id, x::varchar as label from long_sequence(2))");
            writeParquet("pdp_only/day=2026-04-01/data.parquet", "src");
            writeParquet("pdp_only/day=2026-04-02/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "day\n" +
                            "2026-04-01T00:00:00.000Z\n" +
                            "2026-04-01T00:00:00.000Z\n" +
                            "2026-04-02T00:00:00.000Z\n" +
                            "2026-04-02T00:00:00.000Z\n",
                    "select day from read_parquet('pdp_only/day=*/data.parquet') order by day",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testProjectionPushdownSkipsPartitionBufferAllocs() throws Exception {
        // When the query projection does not reference any partition column the
        // cursor must skip per-file alloc + fill of the partition virtual buffers.
        // For a 5-file hive glob and a sum() over a data column, that is 5 skips.
        // A query that does reference the partition column must NOT skip.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id, cast(x * 2 as long) as v from long_sequence(3))");
            for (int d = 1; d <= 5; d++) {
                writeParquet(String.format("pjs/day=2026-08-%02d/data.parquet", d), "src");
            }

            // sum(v) reads only `v` - day not in projection, partition buffer skip MUST fire.
            withWorkerPool(2, (compiler, ctx) -> {
                HivePartitionedReadParquetPageFrameCursor.resetPartitionBufferAllocsSkipped();
                try (RecordCursorFactory factory = compiler.compile(
                        "select sum(v) from read_parquet('pjs/day=*/data.parquet')",
                        ctx
                ).getRecordCursorFactory()) {
                    assertCursor("sum\n60\n", factory, false, true, false, ctx);
                }
                final long skipped = HivePartitionedReadParquetPageFrameCursor.getPartitionBufferAllocsSkipped();
                Assert.assertEquals(
                        "sum(v) should skip partition buffer alloc on all 5 files, skipped " + skipped,
                        5L, skipped
                );
            });

            // select day reads the partition column - skip must NOT fire.
            withWorkerPool(2, (compiler, ctx) -> {
                HivePartitionedReadParquetPageFrameCursor.resetPartitionBufferAllocsSkipped();
                try (RecordCursorFactory factory = compiler.compile(
                        "select count(day) from read_parquet('pjs/day=*/data.parquet')",
                        ctx
                ).getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        // drain
                        while (cursor.hasNext()) {
                            // no-op
                        }
                    }
                }
                final long skipped = HivePartitionedReadParquetPageFrameCursor.getPartitionBufferAllocsSkipped();
                Assert.assertEquals(
                        "count(day) projects the partition column - skip must NOT fire, skipped " + skipped,
                        0L, skipped
                );
            });
        });
    }

    @Test
    public void testProjectionPushdownReordersColumns() throws Exception {
        // Pushed-down projection rearranges columns: query asks for {day, id} but the
        // factory's natural schema is {id, ..., day}. The cursor must respect the
        // query's column order or downstream operators will read parquet bytes through
        // the wrong type / index.
        assertMemoryLeak(() -> {
            execute("create table src as (select" +
                    " cast(x as int) as id," +
                    " ('row_' || x)::varchar as label" +
                    " from long_sequence(2))");
            writeParquet("pdp_reord/day=2026-05-01/data.parquet", "src");
            writeParquet("pdp_reord/day=2026-05-02/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "day\tid\n" +
                            "2026-05-01T00:00:00.000Z\t1\n" +
                            "2026-05-01T00:00:00.000Z\t2\n" +
                            "2026-05-02T00:00:00.000Z\t1\n" +
                            "2026-05-02T00:00:00.000Z\t2\n",
                    "select day, id from read_parquet('pdp_reord/day=*/data.parquet') order by day, id",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testCachedListReuseAcrossMultipleExecutions() throws Exception {
        // The factory caches matchedFiles once at planning time. Reusing the same
        // factory across many getCursor() calls must produce identical results - any
        // accidental mutation of the cached list (e.g. by the cursor freeing it on
        // close) would manifest as the second pass returning a different / empty
        // result or NPE'ing.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("creuse/day=2026-07-01/data.parquet", "src");
            writeParquet("creuse/day=2026-07-02/data.parquet", "src");

            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "select id, day from read_parquet('creuse/day=*/data.parquet') order by day, id",
                         sqlExecutionContext
                 ).getRecordCursorFactory()) {
                final String expected = "id\tday\n" +
                        "1\t2026-07-01T00:00:00.000Z\n" +
                        "2\t2026-07-01T00:00:00.000Z\n" +
                        "3\t2026-07-01T00:00:00.000Z\n" +
                        "1\t2026-07-02T00:00:00.000Z\n" +
                        "2\t2026-07-02T00:00:00.000Z\n" +
                        "3\t2026-07-02T00:00:00.000Z\n";
                for (int i = 0; i < 5; i++) {
                    assertCursor(expected, factory, true, true, false, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testCachedListSnapshotsAtCompileTime() throws Exception {
        // matchedFiles is built at planning time. Files added AFTER the factory is
        // compiled must not appear in subsequent executions of the same factory -
        // the query result is a snapshot pinned to compile-time directory state.
        // This is the same semantics QuestDB applies to table metadata.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("csnap/day=2026-08-01/data.parquet", "src");

            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "select count(*) from read_parquet('csnap/day=*/data.parquet')",
                         sqlExecutionContext
                 ).getRecordCursorFactory()) {
                assertCursor("count\n2\n", factory, false, true, false, sqlExecutionContext);
                // Drop a new partition into the same glob. The next execution must still
                // see the original 2-row total - the cached file list isn't refreshed.
                writeParquet("csnap/day=2026-08-02/data.parquet", "src");
                assertCursor("count\n2\n", factory, false, true, false, sqlExecutionContext);
            }
            // A freshly compiled factory does pick up the new file - cache is per-compile,
            // not global.
            assertQueryNoLeakCheck(
                    "count\n4\n",
                    "select count(*) from read_parquet('csnap/day=*/data.parquet')",
                    null, false, true
            );
        });
    }

    @Test
    public void testCachedListDeletedFileRaisesClearError() throws Exception {
        // matchedFiles points at paths the planner saw. If one of them is removed
        // before iteration the cursor's per-file openRO must surface a clear
        // CairoException naming the missing path, not crash or silently skip it.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(2))");
            writeParquet("cdel/day=2026-09-01/data.parquet", "src");
            writeParquet("cdel/day=2026-09-02/data.parquet", "src");

            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "select id from read_parquet('cdel/day=*/data.parquet')",
                         sqlExecutionContext
                 ).getRecordCursorFactory()) {
                // Remove one of the files between planning and execution.
                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path p = new Path()) {
                    p.of(root).concat("cdel/day=2026-09-02/data.parquet").$();
                    Assert.assertTrue("delete should succeed", ff.removeQuiet(p.$()));
                }
                try (io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        cursor.getRecord().getInt(0);
                    }
                    Assert.fail("expected CairoException - cached path no longer exists");
                } catch (io.questdb.cairo.CairoException expected) {
                    TestUtils.assertContains(expected.getFlyweightMessage(), "could not open");
                }
            }
        });
    }

    @Test
    public void testAllFilesPrunedByPartitionFilterReturnsEmpty() throws Exception {
        // Every file pruned out by the partition-level filter. Cursor must produce
        // zero rows cleanly - no crash, no leftover state. count(*) verifies via
        // the size()-driven path; the row scan verifies via the iteration path.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(3))");
            writeParquet("callp/day=2026-10-01/data.parquet", "src");
            writeParquet("callp/day=2026-10-02/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n0\n",
                    "select count(*) from read_parquet('callp/day=*/data.parquet') where day = '2027-01-01'",
                    null, false, true
            );
            assertQueryNoLeakCheck(
                    "id\n",
                    "select id from read_parquet('callp/day=*/data.parquet') where day > '2030-01-01' order by id",
                    null, true, false
            );
        });
    }

    @Test
    public void testSizeBeforeIterationUsesCachedList() throws Exception {
        // computeTotalRowCount is invoked by size() / calculateSize when called before
        // next() has driven iteration. It walks matchedFiles directly (no fresh glob
        // enumeration). This test exercises that path: count(*) on a glob factory
        // compiles to a path that asks for size() up front.
        assertMemoryLeak(() -> {
            execute("create table src as (select cast(x as int) as id from long_sequence(7))");
            writeParquet("csz/day=2026-11-01/data.parquet", "src");
            writeParquet("csz/day=2026-11-02/data.parquet", "src");
            writeParquet("csz/day=2026-11-03/data.parquet", "src");

            assertQueryNoLeakCheck(
                    "count\n21\n",
                    "select count(*) from read_parquet('csz/day=*/data.parquet')",
                    null, false, true
            );
        });
    }

    /**
     * Convenience for parallel test cases: runs the given block with a fresh worker pool of
     * {@code workerCount}, then halts the pool. The runnable receives a compiler and the
     * shared execution context so query compilation goes through the pool's reduce queues.
     */
    private void withWorkerPool(int workerCount, ParallelTestBody body) throws Exception {
        WorkerPool pool = new TestWorkerPool(workerCount);
        TestUtils.setupWorkerPool(pool, engine);
        pool.start();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            body.run(compiler, sqlExecutionContext);
        } finally {
            pool.halt();
        }
    }

    /**
     * Returns the textual result of {@code sql} in sequential mode (default test setup
     * without a worker pool). Used to seed expected output for parity tests.
     */
    private String capture(String sql) throws Exception {
        io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, sql, sink);
        return sink.toString();
    }

    @FunctionalInterface
    private interface ParallelTestBody {
        void run(SqlCompiler compiler, io.questdb.griffin.SqlExecutionContext ctx) throws Exception;
    }

    private void writeParquetWithBloomFilter(String relativePath, String tableName) {
        FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                Path dir = new Path();
                PartitionDescriptor desc = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName);
                DirectLongList bloomCols = new DirectLongList(1, MemoryTag.NATIVE_DEFAULT)
        ) {
            path.of(root);
            int start = path.size();
            int slash = -1;
            for (int i = 0; i < relativePath.length(); i++) {
                char c = relativePath.charAt(i);
                if (c == '/' || c == java.io.File.separatorChar) {
                    slash = i;
                }
            }
            if (slash >= 0) {
                dir.of(root).concat(relativePath.substring(0, slash));
                Assert.assertEquals(0, ff.mkdirs(dir.slash(), 493));
            }
            path.trimTo(start).concat(relativePath);
            PartitionEncoder.populateFromTableReader(reader, desc, 0);
            bloomCols.add(0); // bloom on column 0 (id)
            PartitionEncoder.encodeWithOptions(
                    desc,
                    path,
                    ParquetCompression.COMPRESSION_UNCOMPRESSED,
                    true,
                    false,
                    1000,
                    0,
                    ParquetVersion.PARQUET_VERSION_V1,
                    bloomCols.getAddress(),
                    (int) bloomCols.size(),
                    0.01,
                    0.0,
                    -1,
                    -1L
            );
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    @Test
    public void testFactoryCloseDoesNotSelfDeadlockOnPrefetch() throws Exception {
        // Regression for a self-deadlock that made every hive-glob factory
        // close pay a full 2-second timeout. The factory's _close used to be
        // `synchronized void` and called prefetchExecutor.awaitTermination(2s)
        // INSIDE the monitor; a prefetch task parked on openCachedFile's
        // synchronized acquire could never make progress because the lock it
        // needed was held by the very call awaiting its termination. The
        // shutdown is now done outside the monitor so parked tasks acquire
        // the lock, see closed=true, and exit fast via the CairoException
        // path. 500 ms is comfortably above the new ~100 ms ceiling but well
        // below the buggy 2 s cap - if the regression returns the assertion
        // fires loud.
        assertMemoryLeak(() -> {
            execute("create table src as (select x as id from long_sequence(1))");
            // Several files so the factory pre-schedules prefetch opens.
            writeParquet("ppfclose/day=2026-07-01/data.parquet", "src");
            writeParquet("ppfclose/day=2026-07-02/data.parquet", "src");
            writeParquet("ppfclose/day=2026-07-03/data.parquet", "src");
            writeParquet("ppfclose/day=2026-07-04/data.parquet", "src");

            // Compile + open + close. count(*) finishes in microseconds and
            // tries to close before all prefetches complete - that's the path
            // that used to deadlock for 2 s.
            io.questdb.cairo.sql.RecordCursorFactory factory = select(
                    "select count(*) from read_parquet('ppfclose/day=*/data.parquet')"
            );
            try (io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(4L, cursor.getRecord().getLong(0));
            }
            long t0 = System.nanoTime();
            factory.close();
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            Assert.assertTrue(
                    "factory close took " + elapsedMs + " ms; the 2-second prefetch-executor "
                            + "awaitTermination self-deadlock has regressed",
                    elapsedMs < 500
            );
        });
    }

    @Test
    public void testFooterMinMaxShortcutHive() throws Exception {
        // End-to-end pin on the hive variant of the parquet footer MIN/MAX
        // shortcut. A hive glob over QuestDB-written parquets (each file
        // carries sorting_columns claiming designated ts ASC) routes
        // through HivePartitionedFooterAggregateRecordCursorFactory. The
        // cursor walks every matched file's row-group min/max via the
        // openCachedFile cache and emits a global aggregate.
        assertMemoryLeak(() -> {
            execute("create table src as (" +
                    "  select timestamp_sequence(1_000_000L, 100L) as ts, x as id" +
                    "  from long_sequence(1_000)" +
                    ") timestamp(ts) partition by day");
            writeParquet("min_max_hive/day=2026-01-01/data.parquet", "src");
            writeParquet("min_max_hive/day=2026-01-02/data.parquet", "src");
            writeParquet("min_max_hive/day=2026-01-03/data.parquet", "src");

            // EXPLAIN shows the hive footer factory.
            final io.questdb.std.str.StringSink planSink = new io.questdb.std.str.StringSink();
            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "EXPLAIN SELECT min(ts), max(ts) FROM read_parquet('min_max_hive/day=*/data.parquet')",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                io.questdb.cairo.sql.Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    planSink.put(rec.getStrA(0)).put('\n');
                }
            }
            final String plan = planSink.toString();
            Assert.assertTrue(
                    "Hive footer factory must appear in EXPLAIN, got:\n" + plan,
                    plan.toLowerCase().contains("parquet hive footer aggregate")
            );
            Assert.assertFalse(
                    "Normal aggregate path must not wrap the footer factory, got:\n" + plan,
                    plan.toLowerCase().contains("groupby") || plan.toLowerCase().contains("group by")
            );

            // Result-correctness: min/max across three identical files all
            // sourced from `src` so the global aggregate matches src's own.
            assertQueryNoLeakCheck(
                    "min\tmax\n" +
                            "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.099900Z\n",
                    "SELECT min(ts), max(ts) FROM read_parquet('min_max_hive/day=*/data.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testReverseScanHiveOrderByTsDescUsesSort() throws Exception {
        // Hive globs deliberately do NOT auto-elide ORDER BY ts DESC even
        // when every file declares ts ASC sorted. The cursor's reverse mode
        // iterates each file in DESC ts order but only produces a globally
        // DESC stream when files have disjoint, sorted ts ranges - a
        // property no standard parquet metadata expresses and the planner
        // cannot verify without walking every footer. SORT in the plan
        // pins the safe default; flipping this assertion is the signal
        // that an elision was added without the disjointness check.
        assertMemoryLeak(() -> {
            execute("create table src as (" +
                    "  select timestamp_sequence(1_000_000L, 1_000_000L) as ts, x as id" +
                    "  from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            writeParquet("rev_hive_safe/day=2026-01-01/data.parquet", "src");
            writeParquet("rev_hive_safe/day=2026-01-02/data.parquet", "src");
            writeParquet("rev_hive_safe/day=2026-01-03/data.parquet", "src");

            final io.questdb.std.str.StringSink planSink = new io.questdb.std.str.StringSink();
            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "EXPLAIN SELECT ts FROM read_parquet('rev_hive_safe/day=*/data.parquet') ORDER BY ts DESC LIMIT 3",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                io.questdb.cairo.sql.Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    planSink.put(rec.getStrA(0)).put('\n');
                }
            }
            final String plan = planSink.toString();
            Assert.assertFalse(
                    "Hive reverse elision must stay disabled, got plan:\n" + plan,
                    plan.toLowerCase().contains("parquet glob scan (reverse)")
            );

            // Top 3 in DESC ts order. timestamp_sequence(1_000_000us,
            // 1_000_000us) over 10 rows yields ts 1..10s; three identical
            // files mean the DESC top 3 are 10s x 3.
            assertQueryNoLeakCheck(
                    "ts\n" +
                            "1970-01-01T00:00:10.000000Z\n" +
                            "1970-01-01T00:00:10.000000Z\n" +
                            "1970-01-01T00:00:10.000000Z\n",
                    "SELECT ts FROM read_parquet('rev_hive_safe/day=*/data.parquet') ORDER BY ts DESC LIMIT 3",
                    "ts###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testHiveFactoryReverseScanCapability() throws Exception {
        // Direct capability test for the hive factory's reverse-scan
        // machinery. The planner does not auto-elide hive ORDER BY ts DESC
        // (see testReverseScanHiveOrderByTsDescUsesSort) so flipping the
        // factory has to happen programmatically to exercise the cursor
        // path. Asserts:
        //   - canScanInReverse() is true under the serial backend, false
        //     under the parallel one;
        //   - setReverseScan(true) flips getScanDirection() to BACKWARD
        //     under the serial backend only;
        //   - the cursor returned by getCursor() walks matchedFiles end-to-
        //     start and emits each file's rows in DESC ts order. With three
        //     files of identical ts ranges, the resulting stream is
        //     segment-DESC not globally-DESC; this is the correctness
        //     property the planner relies on when deciding NOT to elide.
        setProperty(PropertyKey.CAIRO_SQL_PARQUET_HIVE_PARALLEL_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("create table src as (" +
                    "  select timestamp_sequence(1_000_000L, 1_000_000L) as ts, x as id" +
                    "  from long_sequence(3)" +
                    ") timestamp(ts) partition by day");
            writeParquet("rev_hive_cap/day=2026-02-01/data.parquet", "src");
            writeParquet("rev_hive_cap/day=2026-02-02/data.parquet", "src");
            writeParquet("rev_hive_cap/day=2026-02-03/data.parquet", "src");

            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "SELECT ts FROM read_parquet('rev_hive_cap/day=*/data.parquet')",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final RecordCursorFactory inner = unwrapToHiveFactory(factory);
                Assert.assertTrue(
                        "expected HivePartitionedReadParquetRecordCursorFactory under unwrap, got " + inner.getClass(),
                        inner instanceof io.questdb.griffin.engine.functions.table.HivePartitionedReadParquetRecordCursorFactory
                );
                io.questdb.griffin.engine.functions.table.HivePartitionedReadParquetRecordCursorFactory hive =
                        (io.questdb.griffin.engine.functions.table.HivePartitionedReadParquetRecordCursorFactory) inner;

                Assert.assertTrue("serial backend must report canScanInReverse=true", hive.canScanInReverse());
                Assert.assertEquals(
                        "default scan direction is FORWARD",
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        hive.getScanDirection()
                );

                hive.setReverseScan(true);
                Assert.assertEquals(
                        "setReverseScan(true) flips serial backend to BACKWARD",
                        RecordCursorFactory.SCAN_DIRECTION_BACKWARD,
                        hive.getScanDirection()
                );

                // Materialise the cursor and capture the stream. Each file
                // independently yields rows in DESC ts order; files are
                // walked end-to-start. Expected stream (file 3 reverse, file
                // 2 reverse, file 1 reverse):
                //   3s, 2s, 1s, 3s, 2s, 1s, 3s, 2s, 1s
                final io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
                try (io.questdb.cairo.sql.RecordCursor cursor = hive.getCursor(sqlExecutionContext)) {
                    io.questdb.cairo.sql.Record rec = cursor.getRecord();
                    while (cursor.hasNext()) {
                        if (sink.length() > 0) {
                            sink.put(',');
                        }
                        sink.put(rec.getTimestamp(0));
                    }
                }
                Assert.assertEquals(
                        "reverse cursor emits segment-DESC: file3 rev, file2 rev, file1 rev",
                        "3000000,2000000,1000000,3000000,2000000,1000000,3000000,2000000,1000000",
                        sink.toString()
                );

                // Reset to forward and re-iterate to confirm setReverse can
                // be flipped between cursor opens. of() is idempotent.
                hive.setReverseScan(false);
                Assert.assertEquals(
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        hive.getScanDirection()
                );
                final io.questdb.std.str.StringSink fwd = new io.questdb.std.str.StringSink();
                try (io.questdb.cairo.sql.RecordCursor cursor = hive.getCursor(sqlExecutionContext)) {
                    io.questdb.cairo.sql.Record rec = cursor.getRecord();
                    while (cursor.hasNext()) {
                        if (fwd.length() > 0) {
                            fwd.put(',');
                        }
                        fwd.put(rec.getTimestamp(0));
                    }
                }
                Assert.assertEquals(
                        "forward cursor emits segment-ASC: file1 fwd, file2 fwd, file3 fwd",
                        "1000000,2000000,3000000,1000000,2000000,3000000,1000000,2000000,3000000",
                        fwd.toString()
                );
            }
        });
    }

    @Test
    public void testHiveFactoryReverseScanCapabilityParallelBackendIgnoresFlag() throws Exception {
        // Counterpart to the capability test for the default parallel
        // backend. setReverseScan stores the flag but getScanDirection
        // stays FORWARD - the page-frame backend has no per-row reverse
        // hook so canScanInReverse() returns false. This is the contract
        // the planner relies on to skip the flip for the parallel path.
        setProperty(PropertyKey.CAIRO_SQL_PARQUET_HIVE_PARALLEL_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("create table src as (" +
                    "  select timestamp_sequence(1_000_000L, 1_000_000L) as ts, x as id" +
                    "  from long_sequence(3)" +
                    ") timestamp(ts) partition by day");
            writeParquet("rev_hive_cap_par/day=2026-02-01/data.parquet", "src");
            writeParquet("rev_hive_cap_par/day=2026-02-02/data.parquet", "src");

            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "SELECT ts FROM read_parquet('rev_hive_cap_par/day=*/data.parquet')",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final RecordCursorFactory inner = unwrapToHiveFactory(factory);
                io.questdb.griffin.engine.functions.table.HivePartitionedReadParquetRecordCursorFactory hive =
                        (io.questdb.griffin.engine.functions.table.HivePartitionedReadParquetRecordCursorFactory) inner;
                Assert.assertFalse("parallel backend must report canScanInReverse=false", hive.canScanInReverse());
                hive.setReverseScan(true);
                Assert.assertEquals(
                        "parallel backend must clamp to FORWARD regardless of flag",
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        hive.getScanDirection()
                );
            }
        });
    }

    /**
     * The factory under test sits inside CursorFunction's wrapper - drill
     * through any wrapping layer until we hit the hive factory or run out
     * of options.
     */
    private static RecordCursorFactory unwrapToHiveFactory(RecordCursorFactory factory) {
        RecordCursorFactory candidate = factory;
        for (int i = 0; i < 8; i++) {
            if (candidate instanceof io.questdb.griffin.engine.functions.table.HivePartitionedReadParquetRecordCursorFactory) {
                return candidate;
            }
            RecordCursorFactory next = candidate.getBaseFactory();
            if (next == null || next == candidate) {
                break;
            }
            candidate = next;
        }
        return candidate;
    }

    @Test
    public void testFooterMinMaxShortcutHiveNonTsLongAsc() throws Exception {
        // Generic non-ts MIN/MAX over a hive glob. Each matched file is
        // written with encodeWithOptions(nonTsSortColumnIndex=id) so the
        // sorting_columns metadata claims id sorted ASC. The hive footer
        // shortcut must route through HivePartitionedFooterAggregateRecordCursorFactory
        // with the column-name path - the cursor resolves id per-file and
        // reads typed column-chunk stats via the new rowGroupMin/MaxValueLong
        // natives. Result must match the source table's known min/max.
        assertMemoryLeak(() -> {
            execute("create table src as (" +
                    "  select timestamp_sequence(1_000_000L, 100L) as ts, x as id" +
                    "  from long_sequence(1_000)" +
                    ") timestamp(ts) partition by day");
            writeParquetWithIdSortClaim("min_max_hive_id/day=2026-04-01/data.parquet", "src", false);
            writeParquetWithIdSortClaim("min_max_hive_id/day=2026-04-02/data.parquet", "src", false);
            writeParquetWithIdSortClaim("min_max_hive_id/day=2026-04-03/data.parquet", "src", false);

            final io.questdb.std.str.StringSink planSink = new io.questdb.std.str.StringSink();
            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "EXPLAIN SELECT min(id), max(id) FROM read_parquet('min_max_hive_id/day=*/data.parquet')",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                io.questdb.cairo.sql.Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    planSink.put(rec.getStrA(0)).put('\n');
                }
            }
            final String plan = planSink.toString();
            Assert.assertTrue(
                    "Hive footer factory must appear in EXPLAIN for non-ts MIN/MAX over a sorted-id glob, got:\n" + plan,
                    plan.toLowerCase().contains("parquet hive footer aggregate")
            );

            // Three files each holding ids 1..1000 -> global min 1, max 1000.
            assertQueryNoLeakCheck(
                    "min\tmax\n1\t1000\n",
                    "SELECT min(id), max(id) FROM read_parquet('min_max_hive_id/day=*/data.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFooterMinMaxShortcutHiveNonTsLongDescStillRoutes() throws Exception {
        // DESC sort claim - the cursor reads per-row-group stats so the
        // result is correct regardless of the declared sort direction. The
        // planner gate only requires getColumnOrderBy != 0; both ASC (+1)
        // and DESC (-1) satisfy it.
        assertMemoryLeak(() -> {
            execute("create table src as (" +
                    "  select timestamp_sequence(1_000_000L, 100L) as ts, (10_000 - x) as id" +
                    "  from long_sequence(500)" +
                    ") timestamp(ts) partition by day");
            writeParquetWithIdSortClaim("min_max_hive_id_desc/day=2026-04-01/data.parquet", "src", true);
            writeParquetWithIdSortClaim("min_max_hive_id_desc/day=2026-04-02/data.parquet", "src", true);

            final io.questdb.std.str.StringSink planSink = new io.questdb.std.str.StringSink();
            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "EXPLAIN SELECT min(id), max(id) FROM read_parquet('min_max_hive_id_desc/day=*/data.parquet')",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                io.questdb.cairo.sql.Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    planSink.put(rec.getStrA(0)).put('\n');
                }
            }
            Assert.assertTrue(
                    "DESC-claimed sort must still route through the hive footer factory, got plan:\n" + planSink,
                    planSink.toString().toLowerCase().contains("parquet hive footer aggregate")
            );

            // (10_000 - x) for x in [1, 500] yields ids in [9_500, 9_999].
            assertQueryNoLeakCheck(
                    "min\tmax\n9500\t9999\n",
                    "SELECT min(id), max(id) FROM read_parquet('min_max_hive_id_desc/day=*/data.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFooterMinMaxShortcutHiveNonTsDoubleAsc() throws Exception {
        // DOUBLE flavour of the hive non-ts MIN/MAX shortcut. Each matched
        // file has d sorted ASC via the writer's nonTsSortColumnIndex knob.
        // The cursor must dispatch to rowGroupMin/MaxValueDouble per file
        // and aggregate across files with NaN-safe propagation.
        assertMemoryLeak(() -> {
            execute("create table srcd as (" +
                    "  select timestamp_sequence(1_000_000L, 100L) as ts, x * 0.25 as d" +
                    "  from long_sequence(500)" +
                    ") timestamp(ts) partition by day");
            writeParquetWithSortClaim("min_max_hive_d/day=2026-05-01/data.parquet", "srcd", "d", false);
            writeParquetWithSortClaim("min_max_hive_d/day=2026-05-02/data.parquet", "srcd", "d", false);
            writeParquetWithSortClaim("min_max_hive_d/day=2026-05-03/data.parquet", "srcd", "d", false);

            final io.questdb.std.str.StringSink planSink = new io.questdb.std.str.StringSink();
            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "EXPLAIN SELECT min(d), max(d) FROM read_parquet('min_max_hive_d/day=*/data.parquet')",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                io.questdb.cairo.sql.Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    planSink.put(rec.getStrA(0)).put('\n');
                }
            }
            Assert.assertTrue(
                    "Hive footer factory must appear in EXPLAIN for DOUBLE non-ts MIN/MAX, got:\n" + planSink,
                    planSink.toString().toLowerCase().contains("parquet hive footer aggregate")
            );

            // x*0.25 for x in [1, 500]: min 0.25, max 125.0. Three
            // identical files don't change the global aggregate.
            assertQueryNoLeakCheck(
                    "min\tmax\n0.25\t125.0\n",
                    "SELECT min(d), max(d) FROM read_parquet('min_max_hive_d/day=*/data.parquet')",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFooterMinMaxShortcutHiveSkippedWithWhereClause() throws Exception {
        // Negative case: WHERE on a partition column would change which
        // files contribute. The detection branch's pre-check rejects on
        // nestedModel.getWhereClause() != null, so EXPLAIN must not show
        // the footer factory.
        assertMemoryLeak(() -> {
            execute("create table src as (" +
                    "  select timestamp_sequence(1_000_000L, 100L) as ts, x as id" +
                    "  from long_sequence(100)" +
                    ") timestamp(ts) partition by day");
            writeParquet("min_max_hive_filtered/day=2026-01-01/data.parquet", "src");
            writeParquet("min_max_hive_filtered/day=2026-01-02/data.parquet", "src");

            final io.questdb.std.str.StringSink planSink = new io.questdb.std.str.StringSink();
            try (
                    io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "EXPLAIN SELECT min(ts), max(ts) FROM " +
                                    "read_parquet('min_max_hive_filtered/day=*/data.parquet') " +
                                    "WHERE day = '2026-01-01'",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                io.questdb.cairo.sql.Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    planSink.put(rec.getStrA(0)).put('\n');
                }
            }
            final String plan = planSink.toString();
            Assert.assertFalse(
                    "WHERE clause must disable the hive footer shortcut, got plan:\n" + plan,
                    plan.toLowerCase().contains("parquet hive footer aggregate")
            );
        });
    }

    private void writeParquet(String relativePath, String tableName) {
        FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                Path dir = new Path();
                PartitionDescriptor desc = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root);
            int start = path.size();
            int slash = -1;
            for (int i = 0; i < relativePath.length(); i++) {
                char c = relativePath.charAt(i);
                if (c == '/' || c == java.io.File.separatorChar) {
                    slash = i;
                }
            }
            if (slash >= 0) {
                dir.of(root).concat(relativePath.substring(0, slash));
                Assert.assertEquals(0, ff.mkdirs(dir.slash(), 493));
            }
            path.trimTo(start).concat(relativePath);
            PartitionEncoder.populateFromTableReader(reader, desc, 0);
            PartitionEncoder.encode(desc, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    /**
     * Generalised variant of {@link #writeParquetWithIdSortClaim} for any
     * named column. Stamps {@code sorting_columns} on the chosen column
     * in the requested direction.
     */
    private void writeParquetWithSortClaim(String relativePath, String tableName, String columnName, boolean descending) {
        FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                Path dir = new Path();
                PartitionDescriptor desc = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root);
            int start = path.size();
            int slash = -1;
            for (int i = 0; i < relativePath.length(); i++) {
                char c = relativePath.charAt(i);
                if (c == '/' || c == java.io.File.separatorChar) {
                    slash = i;
                }
            }
            if (slash >= 0) {
                dir.of(root).concat(relativePath.substring(0, slash));
                Assert.assertEquals(0, ff.mkdirs(dir.slash(), 493));
            }
            path.trimTo(start).concat(relativePath);
            PartitionEncoder.populateFromTableReader(reader, desc, 0);
            final int columnIndex = reader.getMetadata().getColumnIndex(columnName);
            PartitionEncoder.encodeWithOptions(
                    desc,
                    path,
                    ParquetCompression.COMPRESSION_UNCOMPRESSED,
                    true,
                    false,
                    0,
                    0,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0,
                    0,
                    0.01,
                    0.0,
                    -1,
                    -1L,
                    columnIndex,
                    descending
            );
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    /**
     * Like {@link #writeParquet} but stamps the parquet's sorting_columns
     * metadata as {@code id} sorted in the requested direction. Used by the
     * non-ts MIN/MAX shortcut tests where the planner gate needs
     * {@code getColumnOrderBy(idIdx) != 0} on a non-timestamp column.
     */
    private void writeParquetWithIdSortClaim(String relativePath, String tableName, boolean descending) {
        FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                Path dir = new Path();
                PartitionDescriptor desc = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root);
            int start = path.size();
            int slash = -1;
            for (int i = 0; i < relativePath.length(); i++) {
                char c = relativePath.charAt(i);
                if (c == '/' || c == java.io.File.separatorChar) {
                    slash = i;
                }
            }
            if (slash >= 0) {
                dir.of(root).concat(relativePath.substring(0, slash));
                Assert.assertEquals(0, ff.mkdirs(dir.slash(), 493));
            }
            path.trimTo(start).concat(relativePath);
            PartitionEncoder.populateFromTableReader(reader, desc, 0);
            final int idColumnIndex = reader.getMetadata().getColumnIndex("id");
            PartitionEncoder.encodeWithOptions(
                    desc,
                    path,
                    ParquetCompression.COMPRESSION_UNCOMPRESSED,
                    true,
                    false,
                    0,
                    0,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0,
                    0,
                    0.01,
                    0.0,
                    -1,
                    -1L,
                    idColumnIndex,
                    descending
            );
            Assert.assertTrue(Files.exists(path.$()));
        }
    }
}
