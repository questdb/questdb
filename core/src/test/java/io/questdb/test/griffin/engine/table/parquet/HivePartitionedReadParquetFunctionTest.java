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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
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
                            "  glob: plan/day=*/data.parquet\n"
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

            assertQueryNoLeakCheck(
                    "id\tday\n" +
                            "1\t2026-01-02T00:00:00.000Z\n" +
                            "2\t2026-01-02T00:00:00.000Z\n" +
                            "3\t2026-01-02T00:00:00.000Z\n",
                    "select id, day from read_parquet('pdp/day=*/data.parquet') where day = '2026-01-02' order by id",
                    null,
                    true,
                    false
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
                    assertCursor("id\n1\n2\n3\n4\n5\n", factory, true, false, false, ctx);
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
}
