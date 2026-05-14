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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HivePartitionedReadParquetFunctionTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "false");
        super.setUp();
        inputRoot = root;
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

            assertQueryNoLeakCheck(
                    "id\tday\tsym\n" +
                            "1\t2026-05-01\tBTC-USD\n" +
                            "2\t2026-05-01\tBTC-USD\n" +
                            "1\t2026-05-02\tETH-USD\n" +
                            "2\t2026-05-02\tETH-USD\n",
                    "select id, day, sym from read_parquet('hive/day=*/sym=*/data.parquet') order by day, sym, id",
                    null,
                    true,
                    false
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
                    false
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
                            "1\trow_1\t1.5\t1970-01-01T00:00:01.000000Z\t2026-01-01\n" +
                            "2\trow_2\t3.0\t1970-01-01T00:00:02.000000Z\t2026-01-01\n" +
                            "3\trow_3\t4.5\t1970-01-01T00:00:03.000000Z\t2026-01-01\n" +
                            "1\trow_1\t1.5\t1970-01-01T00:00:01.000000Z\t2026-01-02\n" +
                            "2\trow_2\t3.0\t1970-01-01T00:00:02.000000Z\t2026-01-02\n" +
                            "3\trow_3\t4.5\t1970-01-01T00:00:03.000000Z\t2026-01-02\n",
                    "select id, label, ratio, ts, day from read_parquet('mt/day=*/data.parquet') order by day, id",
                    null,
                    true,
                    false
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
                            "5\t2026-01-01\n" +
                            "5\t2026-01-02\n",
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
                    "id\tday\n3\t2026-01-02\n",
                    "select id, day from read_parquet('comb/day=*/data.parquet') where day = '2026-01-02' and id = 3",
                    null,
                    false,
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
                            "3\t2026-01-02\n" +
                            "4\t2026-01-02\n" +
                            "5\t2026-01-02\n" +
                            "6\t2026-01-02\n" +
                            "7\t2026-01-02\n",
                    "select id, day from read_parquet('lo/day=*/data.parquet') order by day, id limit 12, 17",
                    null,
                    true,
                    false
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
            } catch (io.questdb.cairo.sql.TableReferenceOutOfDateException expected) {
                // expected - first file's schema does not match the second file's
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
                    false,
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
                    "id\tday\n1\t2026-01-02\n2\t2026-01-02\n3\t2026-01-02\n",
                    "select id, day from read_parquet('pdp/day=*/data.parquet') where day = '2026-01-02' order by id",
                    null,
                    true,
                    false
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
                    "id\tday\n1\t2026-01-01\n2\t2026-01-01\n",
                    "select id, day from read_parquet('emp/day=*/data.parquet') order by id",
                    null,
                    true,
                    false
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
