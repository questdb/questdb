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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriteMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeltaTableCreateTest extends AbstractCairoTest {
    private static boolean isDeltaSupported = true;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public boolean isDeltaTableSupported() {
                return isDeltaSupported;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        isDeltaSupported = true;
        super.setUp();
    }

    @Test
    public void testCreateAndShowCreateRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE native_delta (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            execute("CREATE TABLE parquet_delta (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET DELTA");
            execute("CREATE TABLE parquet_after (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY DELTA FORMAT PARQUET");
            assertMode("native_delta", TableWriteMode.DELTA_RUNS, true, TableUtils.TABLE_FORMAT_NATIVE);
            assertMode("parquet_delta", TableWriteMode.DELTA_RUNS, true, TableUtils.TABLE_FORMAT_PARQUET);
            assertMode("parquet_after", TableWriteMode.DELTA_RUNS, true, TableUtils.TABLE_FORMAT_PARQUET);

            final String nativeDdl = showCreate("native_delta");
            TestUtils.assertContains(nativeDdl, "PARTITION BY DAY DELTA");
            execute(nativeDdl.replace("'native_delta'", "'native_copy'"));
            assertMode("native_copy", TableWriteMode.DELTA_RUNS, true, TableUtils.TABLE_FORMAT_NATIVE);
            final String parquetDdl = showCreate("parquet_delta");
            TestUtils.assertContains(parquetDdl, "FORMAT PARQUET DELTA");
            execute(parquetDdl.replace("'parquet_delta'", "'parquet_copy'"));
            assertMode("parquet_copy", TableWriteMode.DELTA_RUNS, true, TableUtils.TABLE_FORMAT_PARQUET);
            assertQuery("SELECT count() FROM native_delta").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testCreateAsSelectWithoutWriterCleansUp() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "CREATE TABLE failed_ctas AS (SELECT timestamp_sequence(0, 1) ts FROM long_sequence(1)) TIMESTAMP(ts) PARTITION BY DAY DELTA",
                    -1,
                    "Delta table writer is not available"
            );
            Assert.assertNull(engine.getTableTokenIfExists("failed_ctas"));
        });
    }

    @Test
    public void testDedupRejected() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA DEDUP UPSERT KEYS(ts)";
            assertExceptionNoLeakCheck(sql, sql.indexOf("DEDUP"), "deduplication is not supported on Delta tables");
            Assert.assertNull(engine.getTableTokenIfExists("d"));
        });
    }

    @Test
    public void testIfNotExistsDoesNotChangeModeOrRequireProvider() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            isDeltaSupported = false;
            execute("CREATE TABLE IF NOT EXISTS d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE IF NOT EXISTS w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            execute("CREATE TABLE IF NOT EXISTS d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            assertMode("d", TableWriteMode.DELTA_RUNS, true, TableUtils.TABLE_FORMAT_NATIVE);
            assertMode("w", TableWriteMode.WAL_SEGMENTS, true, TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testLikeInheritsModeAndFormat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE d (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY DELTA FORMAT PARQUET");
            execute("CREATE TABLE d_copy (LIKE d)");
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE w_copy (LIKE w)");
            execute("CREATE TABLE b (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE b_copy (LIKE b)");
            assertMode("d_copy", TableWriteMode.DELTA_RUNS, true, TableUtils.TABLE_FORMAT_PARQUET);
            assertMode("w_copy", TableWriteMode.WAL_SEGMENTS, true, TableUtils.TABLE_FORMAT_NATIVE);
            assertMode("b_copy", TableWriteMode.WAL_SEGMENTS, false, TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testMixedAndDuplicateModesRejectedAtSecondMode() throws Exception {
        assertMemoryLeak(() -> {
            assertMixedModes("DELTA WAL", "WAL");
            assertMixedModes("DELTA BYPASS WAL", "BYPASS");
            assertMixedModes("WAL DELTA", "DELTA");
            assertMixedModes("BYPASS WAL DELTA", "DELTA");
            assertMixedModes("DELTA DELTA", "DELTA");
            assertMixedModes("DELTA FORMAT PARQUET WAL", "WAL");
            assertMixedModes("WAL FORMAT PARQUET DELTA", "DELTA");
            Assert.assertNull(engine.getTableTokenIfExists("d"));
        });
    }

    @Test
    public void testNonPartitionedAndMissingTimestampRejected() throws Exception {
        assertMemoryLeak(() -> {
            final String nonPartitioned = "CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) DELTA";
            assertExceptionNoLeakCheck(nonPartitioned, nonPartitioned.indexOf("DELTA"), "DELTA requires a partitioned table with a designated timestamp");
            final String partitionNone = "CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY NONE DELTA";
            assertExceptionNoLeakCheck(partitionNone, partitionNone.indexOf("DELTA"), "DELTA requires a partitioned table with a designated timestamp");
            final String noTimestamp = "CREATE TABLE d (ts TIMESTAMP) PARTITION BY DAY DELTA";
            assertExceptionNoLeakCheck(noTimestamp, noTimestamp.indexOf("DAY"), "partitioning is possible only on tables with designated timestamps");
            Assert.assertNull(engine.getTableTokenIfExists("d"));
        });
    }

    @Test
    public void testParserReuseClearsModeIncludingAfterError() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertCompiledMode(compiler, "CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA", TableWriteMode.DELTA_RUNS, true);
                assertCompiledMode(compiler, "CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", TableWriteMode.WAL_SEGMENTS, true);
                assertCompiledMode(compiler, "CREATE TABLE d AS (SELECT timestamp_sequence(0, 1) ts FROM long_sequence(1)) TIMESTAMP(ts) PARTITION BY DAY DELTA", TableWriteMode.DELTA_RUNS, true);
                assertCompiledMode(compiler, "CREATE TABLE b (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", TableWriteMode.WAL_SEGMENTS, false);
                assertCompiledMode(compiler, "CREATE TABLE plain (v INT)", TableWriteMode.WAL_SEGMENTS, false);
            }
            assertExceptionNoLeakCheck("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA WAL", -1, "mutually exclusive");
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertMode("w", TableWriteMode.WAL_SEGMENTS, true, TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testUnavailableProviderRejectsCreateAndCtas() throws Exception {
        assertMemoryLeak(() -> {
            isDeltaSupported = false;
            assertExceptionNoLeakCheck("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA", -1, "Delta table writer is not available");
            assertExceptionNoLeakCheck(
                    "CREATE TABLE ctas AS (SELECT timestamp_sequence(0, 1) ts FROM long_sequence(1)) TIMESTAMP(ts) PARTITION BY DAY DELTA",
                    -1,
                    "Delta table writer is not available"
            );
            Assert.assertNull(engine.getTableTokenIfExists("d"));
            Assert.assertNull(engine.getTableTokenIfExists("ctas"));
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertMode("d", TableWriteMode.WAL_SEGMENTS, true, TableUtils.TABLE_FORMAT_NATIVE);
        });
    }

    @Test
    public void testValidationDoesNotCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            context.setValidationOnly(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA", context).closeAllButSelect();
            } finally {
                context.setValidationOnly(false);
            }
            Assert.assertNull(engine.getTableTokenIfExists("d"));
        });
    }

    private static void assertCompiledMode(SqlCompiler compiler, String sql, int expectedMode, boolean isWal) throws Exception {
        try (CreateTableOperation operation = (CreateTableOperation) compiler.compile(sql, sqlExecutionContext).getOperation()) {
            Assert.assertEquals(expectedMode, operation.getWriteMode());
            Assert.assertEquals(isWal, operation.isWalEnabled());
        }
    }

    private static void assertMixedModes(String modes, String offendingToken) throws Exception {
        final String sql = "CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + modes;
        assertExceptionNoLeakCheck(sql, sql.lastIndexOf(offendingToken), "WAL, BYPASS WAL and DELTA write modes are mutually exclusive");
    }

    private static void assertMode(String tableName, int expectedMode, boolean isWal, int expectedFormat) {
        try (TableReader reader = engine.getReader(tableName)) {
            Assert.assertEquals(expectedMode, reader.getMetadata().getWriteMode());
            Assert.assertEquals(isWal, reader.getMetadata().isWalEnabled());
            Assert.assertEquals(expectedFormat, reader.getMetadata().getTableFormat());
        }
    }

    private static String showCreate(String tableName) throws Exception {
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SHOW CREATE TABLE " + tableName, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            final String ddl = cursor.getRecord().getVarcharA(0).toString();
            Assert.assertFalse(cursor.hasNext());
            return ddl;
        }
    }
}
