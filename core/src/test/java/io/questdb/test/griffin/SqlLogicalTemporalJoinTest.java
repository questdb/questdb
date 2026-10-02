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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.griffin.SqlLogicalSpliceJoinTest.TrackingFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.SqlLogicalSpliceJoinTest.metadata;
import static io.questdb.test.griffin.SqlLogicalSpliceJoinTest.registerSources;
import static io.questdb.test.griffin.SqlLogicalSpliceJoinTest.setInputs;
import static io.questdb.test.griffin.SqlLogicalSpliceJoinTest.unregisterSources;

public class SqlLogicalTemporalJoinTest extends AbstractCairoTest {
    private static final String HEADER = "k\tv\tts\tk1\tv1\tts1\n";

    @Test
    public void testConstructorFailuresConsumeInputsAndPreservePrimary() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException[] armedFailure = {null};
            try (
                    CairoEngine failingEngine = new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                        @Override
                        public int getSqlSmallMapKeyCapacity() {
                            if (armedFailure[0] != null) {
                                throw armedFailure[0];
                            }
                            return super.getSqlSmallMapKeyCapacity();
                        }
                    });
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(failingEngine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                failingEngine.load();
                registerSources(failingEngine.getFunctionFactoryCache());
                for (String join : new String[]{"ASOF", "LT"}) {
                    for (int fullFat = 0; fullFat < 2; fullFat++) {
                        final RuntimeException closeFailure = new RuntimeException("master close");
                        final TrackingFactory master = new TrackingFactory(metadata(ColumnType.INT));
                        final TrackingFactory slave = new TrackingFactory(metadata(ColumnType.INT));
                        master.closeFailure = closeFailure;
                        setInputs(master, slave);
                        try (SqlCompilerImpl compiler = new SqlCompilerImpl(failingEngine)) {
                            compiler.setFullFatJoins(fullFat == 1);
                            final RuntimeException failure = new RuntimeException("temporal map preparation");
                            armedFailure[0] = failure;
                            final RuntimeException actual = Assert.assertThrows(RuntimeException.class,
                                    () -> compiler.compile("SELECT * FROM lp_join_m() m " + join + " JOIN lp_join_s() s ON k", context));
                            armedFailure[0] = null;
                            Assert.assertSame(failure, actual);
                            Assert.assertEquals(1, actual.getSuppressed().length);
                            Assert.assertSame(closeFailure, actual.getSuppressed()[0]);
                            Assert.assertEquals(1, master.closeCount);
                            Assert.assertEquals(1, slave.closeCount);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFilteredSlaveAndFactoryOutlivesCompilation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertTemporal("SELECT * FROM lp_temporal_m m ASOF JOIN (SELECT * FROM lp_temporal_s WHERE v>11) s ON k",
                    false, "AsOf Join Fast", HEADER + """
                    1	1	2020-01-01T00:00:01.000000Z	null	null\t
                    2	2	2020-01-01T00:00:02.000000Z	2	22	2020-01-01T00:00:02.000000Z
                    1	3	2020-01-01T00:00:03.000000Z	1	33	2020-01-01T00:00:03.000000Z
                    3	4	2020-01-01T00:00:04.000000Z	null	null\t
                    """);
            assertTemporal("SELECT * FROM lp_temporal_m m ASOF JOIN (SELECT ts,v,k FROM lp_temporal_s WHERE v>11) s ON k",
                    false, "AsOf Join Fast", """
                    k	v	ts	ts1	v1	k1
                    1	1	2020-01-01T00:00:01.000000Z		null	null
                    2	2	2020-01-01T00:00:02.000000Z	2020-01-01T00:00:02.000000Z	22	2
                    1	3	2020-01-01T00:00:03.000000Z	2020-01-01T00:00:03.000000Z	33	1
                    3	4	2020-01-01T00:00:04.000000Z		null	null
                    """);
            assertTemporal("SELECT * FROM lp_temporal_m m ASOF JOIN (SELECT ts,v,k FROM lp_temporal_s WHERE v>11) s",
                    false, "AsOf Join Fast", """
                    k	v	ts	ts1	v1	k1
                    1	1	2020-01-01T00:00:01.000000Z		null	null
                    2	2	2020-01-01T00:00:02.000000Z	2020-01-01T00:00:02.000000Z	22	2
                    1	3	2020-01-01T00:00:03.000000Z	2020-01-01T00:00:03.000000Z	33	1
                    3	4	2020-01-01T00:00:04.000000Z	2020-01-01T00:00:03.000000Z	33	1
                    """);
        });
    }

    @Test
    public void testFullFatKeepsSymbolKeyType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_temporal_str(k STRING,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_temporal_vch(k VARCHAR,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_temporal_sym(k SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_temporal_str VALUES ('a','2020-01-01T00:00:02Z'),('b','2020-01-01T00:00:03Z')");
            execute("INSERT INTO lp_temporal_vch VALUES ('a','2020-01-01T00:00:02Z'),('b','2020-01-01T00:00:03Z')");
            execute("INSERT INTO lp_temporal_sym VALUES ('a','2020-01-01T00:00:01Z'),('c','2020-01-01T00:00:01Z')");
            for (String join : new String[]{"ASOF", "LT"}) {
                for (String masterTable : new String[]{"lp_temporal_str", "lp_temporal_vch"}) {
                    for (String slave : new String[]{"lp_temporal_sym", "(SELECT k, ts, rnd_int() r FROM lp_temporal_sym)"}) {
                        for (boolean isFullFat : new boolean[]{false, true}) {
                            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                                compiler.setFullFatJoins(isFullFat);
                                try (RecordCursorFactory factory = compiler.compile(
                                        "SELECT m.k, s.k sk, s.ts FROM " + masterTable + " m " + join + " JOIN " + slave + " s ON k", sqlExecutionContext
                                ).getRecordCursorFactory()) {
                                    Assert.assertEquals(ColumnType.SYMBOL, factory.getMetadata().getColumnType(1));
                                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("""
                                            k	sk	ts
                                            a	a	2020-01-01T00:00:01.000000Z
                                            b\t\t
                                            """);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFullFatSlaveTimestampKeyRestoresOutput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_temporal_tm(k TIMESTAMP,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_temporal_ts(k TIMESTAMP,ts TIMESTAMP) TIMESTAMP(k)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT * FROM lp_temporal_tm m ASOF JOIN lp_temporal_ts s ON k", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    final RecordMetadata metadata = factory.getMetadata();
                    Assert.assertEquals(4, metadata.getColumnCount());
                    Assert.assertEquals(1, metadata.getTimestampIndex());
                    Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, metadata.getColumnType(2));
                    Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, metadata.getColumnType(3));
                }
            }
        });
    }

    @Test
    public void testKeyedAndUnkeyedAlgorithmsRestoreFullFatOutputOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String keyed = HEADER + """
                    1	1	2020-01-01T00:00:01.000000Z	1	11	2020-01-01T00:00:01.000000Z
                    2	2	2020-01-01T00:00:02.000000Z	2	22	2020-01-01T00:00:02.000000Z
                    1	3	2020-01-01T00:00:03.000000Z	1	33	2020-01-01T00:00:03.000000Z
                    3	4	2020-01-01T00:00:04.000000Z	null	null\t
                    """;
            final String keyedLt = HEADER + """
                    1	1	2020-01-01T00:00:01.000000Z	null	null\t
                    2	2	2020-01-01T00:00:02.000000Z	null	null\t
                    1	3	2020-01-01T00:00:03.000000Z	1	11	2020-01-01T00:00:01.000000Z
                    3	4	2020-01-01T00:00:04.000000Z	null	null\t
                    """;
            assertTemporal("SELECT * FROM lp_temporal_m m ASOF JOIN lp_temporal_s s ON k", false, "AsOf Join Fast", keyed);
            assertTemporal("SELECT /*+ asof_linear(m s) */ * FROM lp_temporal_m m ASOF JOIN lp_temporal_s s ON k", false, "AsOf Join Light", keyed);
            assertTemporal("SELECT * FROM lp_temporal_m m LT JOIN lp_temporal_s s ON k", false, "Lt Join Light", keyedLt);
            assertTemporal("SELECT * FROM lp_temporal_m m ASOF JOIN lp_temporal_s s ON k", true, "AsOf Join", keyed);
            assertTemporal("SELECT * FROM lp_temporal_m m LT JOIN lp_temporal_s s ON k", true, "Lt Join", keyedLt);
            assertTemporal("SELECT * FROM lp_temporal_m m ASOF JOIN lp_temporal_s s", false, "AsOf Join Fast", HEADER + """
                    1	1	2020-01-01T00:00:01.000000Z	1	11	2020-01-01T00:00:01.000000Z
                    2	2	2020-01-01T00:00:02.000000Z	2	22	2020-01-01T00:00:02.000000Z
                    1	3	2020-01-01T00:00:03.000000Z	1	33	2020-01-01T00:00:03.000000Z
                    3	4	2020-01-01T00:00:04.000000Z	1	33	2020-01-01T00:00:03.000000Z
                    """);
            assertTemporal("SELECT /*+ asof_linear(m s) */ * FROM lp_temporal_m m LT JOIN lp_temporal_s s", false, "Lt Join", HEADER + """
                    1	1	2020-01-01T00:00:01.000000Z	null	null\t
                    2	2	2020-01-01T00:00:02.000000Z	1	11	2020-01-01T00:00:01.000000Z
                    1	3	2020-01-01T00:00:03.000000Z	2	22	2020-01-01T00:00:02.000000Z
                    3	4	2020-01-01T00:00:04.000000Z	1	33	2020-01-01T00:00:03.000000Z
                    """);
        });
    }

    @Test
    public void testSymbolHintsAndToleranceKeepExistingAlgorithms() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_temporal_sm(k SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE lp_temporal_ss(k SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_temporal_sm VALUES('A',1,'2020-01-01T00:00:02Z'),('A',2,'2020-01-01T00:00:04Z')");
            execute("INSERT INTO lp_temporal_ss VALUES('A',10,'2020-01-01T00:00:01Z'),('B',20,'2020-01-01T00:00:03Z')");
            final String expected = HEADER + """
                    A	1	2020-01-01T00:00:02.000000Z	A	10	2020-01-01T00:00:01.000000Z
                    A	2	2020-01-01T00:00:04.000000Z		null\t
                    """;
            final String join = " * FROM lp_temporal_sm m ASOF JOIN lp_temporal_ss s ON k TOLERANCE 1s";
            assertTemporal("SELECT /*+ asof_dense(m s) */" + join, false, "AsOf Join Dense Single Symbol", expected);
            assertTemporal("SELECT /*+ asof_index(m s) */" + join, false, "AsOf Join Indexed", expected);
            assertTemporal("SELECT /*+ asof_memoized(m s) */" + join, false, "AsOf Join Memoized", expected);
            assertTemporal("SELECT /*+ asof_memoized_driveby(m s) */" + join, false, "AsOf Join Memoized", expected);
            assertTemporal("SELECT" + join, true, "AsOf Join", expected);
        });
    }

    @Test
    public void testToleranceParserUsesHigherTimestampPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_tol_micro(v INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_tol_micro_s(w INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_tol_nano_s(w INT,ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("INSERT INTO lp_tol_micro VALUES(1,'2020-01-01T00:00:10.000001Z')");
            execute("INSERT INTO lp_tol_micro_s VALUES(1,'2020-01-01T00:00:10.000000Z')");
            execute("INSERT INTO lp_tol_nano_s VALUES(1,'2020-01-01T00:00:10.000000998Z'),(2,'2020-01-01T00:00:10.000000999Z')");
            // Micro-only: 2ns truncates to 0us, so the 1us-older row is out of tolerance.
            assertQuery("SELECT m.v,s.w FROM lp_tol_micro m ASOF JOIN lp_tol_micro_s s TOLERANCE 2n")
                    .noRandomAccess().expectSize().returns("v\tw\n1\tnull\n");
            assertQuery("SELECT m.v,s.w FROM lp_tol_micro m ASOF JOIN lp_tol_micro_s s TOLERANCE 2s")
                    .noRandomAccess().expectSize().returns("v\tw\n1\t1\n");
            // Nano slave: the tolerance applies at nanosecond precision.
            assertQuery("SELECT m.v,s.w FROM lp_tol_micro m ASOF JOIN lp_tol_nano_s s TOLERANCE 2n")
                    .noRandomAccess().expectSize().returns("v\tw\n1\t2\n");
            assertQuery("SELECT m.v,s.w FROM lp_tol_micro m ASOF JOIN (SELECT * FROM lp_tol_nano_s WHERE w=1) s TOLERANCE 1n")
                    .noRandomAccess().expectSize().returns("v\tw\n1\tnull\n");
            assertQuery("SELECT m.v,s.w FROM lp_tol_micro m ASOF JOIN (SELECT * FROM lp_tol_nano_s WHERE w=1) s TOLERANCE 2n")
                    .noRandomAccess().expectSize().returns("v\tw\n1\t1\n");
            assertException("SELECT m.v,s.w FROM lp_tol_micro m ASOF JOIN lp_tol_micro_s s TOLERANCE 2y", 72, "unsupported TOLERANCE unit");
        });
    }

    @Test
    public void testValidationFailureConsumesBothInputs() throws Exception {
        assertMemoryLeak(() -> {
            registerSources(engine.getFunctionFactoryCache());
            try {
                final TrackingFactory master = new TrackingFactory(metadata(ColumnType.INT));
                final TrackingFactory slave = new TrackingFactory(metadata(ColumnType.INT));
                master.metadata.setTimestampIndex(-1);
                setInputs(master, slave);
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    final SqlException error = Assert.assertThrows(SqlException.class,
                            () -> compiler.compile("SELECT * FROM lp_join_m() m ASOF JOIN lp_join_s() s ON k", sqlExecutionContext));
                    Assert.assertEquals("left side of time series join has no timestamp", error.getFlyweightMessage().toString());
                    Assert.assertEquals(28, error.getPosition());
                    Assert.assertEquals(1, master.closeCount);
                    Assert.assertEquals(1, slave.closeCount);
                }
            } finally {
                unregisterSources(engine.getFunctionFactoryCache());
            }
        });
    }

    private static void assertPlanContains(RecordCursorFactory factory, String algorithm) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), algorithm);
    }

    private void assertTemporal(String sql, boolean isFullFat, String algorithm, String expected) throws Exception {
        final RecordCursorFactory retained;
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        }
        try (RecordCursorFactory factory = retained) {
            Assert.assertEquals(2, factory.getMetadata().getTimestampIndex());
            Assert.assertEquals(6, factory.getMetadata().getColumnCount());
            assertPlanContains(factory, algorithm);
            Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, factory.getScanDirection());
            assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
        }
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE lp_temporal_m(k INT,v INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE lp_temporal_s(k INT,v INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO lp_temporal_m VALUES
                    (1,1,'2020-01-01T00:00:01Z'),(2,2,'2020-01-01T00:00:02Z'),
                    (1,3,'2020-01-01T00:00:03Z'),(3,4,'2020-01-01T00:00:04Z')
                """);
        execute("""
                INSERT INTO lp_temporal_s VALUES
                    (1,11,'2020-01-01T00:00:01Z'),(2,22,'2020-01-01T00:00:02Z'),(1,33,'2020-01-01T00:00:03Z')
                """);
    }
}
