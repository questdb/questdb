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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MatViewQueryTest extends AbstractCairoTest {
    private static final String INITIAL_ROWS = "k\tpeak\tts\n"
            + "A\t20\t2020-01-01T00:00:00.000000Z\n"
            + "B\t5\t2020-01-01T00:00:00.000000Z\n"
            + "A\t7\t2020-01-01T01:00:00.000000Z\n";

    @Test
    public void testInsertSelectReadsMaterializedRows() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            execute("CREATE TABLE lp_mat_copy(k SYMBOL,peak INT,ts TIMESTAMP) TIMESTAMP(ts)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO lp_mat_copy SELECT k,peak,ts FROM lp_mat WHERE peak>5 ORDER BY ts");
                try (RecordCursorFactory factory = compiler.compile("SELECT k,peak,ts FROM lp_mat_copy ORDER BY ts,k", sqlExecutionContext)
                        .getRecordCursorFactory()) {
                    assertResult(factory, "k\tpeak\tts\nA\t20\t2020-01-01T00:00:00.000000Z\nA\t7\t2020-01-01T01:00:00.000000Z\n");
                }
            }
        });
    }

    @Test
    public void testMaterializedRowsSupportFiltersOrderAndGrouping() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            assertMatView("SELECT k,peak,ts FROM lp_mat ORDER BY ts,k", INITIAL_ROWS);
            assertMatView("SELECT k,peak FROM lp_mat WHERE peak>5 ORDER BY ts DESC", "k\tpeak\nA\t7\nA\t20\n");
            assertMatView("SELECT k,peak FROM lp_mat WHERE ts>='2020-01-01T01:00:00Z'", "k\tpeak\nA\t7\n");
            assertMatView("SELECT k,sum(peak) total FROM lp_mat GROUP BY k ORDER BY k", "k\ttotal\nA\t27\nB\t5\n");
            assertMatView("SELECT k,peak FROM lp_mat ORDER BY ts DESC,peak DESC LIMIT 1", "k\tpeak\nA\t7\n");
        });
    }

    @Test
    public void testOrdinaryAndLiveViewReads() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            execute("CREATE VIEW lp_mat_plain AS (SELECT k,v,ts FROM lp_mat_base)");
            execute("CREATE LIVE VIEW lp_mat_live FLUSH EVERY 1s START FROM NOW AS "
                    + "(SELECT ts,k,v,count() OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) rn FROM lp_mat_base)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_mat_plain", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, "count\n4\n");
                }
                try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_mat_live", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, "count\n0\n");
                }
                try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_mat", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, "count\n3\n");
                }
            }
        });
    }

    @Test
    public void testRetainedFactorySeesRefreshAfterCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile("SELECT k,peak,ts FROM lp_mat ORDER BY ts,k", sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory other = compiler.compile("SELECT count() FROM lp_mat_base", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(other);
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertResult(factory, INITIAL_ROWS);
                execute("INSERT INTO lp_mat_base VALUES('A',30,'2020-01-01T00:03:00Z'),('C',9,'2020-01-01T02:01:00Z')");
                drainWalAndMatViewQueues();
                assertResult(factory, "k\tpeak\tts\n"
                        + "A\t30\t2020-01-01T00:00:00.000000Z\n"
                        + "B\t5\t2020-01-01T00:00:00.000000Z\n"
                        + "A\t7\t2020-01-01T01:00:00.000000Z\n"
                        + "C\t9\t2020-01-01T02:00:00.000000Z\n");
            }
        });
    }

    @Test
    public void testWriteValidationStillRejectsMaterializedViewTargets() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertMatViewRejected(compiler, "UPDATE lp_mat SET peak=peak+1", 0);
                assertMatViewRejected(compiler, "INSERT INTO lp_mat VALUES('A',1,'2020-01-01')", 12);
                assertMatViewRejected(compiler, "INSERT INTO lp_mat SELECT * FROM lp_mat", 12);
                try (RecordCursorFactory factory = compiler.compile("SELECT k,peak,ts FROM lp_mat ORDER BY ts,k", sqlExecutionContext)
                        .getRecordCursorFactory()) {
                    assertResult(factory, INITIAL_ROWS);
                }
            }
        });
    }

    private void assertMatView(String sql, String expected) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertNotNull(compiler.getPlanForTesting());
            assertResult(factory, expected);
        }
    }

    private void assertMatViewRejected(SqlCompilerImpl compiler, String sql, int position) {
        try {
            execute(compiler, sql);
            Assert.fail("materialized view target must be rejected");
        } catch (SqlException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertEquals("cannot modify materialized view [view=lp_mat]", e.getFlyweightMessage());
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createView() throws Exception {
        execute("CREATE TABLE lp_mat_base(k SYMBOL,v INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO lp_mat_base VALUES('A',10,'2020-01-01T00:01:00Z'),('A',20,'2020-01-01T00:02:00Z'),"
                + "('B',5,'2020-01-01T00:05:00Z'),('A',7,'2020-01-01T01:01:00Z')");
        execute("CREATE MATERIALIZED VIEW lp_mat AS (SELECT ts,k,max(v) peak FROM lp_mat_base SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        Assert.assertTrue(engine.verifyTableName("lp_mat").isMatView());
    }
}
