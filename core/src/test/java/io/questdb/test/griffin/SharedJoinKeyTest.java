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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SharedJoinKeyTest extends AbstractCairoTest {
    @Test
    public void testLeftJoinSharedSlaveKeyKeepsUnmatchedMaster() throws Exception {
        assertMemoryLeak(() -> {
            createMasterMismatch();
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertRows("""
                        SELECT m.id mid,s.id sid,m.a ma,m.b mb,s.a sa,s.b sb
                        FROM lp_shared_m m LEFT JOIN lp_shared_s s
                        ON m.a=s.a AND m.b=s.a
                        """, """
                        mid	sid	ma	mb	sa	sb
                        11	null	1	2	null	null
                        22	30	1	1	1	9
                        """, isFullFat);
            }
        });
    }

    @Test
    public void testSpliceSharedMasterKeyKeepsEverySlaveEvent() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("INSERT INTO lp_shared_m VALUES(30,1,9,3)");
            execute("INSERT INTO lp_shared_s VALUES(11,1,1,1),(22,1,2,2)");
            assertRows("""
                    SELECT m.id mid,s.id sid,m.a ma,m.b mb,s.a sa,s.b sb
                    FROM lp_shared_m m SPLICE JOIN lp_shared_s s
                    ON m.a=s.a AND m.a=s.b
                    """, """
                    mid	sid	ma	mb	sa	sb
                    null	11	null	null	1	1
                    null	22	null	null	1	2
                    30	11	1	9	1	1
                    """, false);
        });
    }

    @Test
    public void testSpliceSharedSlaveKeyKeepsEveryMasterEvent() throws Exception {
        assertMemoryLeak(() -> {
            createMasterMismatch();
            assertRows("""
                    SELECT m.id mid,s.id sid,m.a ma,m.b mb,s.a sa,s.b sb
                    FROM lp_shared_m m SPLICE JOIN lp_shared_s s
                    ON m.a=s.a AND m.b=s.a
                    """, """
                    mid	sid	ma	mb	sa	sb
                    11	null	1	2	null	null
                    22	null	1	1	null	null
                    22	30	1	1	1	9
                    """, false);
        });
    }

    @Test
    public void testTemporalSharedSlaveKeyKeepsFutureUnmatchedMaster() throws Exception {
        assertMemoryLeak(() -> {
            createMasterMismatch();
            for (int kind = 0; kind < 2; kind++) {
                final String join = kind == 0 ? "ASOF" : "LT";
                assertRows("SELECT m.id mid,s.id sid,m.a ma,m.b mb,s.a sa,s.b sb FROM lp_shared_m m "
                        + join + " JOIN lp_shared_s s ON m.a=s.a AND m.b=s.a", """
                        mid	sid	ma	mb	sa	sb
                        11	null	1	2	null	null
                        22	null	1	1	null	null
                        """, false);
            }
        });
    }

    private void assertRows(String sql, String expected, boolean isFullFat) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns(expected);
            }
        }
    }

    private void createMasterMismatch() throws Exception {
        createTables();
        execute("INSERT INTO lp_shared_m VALUES(11,1,2,1),(22,1,1,2)");
        execute("INSERT INTO lp_shared_s VALUES(30,1,9,3)");
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE lp_shared_m(id INT,a INT,b INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_shared_s(id INT,a INT,b INT,ts TIMESTAMP) TIMESTAMP(ts)");
    }
}
