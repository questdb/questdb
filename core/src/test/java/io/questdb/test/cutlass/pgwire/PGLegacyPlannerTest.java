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

package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.CountingSqlCompilerFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.Statement;

public class PGLegacyPlannerTest extends BasePGTest {
    private static final CountingSqlCompilerFactory COMPILER_FACTORY = new CountingSqlCompilerFactory();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = COMPILER_FACTORY.engineFactory();
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testInsertCacheFollowsReloadedPlanner() throws Exception {
        // the modes whose Parse, Bind and Execute share one Sync, which caches the insert
        assertWithPgServer(CONN_AWARE_EXTENDED_LIMITED, (connection, binary, mode, port) -> {
            execute("CREATE TABLE t (x INT)");
            try (Statement stmt = connection.createStatement()) {
                // the second run takes the insert the new planner compiled from the connection's cache
                stmt.execute("INSERT INTO t VALUES (1)");
                stmt.execute("INSERT INTO t VALUES (1)");
                final int compilesBefore = COMPILER_FACTORY.getLegacyCompiles();
                setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
                // the connection follows the server, so the same insert now compiles with the legacy planner
                stmt.execute("INSERT INTO t VALUES (1)");
                Assert.assertEquals(compilesBefore + 1, COMPILER_FACTORY.getLegacyCompiles());
                // and then comes from the cache too
                stmt.execute("INSERT INTO t VALUES (1)");
                Assert.assertEquals(compilesBefore + 1, COMPILER_FACTORY.getLegacyCompiles());
                setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "false");
                stmt.execute("INSERT INTO t VALUES (1)");
                Assert.assertEquals(compilesBefore + 1, COMPILER_FACTORY.getLegacyCompiles());
            }
            assertQuery("SELECT count() FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            5
                            """);
        });
    }

    @Test
    public void testPreparedBatchUnderLegacyPlanner() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            execute("CREATE TABLE t (x INT)");
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            final int compilesBefore = COMPILER_FACTORY.getLegacyCompiles();
            // a batch binds the same statement again before the pipeline syncs, which copies the executed statement
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO t VALUES (?)")) {
                for (int i = 1; i <= 3; i++) {
                    stmt.setInt(1, i);
                    stmt.addBatch();
                }
                Assert.assertArrayEquals(new int[]{1, 1, 1}, stmt.executeBatch());
            }
            Assert.assertTrue(COMPILER_FACTORY.getLegacyCompiles() > compilesBefore);
            assertQuery("SELECT x FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }
}
