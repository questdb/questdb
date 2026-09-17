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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Set-operation result nullability follows the output row population:
 * UNION and UNION ALL emit rows from both branches, so a result column is
 * NOT NULL only when it is NOT NULL in both branches; EXCEPT, EXCEPT ALL,
 * INTERSECT and INTERSECT ALL emit rows from branch A only, so they keep
 * branch A's flag — including sentinel-as-data rendering of branch A rows.
 */
public class NotNullSetOperationTest extends AbstractCairoTest {

    // nn: NOT NULL branch; nu: nullable branch sharing value 20 with nn,
    // holding one genuine NULL. Basis of the operand-order matrix.
    private void createMatrixTables() throws SqlException {
        execute("CREATE TABLE nn (v INT NOT NULL)");
        execute("INSERT INTO nn VALUES (10), (20)");
        execute("CREATE TABLE nu (v INT)");
        execute("INSERT INTO nu VALUES (NULL), (20), (30)");
    }

    // nnS: NOT NULL branch carrying the INT sentinel bit pattern as data.
    private void createSentinelTable() throws SqlException {
        execute("CREATE TABLE nnS (v INT NOT NULL)");
        execute("INSERT INTO nnS VALUES (10), (-2147483648)");
    }

    @Test
    public void testExceptCastPath() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            execute("CREATE TABLE nl (v LONG)");
            execute("INSERT INTO nl VALUES (NULL), (40)");

            assertQuery("SELECT v FROM nn EXCEPT SELECT v FROM nl")
                    .noLeakCheck()
                    .returns("""
                            v
                            10
                            20
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nn EXCEPT SELECT v FROM nl) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testExceptNotNullFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nn EXCEPT SELECT v FROM nu")
                    .noLeakCheck()
                    .returns("""
                            v
                            10
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nn EXCEPT SELECT v FROM nu) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testExceptNullableFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nu EXCEPT SELECT v FROM nn")
                    .noLeakCheck()
                    .returns("""
                            v
                            null
                            30
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nu EXCEPT SELECT v FROM nn) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testExceptSentinelDataMatchesRealNullByBitPattern() throws Exception {
        // Syntactic-limit doctrine, pinned deliberately: EXCEPT matches rows by
        // raw bit pattern, so branch A's sentinel-as-data row is removed by a
        // genuine NULL on branch B. Do not "fix" this; it is inherent to
        // sentinel encoding.
        assertMemoryLeak(() -> {
            createMatrixTables();
            createSentinelTable();
            assertQuery("SELECT v FROM nnS EXCEPT SELECT v FROM nu")
                    .noLeakCheck()
                    .returns("""
                            v
                            10
                            """);
        });
    }

    @Test
    public void testExceptSentinelDataSurvives() throws Exception {
        assertMemoryLeak(() -> {
            createSentinelTable();
            execute("CREATE TABLE other (v INT)");
            execute("INSERT INTO other VALUES (5)");

            // EXCEPT emits branch A rows only; A's NOT NULL flag must be kept
            // so the sentinel bit pattern still renders as data.
            assertQuery("SELECT v FROM nnS EXCEPT SELECT v FROM other")
                    .noLeakCheck()
                    .returns("""
                            v
                            10
                            -2147483648
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nnS EXCEPT SELECT v FROM other) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testIntersectNotNullFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nn INTERSECT SELECT v FROM nu")
                    .noLeakCheck()
                    .returns("""
                            v
                            20
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nn INTERSECT SELECT v FROM nu) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testIntersectNullableFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nu INTERSECT SELECT v FROM nn")
                    .noLeakCheck()
                    .returns("""
                            v
                            20
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nu INTERSECT SELECT v FROM nn) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testIntersectSentinelDataMatchesRealNullByBitPattern() throws Exception {
        // Syntactic-limit doctrine, pinned deliberately: INTERSECT matches by
        // raw bit pattern, so branch A's sentinel-as-data row is matched by a
        // genuine NULL on branch B and emitted — as branch A data, rendered
        // with branch A's NOT NULL flag.
        assertMemoryLeak(() -> {
            createMatrixTables();
            createSentinelTable();
            assertQuery("SELECT v FROM nnS INTERSECT SELECT v FROM nu")
                    .noLeakCheck()
                    .returns("""
                            v
                            -2147483648
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nnS INTERSECT SELECT v FROM nu) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testUnionAllCastNotNullFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            execute("CREATE TABLE nl (v LONG)");
            execute("INSERT INTO nl VALUES (NULL), (40)");

            assertQuery("SELECT v FROM nn UNION ALL SELECT v FROM nl")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v
                            10
                            20
                            null
                            40
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nn UNION ALL SELECT v FROM nl) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testUnionAllCastNullableFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            execute("CREATE TABLE nl (v LONG)");
            execute("INSERT INTO nl VALUES (NULL), (40)");

            assertQuery("SELECT v FROM nl UNION ALL SELECT v FROM nn")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v
                            null
                            40
                            10
                            20
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nl UNION ALL SELECT v FROM nn) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testUnionAllConstantNullBranch() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nn UNION ALL SELECT CAST(NULL AS INT)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v
                            10
                            20
                            null
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nn UNION ALL SELECT CAST(NULL AS INT)) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testUnionAllMergePathNotNullFirst() throws Exception {
        // Designated timestamps on both branches plus timestamp order request
        // route through the merge UNION ALL factory (copyOfNew metadata path).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tnn (v INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tnn VALUES (10, '2024-01-01T00:00:00'), (20, '2024-01-01T02:00:00')");
            execute("CREATE TABLE tnu (v INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tnu VALUES (NULL, '2024-01-01T01:00:00'), (30, '2024-01-01T03:00:00')");

            final String unionSql = "SELECT ts, v FROM tnn UNION ALL SELECT ts, v FROM tnu";
            assertQuery("SELECT * FROM (" + unionSql + ") ORDER BY ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\t20
                            2024-01-01T03:00:00.000000Z\t30
                            """);
            assertQuery("SELECT count() FROM (SELECT * FROM (" + unionSql + ") ORDER BY ts) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testUnionAllNotNullFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nn UNION ALL SELECT v FROM nu")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v
                            10
                            20
                            null
                            20
                            30
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nn UNION ALL SELECT v FROM nu) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testUnionAllNullableFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nu UNION ALL SELECT v FROM nn")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v
                            null
                            20
                            30
                            10
                            20
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nu UNION ALL SELECT v FROM nn) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testUnionNotNullFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nn UNION SELECT v FROM nu ORDER BY v")
                    .noLeakCheck()
                    .returns("""
                            v
                            null
                            10
                            20
                            30
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nn UNION SELECT v FROM nu) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testUnionNullableFirst() throws Exception {
        assertMemoryLeak(() -> {
            createMatrixTables();
            assertQuery("SELECT v FROM nu UNION SELECT v FROM nn ORDER BY v")
                    .noLeakCheck()
                    .returns("""
                            v
                            null
                            10
                            20
                            30
                            """);
            assertQuery("SELECT count() FROM (SELECT v FROM nu UNION SELECT v FROM nn) WHERE v IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }
}
