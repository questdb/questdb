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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MatViewCursorFunctionDeterminismTest extends AbstractCairoTest {
    private final boolean isParallel;

    public MatViewCursorFunctionDeterminismTest(boolean isParallel) {
        this.isParallel = isParallel;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, String.valueOf(isParallel));
        super.setUp();
        inputRoot = root;
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testRejectsNonDeterministicScalarBoundDespiteMonotonicWrapper() throws Exception {
        // Reject-direction pin for the shared-holder residual path: ScalarSubQueryBoundRefFunction
        // deliberately does not forward the sub-query factory's fail-safe determinism hint to the
        // guard, so genuinely non-deterministic bounds must keep being rejected by the guard firing
        // on the offending function itself while the sub-query body is generated - before any
        // pruning holder exists. If that generation-time rejection ever regresses, this fails.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertNonDeterministicBoundRejected(
                    "CREATE MATERIALIZED VIEW mv AS SELECT ts, sum(v) AS s FROM base " +
                            "WHERE dateadd('h', 1, ts) >= (SELECT now()) SAMPLE BY 1h", "now");
            assertNonDeterministicBoundRejected(
                    "CREATE MATERIALIZED VIEW mv AS SELECT ts, sum(v) AS s FROM base " +
                            "WHERE dateadd('h', 1, ts) >= (SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) SAMPLE BY 1h", "rnd_timestamp");
        });
    }

    private static void assertNonDeterministicBoundRejected(String sql, String offendingFunction) {
        try {
            execute(sql);
            Assert.fail("expected non-deterministic rejection naming " + offendingFunction);
        } catch (Throwable e) {
            io.questdb.test.tools.TestUtils.assertContains(e.getMessage(),
                    "non-deterministic function cannot be used in materialized view: " + offendingFunction);
        }
    }

    @Test
    public void testRejectsBooleanSubQuery() throws Exception {
        assertMatViewRejected(
                "true",
                "(SELECT value FROM read_parquet('external_value.parquet'))"
        );
    }

    @Test
    public void testRejectsDualCursorBetween() throws Exception {
        assertMatViewRejected(
                "'2024-01-01T00:00:00.000000Z'::TIMESTAMP",
                "ts BETWEEN (SELECT value FROM read_parquet('external_value.parquet')) " +
                        "AND (SELECT value FROM read_parquet('external_value.parquet'))"
        );
    }

    @Test
    public void testRejectsHiCursorBetween() throws Exception {
        assertMatViewRejected(
                "'2024-01-01T00:00:00.000000Z'::TIMESTAMP",
                "ts BETWEEN '2023-01-01T00:00:00.000000Z' " +
                        "AND (SELECT value FROM read_parquet('external_value.parquet'))"
        );
    }

    @Test
    public void testRejectsLoCursorBetween() throws Exception {
        assertMatViewRejected(
                "'2024-01-01T00:00:00.000000Z'::TIMESTAMP",
                "ts BETWEEN (SELECT value FROM read_parquet('external_value.parquet')) " +
                        "AND '2025-01-01T00:00:00.000000Z'"
        );
    }

    private static void assertMatViewRejected(CharSequence sourceExpression, CharSequence predicate) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE external_value AS (SELECT " + sourceExpression + " AS value FROM long_sequence(1))");
            encodeTable("external_value", "external_value.parquet");
            execute("CREATE TABLE base (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            final String sql = "CREATE MATERIALIZED VIEW mv AS " +
                    "SELECT count() AS n, ts FROM base WHERE " + predicate + " SAMPLE BY 1h";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("SELECT", sql.indexOf("WHERE")),
                    "non-deterministic function cannot be used in materialized view: sub-query"
            );
        });
    }

    private static void encodeTable(CharSequence tableName, CharSequence fileName) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(fileName);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }
}
