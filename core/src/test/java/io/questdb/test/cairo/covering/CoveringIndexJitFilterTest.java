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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoveringIndexJitFilterTest extends AbstractCairoTest {

    @Before
    public void configureFrames() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
    }

    @Test
    public void testJavaFallbackRetainsCoveringRoute() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            final String sql = "SELECT score, qty, tag FROM tab WHERE sym = 'A' AND length(tag) > 3 ORDER BY score";
            assertQuery(sql)
                    .noLeakCheck()
                    .withPlanContaining("Async Filter", "CoveringIndex on: sym")
                    .returns("""
                            score\tqty\ttag
                            10\t100\tkeep-one
                            20\t400\tdrop
                            95\t300\tkeep-three
                            """);
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertFalse(factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testJitFilterFirstFixedAndVariableNativeAndParquet() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertJitResults();

            engine.releaseAllWriters();
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            assertJitResults();
        });
    }

    @Test
    public void testSparseStringAndBinaryProjectionKeepsAuxOffsetsValid() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE sparse (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (score, note, payload),
                        score INT,
                        note STRING,
                        payload BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO sparse
                    SELECT
                        '2024-01-01'::TIMESTAMP + x * 1_000_000L,
                        'A'::SYMBOL,
                        x * 10,
                        'note-' || x,
                        rnd_bin(4, 4, 0)
                    FROM long_sequence(6)
                    """);
            engine.releaseAllWriters();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            assertQuery("SELECT score, note, payload IS NOT NULL AS has_payload FROM sparse WHERE sym = 'A' AND score = 60")
                    .noLeakCheck()
                    .withPlanContaining("Async JIT Filter", "CoveringIndex on: sym")
                    .returns("""
                            score\tnote\thas_payload
                            60\tnote-6\ttrue
                            """);
        });
    }

    private void assertJitResults() throws Exception {
        final String sql = "SELECT score, qty, tag FROM tab WHERE sym = 'A' AND score >= 90 ORDER BY score";
        assertQuery(sql)
                .noLeakCheck()
                .withPlanContaining("Async JIT Filter", "CoveringIndex on: sym")
                .returns("""
                        score\tqty\ttag
                        90\t200\t
                        95\t300\tkeep-three
                        """);
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertTrue(factory.usesCompiledFilter());
        }

        assertQuery("SELECT count() FROM tab WHERE sym = 'A' AND score >= 90")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .withPlanContaining("Async JIT Filter", "CoveringIndex on: sym")
                .returns("count\n2\n");
    }

    private void createTable() throws Exception {
        execute("""
                CREATE TABLE tab (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING INCLUDE (score, qty, tag),
                    score INT,
                    qty LONG,
                    tag VARCHAR
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """);
        execute("""
                INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00.000000Z', 'A', 10, 100, 'keep-one'),
                    ('2024-01-01T01:00:00.000000Z', 'A', 90, 200, null),
                    ('2024-01-01T02:00:00.000000Z', 'A', 95, 300, 'keep-three'),
                    ('2024-01-01T03:00:00.000000Z', 'A', 20, 400, 'drop'),
                    ('2024-01-01T04:00:00.000000Z', 'A', 30, 500, 'x'),
                    ('2024-01-01T05:00:00.000000Z', 'A', 40, 600, 'y'),
                    ('2024-01-01T06:00:00.000000Z', 'B', 99, 700, 'other')
                """);
        engine.releaseAllWriters();
    }
}
