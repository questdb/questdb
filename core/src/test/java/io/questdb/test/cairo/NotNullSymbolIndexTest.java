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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Interaction tests for SYMBOL NOT NULL with INDEX.
 * <p>
 * Background: SYMBOL columns store an int key per row, with -1 (SymbolTable.VALUE_IS_NULL)
 * as the null sentinel. When the column is also indexed, the BitmapIndex maintains
 * a posting list for each key. The combination matters because:
 * <ul>
 *   <li>The writer must reject a null insert at constraint-check time, before
 *       the symbol map writer or index writer ever sees the -1 key.</li>
 *   <li>An IS NULL query must not return any row, regardless of whether it
 *       executes through the index or through a row scan — the constraint
 *       guarantees zero rows.</li>
 *   <li>The metadata flag must survive a writer/reader cycle so the
 *       constant-fold optimisation in EqSymStrFunctionFactory keeps firing
 *       across reopens.</li>
 * </ul>
 */
public class NotNullSymbolIndexTest extends AbstractCairoTest {

    @Test
    public void testSymbolNotNullIndexedQueryByNullEmpty() throws Exception {
        // Insert valid (non-null) symbols and then probe the index via IS NULL.
        // The constraint guarantees no row stores VALUE_IS_NULL, so IS NULL must
        // return zero rows. This exercises the EqSymStrFunctionFactory constant
        // short-circuit which folds `sym = NULL` to BooleanConstant.FALSE when
        // the SYMBOL column is NOT NULL — without that fold the planner would
        // probe the index for the -1 key and might surface phantom matches if
        // the index ever stored one.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (s SYMBOL INDEX NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        ('alpha', '2024-01-01T00:00:00'),
                        ('beta',  '2024-01-01T00:00:01'),
                        ('alpha', '2024-01-01T00:00:02'),
                        ('gamma', '2024-01-01T00:00:03')
                    """);

            assertQuery("SELECT count(*) FROM t WHERE s IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM t WHERE s IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n4\n");

            // The column must remain indexed and NOT NULL after the inserts.
            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata md = reader.getMetadata();
                int idx = md.getColumnIndex("s");
                assertTrue(md.isColumnIndexed(idx));
                assertTrue(md.isNotNull(idx));
            }
        });
    }

    @Test
    public void testSymbolNotNullIndexedRejectsExplicitNull() throws Exception {
        // An explicit NULL literal into a NOT NULL column is a compile-time error,
        // reported at the NULL token before the symbol map writer or index writer
        // ever sees the -1 key.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (s SYMBOL INDEX NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            assertExceptionNoLeakCheck(
                    "INSERT INTO t VALUES (NULL, '2024-01-01')",
                    22,
                    "NOT NULL constraint violation [column=s]"
            );

            // No row landed.
            assertQuery("SELECT count(*) FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM t WHERE s IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testSymbolNotNullIndexedRejectsNullViaMissingColumn() throws Exception {
        // The "no value supplied" path — the writer detects the column is NOT NULL
        // and rejects before any null setter runs. Mirrors the existing
        // testSymbolNotNullMissingRejected coverage but with INDEX added so the
        // index initialisation path also runs first.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (s SYMBOL INDEX NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for SYMBOL NOT NULL INDEX (missing column)");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=s");
            }
        });
    }

    @Test
    public void testSymbolNotNullPlanFoldsIsNullToFalse() throws Exception {
        // EqSymStrFunctionFactory.createHalfConstantFunc has a fast-path:
        // when the symbol column is NOT NULL and the constant is NULL, return
        // BooleanConstant.FALSE. The query plan should reflect the fold — the
        // resulting plan must not reference an index probe for the NULL key,
        // because the IS NULL predicate has been collapsed to a constant.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (s SYMBOL INDEX NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        ('alpha', '2024-01-01T00:00:00'),
                        ('beta',  '2024-01-01T00:00:01'),
                        ('gamma', '2024-01-01T00:00:02')
                    """);

            // The fold to FALSE means the WHERE clause is a constant filter that
            // rejects every row. Counts must be zero regardless of the planner
            // path actually chosen.
            assertQuery("SELECT count(*) FROM t WHERE s IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM t WHERE NULL = s")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM t WHERE s = NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");

            // Compose with another predicate to make sure the constant fold
            // composes safely (`(s IS NULL) OR (s = 'beta')` must reduce to
            // `s = 'beta'` and surface only the matching row).
            assertQuery("SELECT ts, s FROM t WHERE s IS NULL OR s = 'beta' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:01.000000Z\tbeta
                            """);
        });
    }

    @Test
    public void testSymbolNotNullSurvivesSymbolMapReload() throws Exception {
        // Insert symbols, release readers, reopen, verify the index still works
        // and the NOT NULL flag is preserved across the reload. The symbol map
        // and index live in separate files from the column metadata so a
        // serialisation bug could surface only after reopen.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (s SYMBOL INDEX NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        ('alpha', '2024-01-01T00:00:00'),
                        ('beta',  '2024-01-01T00:00:01'),
                        ('alpha', '2024-01-01T00:00:02')
                    """);

            // Drop all cached writers/readers to force a cold open below.
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata md = reader.getMetadata();
                int idx = md.getColumnIndex("s");
                assertTrue("INDEX must survive reload", md.isColumnIndexed(idx));
                assertTrue("NOT NULL must survive reload", md.isNotNull(idx));
            }

            // Index lookup still works after the reload — and IS NULL still
            // folds to false (the EqSymStrFunctionFactory fast-path consults
            // the freshly-loaded metadata flag).
            assertQuery("SELECT count(*) FROM t WHERE s = 'alpha'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            2
                            """);
            assertQuery("SELECT count(*) FROM t WHERE s IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");

            // A subsequent insert that omits the symbol respects the constraint
            // after the reload — exercises the missing-column rejection path
            // which is the same code site that catches missing values pre-reload.
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-02')");
                fail("Expected NOT NULL violation after reload");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
            }
        });
    }
}
