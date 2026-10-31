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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every runtime bound interval extraction hands to the interval model must close exactly once, whichever
 * step of its lifecycle fails: instantiation, {@code init()}, the value read or {@code close()} itself.
 */
public class IntervalExtractionOwnershipTest extends AbstractCairoTest {
    private static final String[] PREDICATES = {
            "ts = owned_long($1)",
            "ts != owned_long($1)",
            "ts > owned_long($1) AND ts <= owned_long($2)",
            "ts BETWEEN owned_long($1) AND owned_long($2)",
            "ts NOT BETWEEN owned_long($1) AND '2020-01-02'",
            "ts IN owned_long($1)::STRING",
            "ts NOT IN owned_long($1)::STRING",
            "ts = owned_long($1) OR ts = owned_long($2)",
            "ts IN (owned_long($1), owned_long($2)) OR ts = '2020-01-03'",
            "ts IN '2020-01-01T00:00:00.0000001Z' OR ts = owned_long($1)",
            "dateadd('d', 1, ts) > owned_long($1)"
    };

    @Test
    public void testBoundCloseFailureClosesBoundsOnce() throws Exception {
        assertPredicates((fixture, compiler, sql) -> {
            final RuntimeException closeFailure = new RuntimeException("owned_long close");
            fixture.failLongClose(closeFailure);
            Assert.assertSame(closeFailure, runAndClose(fixture, compiler, sql));
            fixture.assertAllClosedOnce();
        });
    }

    @Test
    public void testBoundInitFailureClosesBoundsOnce() throws Exception {
        assertPredicates((fixture, compiler, sql) -> {
            final RuntimeException initFailure = new RuntimeException("owned_long init");
            fixture.failLongInit(initFailure);
            try {
                fixture.assertCursorFails(compiler, sqlExecutionContext, sql, initFailure);
            } finally {
                fixture.failLongInit(null);
            }
        });
    }

    @Test
    public void testBoundInstantiationFailureClosesBoundsOnce() throws Exception {
        assertPredicates((fixture, compiler, sql) -> {
            fixture.assertLongInstantiationFaultSweep(compiler, sqlExecutionContext, sql);
            fixture.failLongClose(new RuntimeException("owned_long close"));
            fixture.assertLongInstantiationFaultSweep(compiler, sqlExecutionContext, sql);
        });
    }

    @Test
    public void testBoundReadFailureClosesBoundsOnce() throws Exception {
        assertPredicates((fixture, compiler, sql) -> {
            final RuntimeException readFailure = new RuntimeException("owned_long read");
            fixture.failLongRead(readFailure);
            try {
                fixture.assertCursorFails(compiler, sqlExecutionContext, sql, readFailure);
            } finally {
                fixture.failLongRead(null);
            }
        });
    }

    @Test
    public void testBoundsMoveIntoIntervalModel() throws Exception {
        assertPredicates((fixture, compiler, sql) -> {
            fixture.reset();
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertIntervalScan(factory);
            }
            fixture.assertAllClosedOnce();
        });
    }

    @Test
    public void testEmptyModelClosesHandedBoundOnce() throws Exception {
        assertPredicates(new String[]{"ts = owned_long($1) AND ts > '2020-01-03' AND ts < '2020-01-02'"}, (fixture, compiler, sql) -> {
            fixture.reset();
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertIntervalScan(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            fixture.assertAllClosedOnce();
            fixture.assertLongInstantiationFaultSweep(compiler, sqlExecutionContext, sql);
            final RuntimeException closeFailure = new RuntimeException("owned_long close");
            fixture.failLongClose(closeFailure);
            Assert.assertSame(closeFailure, runAndClose(fixture, compiler, sql));
            fixture.assertAllClosedOnce();
        });
    }

    @Test
    public void testSubqueryBoundFailuresCloseOnce() throws Exception {
        assertPredicates(new String[]{"ts = (SELECT t FROM owned_table(0))", "ts > (SELECT t FROM owned_table(0))"},
                (fixture, compiler, sql) -> {
                    final OwnershipFixture.TableSpec spec = fixture.table("t", ColumnType.TIMESTAMP);
                    fixture.reset();
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertIntervalScan(factory);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertFalse(cursor.hasNext());
                        }
                    }
                    fixture.assertAllClosedOnce();
                    fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext, sql);
                    final RuntimeException closeFailure = new RuntimeException("owned_table close");
                    spec.closeFailure = closeFailure;
                    Assert.assertSame(closeFailure, runAndClose(fixture, compiler, sql));
                    fixture.assertAllClosedOnce();
                });
    }

    private static void assertIntervalScan(RecordCursorFactory factory) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), "Interval forward scan");
    }

    /**
     * Compiles, drains and closes the query, returning the first failure on the way.
     */
    private static Throwable runAndClose(OwnershipFixture fixture, SqlCompilerImpl compiler, String sql) {
        fixture.reset();
        Throwable failure = null;
        RecordCursorFactory factory = null;
        try {
            factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.drainCursor(cursor);
            }
        } catch (Throwable th) {
            failure = th;
        }
        try {
            Misc.free(factory);
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            }
        }
        return failure;
    }

    private void assertPredicates(PredicateAssertion assertion) throws Exception {
        assertPredicates(PREDICATES, assertion);
    }

    private void assertPredicates(String[] predicates, PredicateAssertion assertion) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE i_owner (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO i_owner VALUES
                    (1, '2020-01-01T00:00:00.000000Z'),
                    (2, '2020-01-02T00:00:00.000000Z'),
                    (3, '2020-01-03T00:00:00.000000Z')
                    """);
            bindVariableService.clear();
            bindVariableService.setLong(0, 1_577_836_800_000_000L);
            bindVariableService.setLong(1, 1_577_923_200_000_000L);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (String predicate : predicates) {
                    try (OwnershipFixture fixture = new OwnershipFixture(engine)) {
                        assertion.run(fixture, compiler, "SELECT * FROM i_owner WHERE " + predicate);
                    } catch (Throwable th) {
                        throw new AssertionError(predicate, th);
                    }
                }
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @FunctionalInterface
    private interface PredicateAssertion {
        void run(OwnershipFixture fixture, SqlCompilerImpl compiler, String sql) throws Exception;
    }
}
